package clusters

import (
	"math/rand/v2"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/google/uuid"
)

const (
	CLUSTER_SLOTS         = 1 << 14            // 16384
	SLOT_WORDS            = CLUSTER_SLOTS / 64 // 256
	CLUSTER_BUS_PORT_INCR = 10000
	CLUSTER_MSG_TYPE_PING = 0
	CLUSTER_MSG_TYPE_PONG = 1

	// flags
	CLUSTER_HANDSHAKE_NODE = 32
	CLUSTER_MEET_NODE      = 128
)

type tcp struct {
	host string
	port int
}
type ClusterNode struct {
	ID             string
	ClientPort     int
	ClusterBusPort int
	OwnedSlots     [SLOT_WORDS]uint64
	NumSlots       int
	flags          int
	t              tcp
	link           *clusterLink
	inboundLink    *clusterLink
	pongReceived   time.Duration
	pingSent       time.Duration
}

type ClusterState struct {
	Self  *ClusterNode
	Nodes map[string]*ClusterNode     // ID to node mapping
	Slots [CLUSTER_SLOTS]*ClusterNode // Global array for slot ownership
}

type clusterLink struct {
	connection net.Conn
	node       *ClusterNode
	inbound    bool
	ctime      time.Time
	send       chan []byte
}

/*
TODO:
A. Static slotting + MOVED
B. Rebalance calculation
C. Migration state machine
D. Cluster-bus transport
E. Cluster-state synchronization over bus
*/

func NewNode(clientPort string, state *ClusterState) *ClusterNode {

	client, _ := strconv.Atoi(clientPort[1:])
	clusterBusPort := client + CLUSTER_BUS_PORT_INCR

	newNode := &ClusterNode{
		ID:             uuid.NewString(),
		ClientPort:     client,
		ClusterBusPort: clusterBusPort,
		NumSlots:       0,
	}

	return newNode
}

func RebalanceSlots() {

}

func GetNodeFromHash(key string, state *ClusterState) {
	//	hash key
	// 	compute slot
	//	make it match self. if not matching, return MOVED

	fmt.Println("TODO!")
}

func ClusterStartHandshake(senderHost string, senderPort int, state *ClusterState) error {

	senderCPort := senderPort + CLUSTER_BUS_PORT_INCR
	fmt.Println(senderPort)
	fmt.Println(senderCPort)

	handshakeOrMeetNode := &ClusterNode{
		ID:    "temp_" + uuid.NewString(), //  temporary ID, this should / would be reset to a cluster node ID when bus packets are exchanged
		flags: CLUSTER_HANDSHAKE_NODE | CLUSTER_MEET_NODE,
	}

	tcp := tcp{
		host: senderHost,
		port: senderPort,
	}

	handshakeOrMeetNode.t = tcp
	handshakeOrMeetNode.ClusterBusPort = senderCPort
	state.Nodes[handshakeOrMeetNode.ID] = handshakeOrMeetNode

	return nil
}

func ClusterMeet(targetConn net.Conn, bootstrapPort int, bootstrapHost string) error {
	rconn := resp.NewServer(targetConn)
	reader := resp3.NewReader(targetConn)

	err := rconn.WriteArrayString([]string{
		"CLUSTER",
		"MEET",
		bootstrapHost,
		strconv.Itoa(bootstrapPort),
	})

	if err != nil {
		return err
	}

	v, _, err := reader.ReadValue()
	if err != nil {
		return err
	}

	result := v.SmartResult()

	switch r := result.(type) {
	case string:
		if r != "OK" {
			return fmt.Errorf("unexpected response: %s", r)
		}
	case error:
		return r
	default:
		return fmt.Errorf("unexpected response: %#v", r)
	}

	return nil
}

func clusterSendPing(link *clusterLink, t int) {

}

/*
This cron job has two responsibilities (so far)

  - For every tick (in time, so 10x a second) check the state.Nodes map for any node that:
    has the flag CLUSTER_HANDSHAKE_NODE | CLUSTER_MEET_NODE
    link is nil (AND)
    id has "temp_" prefix (AND)

    then send MEET to that node, set the node.id to the right node id, and link to appropriate link

* Once every second, send a PING to a random node that is NOT Self
*/
func ClusterCron(state *ClusterState, iterations int) {

	var minPong time.Duration = 0
	// now := time.Now()
	var minPongNode *ClusterNode

	if iterations%10 == 0 {

		n := rand.IntN(len(state.Nodes))
		counter := 0

		for _, node := range state.Nodes {

			if counter != 0 && counter == n{
				continue
			}

			if node.link == nil || node.pingSent != 0 {
				continue
			}

			if node.flags&(CLUSTER_MEET_NODE|CLUSTER_HANDSHAKE_NODE) != 0 {
				continue
			}

			if minPongNode == nil || minPong > node.pongReceived {
				minPongNode = node
				minPong = node.pongReceived
			}
		}

		if minPongNode != nil {
			fmt.Printf("[DEBUG] Pinging node %s\n", minPongNode.ID)
			clusterSendPing(state.Self.link, CLUSTER_MSG_TYPE_PING)
		}

	}
}
