package clusters

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
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
	CLUSTERMSG_TYPE_PING  = 0
	CLUSTERMSG_TYPE_PONG  = 1

	// flags
	CLUSTER_HANDSHAKE_NODE = 32
	CLUSTER_MEET_NODE      = 128

	// CLUSTERMSG SIZES
	CLUSTERMSG_GOSSIP_SIZE = 40 + // nodeName
		4 + // pingSent
		4 + // pongReceived
		2 + // cport
		2 + // port
		2 // flags

	CLUSTERMSG_HEADER_SIZE = 4 + // Sig
		4 + // totlen
		2 + // Type
		2 + // port
		40 + // sender
		2 + // cport
		2 + // flags
		2 // State

)

type tcp struct {
	host string
	port int
}

type ClusterNode struct {
	Name           string
	Host           string
	ClientPort     int
	ClusterBusPort int
	OwnedSlots     [SLOT_WORDS]uint64
	NumSlots       int
	flags          int
	t              tcp
	link           *clusterLink
	inbound        *clusterLink
	pongReceived   time.Time
	pingSent       time.Time
}

type ClusterState struct {
	Self  *ClusterNode
	Nodes map[string]*ClusterNode // ID to node mapping
	Host  string
	Slots [CLUSTER_SLOTS]*ClusterNode // Global array for slot ownership
	State int
}

type clusterLink struct {
	connection net.Conn
	node       *ClusterNode
	inbound    bool
	ctime      time.Time
	send       chan []byte
}

type clusterMsg struct {
	Sig    [4]byte                 // protocol signature ("RCmb")
	Type   uint16                  // message type
	port   uint16                  // sender TCP port
	totLen uint32                  // total message length
	count  uint16                  // gossip count
	sender string                  // sender node ID
	cport  uint16                  // sender cluster bus port
	flags  uint16                  // sender node flags
	State  int                     // sender's cluster state
	Gossip []*clusterMsgDataGossip // sender's view of other nodes
}

type clusterMsgDataGossip struct {
	nodeName     string // gossiped node ID
	pingSent     uint32 // last PING sent
	pongReceived uint32 // last PONG received
	cport        uint16 // gossiped node cluster bus port
	port         uint16 // gossiped node TCP port
	flags        uint16 // gossiped node flags
}

var serverState *ClusterState

/*
TODO:
A. Static slotting + MOVED
B. Rebalance calculation
C. Migration state machine
D. Cluster-bus transport
E. Cluster-state synchronization over bus
*/

func NewNode(clientPort string, host string, state *ClusterState) *ClusterNode {
	client, err := strconv.Atoi(clientPort)
	if err != nil {
		panic(fmt.Errorf("[ERROR] %e", err))
	}

	clusterBusPort := client + CLUSTER_BUS_PORT_INCR

	return &ClusterNode{
		Name:           uuid.NewString(),
		Host:           host,
		ClientPort:     client,
		ClusterBusPort: clusterBusPort,
		NumSlots:       0,
	}
}

// func RebalanceSlots() {
//
// }
//
// func GetNodeFromHash(key string, state *ClusterState) {
// 	//	hash key
// 	// 	compute slot
// 	//	make it match self. if not matching, return MOVED
//
// 	fmt.Println("TODO!")
// }

func writeFull(conn net.Conn, buf []byte) error {
	for len(buf) > 0 {
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}

		buf = buf[n:]
	}

	return nil
}
func clusterReadLoop(link *clusterLink) {

}

func clusterWriteLoop(link *clusterLink) {
	for buf := range link.send {
		if err := writeFull(link.connection, buf); err != nil {
			return
		}
	}
}
func CreateClusterLink(conn net.Conn, node *ClusterNode, inbound bool) *clusterLink {
	link := &clusterLink{
		connection: conn,
		node:       node,
		inbound:    inbound,
		ctime:      time.Now(),
		send:       make(chan []byte, 256),
	}

	if node != nil {
		if inbound {
			node.inbound = link
		} else {
			node.link = link
		}
	}

	go clusterReadLoop(link)
	go clusterWriteLoop(link)

	return link
}

func ClusterStartHandshake(senderHost string, senderPort int, state *ClusterState) error {
	senderCPort := senderPort + CLUSTER_BUS_PORT_INCR

	node := &ClusterNode{
		Name:           "temp_" + uuid.NewString(),
		flags:          CLUSTER_HANDSHAKE_NODE | CLUSTER_MEET_NODE,
		ClientPort:     senderPort,
		ClusterBusPort: senderCPort,
		t: tcp{
			host: senderHost,
			port: senderPort,
		},
	}

	state.Nodes[node.Name] = node

	conn, err := net.Dial(
		"tcp",
		net.JoinHostPort(senderHost, strconv.Itoa(senderCPort)),
	)
	if err != nil {
		return err
	}

	fmt.Printf("[DEBUG - MEET] HANDSHAKE ON SENDER PORT: %d\n", senderPort)
	fmt.Printf("[DEBUG - MEET] HANDSHAKE ON SENDER HOST: %s\n", senderHost)
	fmt.Printf("[DEBUG - MEET] NODE NAME: %s\n", node.Name)
	fmt.Printf("[DEBUG - MEET] NODE BUS PORT: %d\n", node.ClusterBusPort)

	CreateClusterLink(conn, node, false)

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
		} else {
			fmt.Printf("[DEBUG-MEET] MEET FROM PORT %d SUCCESSFUL!\n", bootstrapPort)
		}
	case error:
		return r
	default:
		return fmt.Errorf("unexpected response: %#v", r)
	}

	return nil
}

func clusterMsgBuildHdr(state ClusterState, Type int) *clusterMsg {

	var hdr clusterMsg

	myself := state.Self.Name

	hdr.Sig = [4]byte{'R', 'C', 'm', 'b'}

	hdr.Type = uint16(Type)
	hdr.sender = myself
	hdr.cport = uint16(state.Self.ClusterBusPort)
	hdr.flags = uint16(state.Self.flags)
	hdr.State = state.State
	hdr.port = uint16(state.Self.t.port)

	return &hdr
}

func getRandomNode(nodes map[string]*ClusterNode) *ClusterNode {
	n := rand.IntN(len(nodes))

	for _, node := range nodes {
		if n == 0 {
			return node
		}
		n--
	}

	return nil
}

func clusterSetGossipEntry(hdr *clusterMsg, i int, n *ClusterNode) {
	gossip := &clusterMsgDataGossip{}
	hdr.Gossip[i] = gossip

	gossip.nodeName = n.Name

	if n.pingSent.IsZero() {
		gossip.pingSent = 0
	} else {
		gossip.pingSent = uint32(n.pingSent.Unix())
	}

	if n.pongReceived.IsZero() {
		gossip.pongReceived = 0
	} else {
		gossip.pongReceived = uint32(n.pongReceived.Unix())
	}

	gossip.port = uint16(n.t.port)
	gossip.cport = uint16(n.ClusterBusPort)
	gossip.flags = uint16(n.flags)
}

func clusterSendPing(link *clusterLink, serverState *ClusterState, Type int) {

	freshNodes := len(serverState.Nodes) - 2 // all  - (sender + reciever)
	hdr := clusterMsgBuildHdr(*serverState, Type)

	// https://github.com/redis/redis/blob/4602d6e93e030efdc48f94dc2e3d3f9f32e7c72d/src/cluster_legacy.c#L3808-L3833
	wanted := int(math.Floor(float64(len(serverState.Nodes) / 10)))
	if wanted < 3 {
		wanted = 3
	}

	if wanted == freshNodes {
		wanted = freshNodes
	}

	if link.inbound && Type == CLUSTERMSG_TYPE_PING {
		link.node.pingSent = time.Now()
	}

	gossipCount := 0
	maxIterations := wanted * 3
	selected := make(map[string]bool)

	for freshNodes > 0 && gossipCount < wanted && maxIterations > 0 {

		curNode := getRandomNode(serverState.Nodes)

		if curNode == serverState.Self || curNode == link.node {
			continue
		}

		if selected[curNode.Name] {
			continue
		}

		// omitting some states included in redis source
		if curNode.flags&CLUSTER_HANDSHAKE_NODE != 0 || curNode.link == nil || curNode.NumSlots == 0 {
			// freshNodes--
			continue
		}

		selected[curNode.Name] = true
		clusterSetGossipEntry(hdr, gossipCount, curNode)
		maxIterations--
		gossipCount++
		freshNodes--
	}

	var totLen uint32 = 0
	totLen += CLUSTERMSG_HEADER_SIZE
	totLen += uint32(CLUSTERMSG_GOSSIP_SIZE * gossipCount)
	hdr.totLen = totLen
	hdr.count = uint16(gossipCount)

	link.send <- encodeClusterMsg(hdr)
}

func encodeClusterMsg(msg *clusterMsg) []byte {
	buf := make([]byte, 0, msg.totLen)

	// ---- Header ----

	// signature: 4 bytes
	buf = append(buf, msg.Sig[:]...)

	// total length: 4 bytes
	buf = binary.BigEndian.AppendUint32(buf, msg.totLen)

	// type: 2 bytes
	buf = binary.BigEndian.AppendUint16(buf, msg.Type)

	// port: 2 bytes
	buf = binary.BigEndian.AppendUint16(buf, msg.port)

	// sender: fixed 40 bytes
	sender := make([]byte, 40)
	copy(sender, []byte(msg.sender))
	buf = append(buf, sender...)

	// cluster bus port: 2 bytes
	buf = binary.BigEndian.AppendUint16(buf, msg.cport)

	// flags: 2 bytes
	buf = binary.BigEndian.AppendUint16(buf, msg.flags)

	// state: 2 bytes
	buf = binary.BigEndian.AppendUint16(buf, uint16(msg.State))

	// ---- Gossip entries ----

	for _, gossip := range msg.Gossip {
		// node name: fixed 40 bytes
		nodeName := make([]byte, 40)
		copy(nodeName, []byte(gossip.nodeName))
		buf = append(buf, nodeName...)

		// ping sent: 4 bytes
		buf = binary.BigEndian.AppendUint32(buf, gossip.pingSent)

		// pong received: 4 bytes
		buf = binary.BigEndian.AppendUint32(buf, gossip.pongReceived)

		// client port: 2 bytes
		buf = binary.BigEndian.AppendUint16(buf, gossip.port)

		// cluster bus port: 2 bytes
		buf = binary.BigEndian.AppendUint16(buf, gossip.cport)

		// flags: 2 bytes
		buf = binary.BigEndian.AppendUint16(buf, gossip.flags)
	}

	if len(buf) != int(msg.totLen) {
		panic(fmt.Sprintf(
			"cluster message size mismatch: encoded=%d expected=%d",
			len(buf),
			msg.totLen,
		))
	}

	return buf
}
func ClusterCron(state *ClusterState, iterations int) {

	minPong := time.Time{}
	// now := time.Now()
	var minPongNode *ClusterNode

	serverState = state

	// Once every second, send a PING to a random node that is NOT Self
	if iterations%10 == 0 {

		n := rand.IntN(len(state.Nodes))
		counter := 0

		for _, node := range state.Nodes {

			if counter != n {
				counter++
				continue
			}

			if node.link == nil || !node.pingSent.IsZero() {
				continue
			}

			if node.flags&(CLUSTER_MEET_NODE|CLUSTER_HANDSHAKE_NODE) != 0 {
				continue
			}

			if minPongNode == nil && minPong.After(node.pongReceived) {
				minPongNode = node
				minPong = node.pongReceived
			}

			break
		}

		if minPongNode != nil {
			fmt.Printf("[DEBUG] Pinging node %s\n", minPongNode.Name)
			clusterSendPing(minPongNode.link, state, CLUSTERMSG_TYPE_PING)
		}

	}
}
