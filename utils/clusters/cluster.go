package clusters

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
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
		2 + // State
		SLOT_WORDS*8 // slots bitmap

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
	Mu    sync.RWMutex
}

type clusterLink struct {
	connection net.Conn
	node       *ClusterNode
	inbound    bool
	ctime      time.Time
	send       chan []byte
}

type clusterMsg struct {
	// message metadata
	Sig    [4]byte // protocol signature ("RCmb")
	Type   uint16  // message type
	totLen uint32  // total message length
	count  uint16  // gossip count

	// sender information
	port   uint16 // sender TCP port
	sender string // sender node ID
	cport  uint16 // sender cluster bus port
	flags  uint16 // sender node flags
	State  int    // sender's cluster state

	slots [SLOT_WORDS]uint64

	Gossip []*clusterMsgDataGossip // sender's view of other nodes
}

type clusterMsgDataGossip struct {
	nodeName     string // gossiped node ID
	cport        uint16 // gossiped node cluster bus port
	port         uint16 // gossiped node TCP port
	flags        uint16 // gossiped node flags
	pingSent     uint32 // last PING sent
	pongReceived uint32 // last PONG received
}

var serverState *ClusterState

func NewNode(clientPort string, host string) *ClusterNode {
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

	for {
		prefix := make([]byte, 8)

		_, err := io.ReadFull(link.connection, prefix)
		if err != nil {
			return
		}

		if string(prefix[:4]) != "RCmb" {
			return
		}

		totLen := binary.BigEndian.Uint32(prefix[4:8])
		buf := make([]byte, int(totLen))

		copy(buf[:8], prefix)

		_, err = io.ReadFull(link.connection, buf[8:])
		if err != nil {
			return
		}

		msg, err := decodeClusterMsg(buf)

		clusterProcessMsg(link, msg)

	}
}

func countSlots(slots [SLOT_WORDS]uint64) int {
	count := 0

	for _, word := range slots {
		count += bits.OnesCount64(word)
	}

	return count
}

func clusterProcessGossip(msg *clusterMsg) {
	for _, gossip := range msg.Gossip {
		// Don't learn ourselves through gossip.
		if gossip.nodeName == serverState.Self.Name {
			continue
		}

		node, exists := serverState.Nodes[gossip.nodeName]

		if !exists {
			node = &ClusterNode{
				Name:           gossip.nodeName,
				ClientPort:     int(gossip.port),
				ClusterBusPort: int(gossip.cport),
				flags:          int(gossip.flags),
			}

			serverState.Nodes[node.Name] = node
			continue
		}

		// Refresh our existing view of this node.
		node.ClientPort = int(gossip.port)
		node.ClusterBusPort = int(gossip.cport)
		node.flags = int(gossip.flags)
	}
}

// using the gossip, update serverState
// send PONG via clusterSendPing()
func clusterProcessMsg(link *clusterLink, msg *clusterMsg) {

	fmt.Printf(
		"[GOSSIP] receiver=%s sender=%s gossipCount=%d\n",
		serverState.Self.Name,
		msg.sender,
		len(msg.Gossip),
	)

	for _, gossip := range msg.Gossip {
		fmt.Printf(
			"[GOSSIP] learned node=%s port=%d cport=%d flags=%d\n",
			gossip.nodeName,
			gossip.port,
			gossip.cport,
			gossip.flags,
		)
	}

	serverState.Mu.Lock()
	if link.node != nil {
		if strings.HasPrefix(link.node.Name, "temp_") {
			delete(serverState.Nodes, link.node.Name)

			link.node.Name = msg.sender
			link.node.ClientPort = int(msg.port)
			link.node.ClusterBusPort = int(msg.cport)
			link.node.flags = int(msg.flags)
		}
	} else {
		node := &ClusterNode{
			Name:           msg.sender,
			ClientPort:     int(msg.port),
			ClusterBusPort: int(msg.cport),
			flags:          int(msg.flags),
		}

		link.node = node
	}

	link.node.NumSlots = countSlots(msg.slots)
	serverState.Nodes[link.node.Name] = link.node

	clusterProcessGossip(msg)

	serverState.Mu.Unlock()

	switch msg.Type {
	case CLUSTERMSG_TYPE_PING:
		clusterSendPing(link, CLUSTERMSG_TYPE_PONG)
	case CLUSTERMSG_TYPE_PONG:
		if link.node != nil {
			link.node.pongReceived = time.Now()
			link.node.pingSent = time.Time{}
		}
	}

	fmt.Printf("[STATE] %s knows:\n", serverState.Self.Name)

	for name, node := range serverState.Nodes {
		fmt.Printf(
			"  %s port=%d cport=%d\n",
			name,
			node.ClientPort,
			node.ClusterBusPort,
		)
	}

}

func clusterWriteLoop(link *clusterLink) {
	// clusterWriteLoop
	for buf := range link.send {
		fmt.Printf("[TRACE] writer sending %d bytes to %s\n",
			len(buf), link.connection.RemoteAddr())

		if err := writeFull(link.connection, buf); err != nil {
			fmt.Printf("[TRACE] write failed: %v\n", err)
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

func ClusterStartHandshake(senderHost string, senderPort int) error {
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

	serverState.Nodes[node.Name] = node

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

	link := CreateClusterLink(conn, node, false)

	fmt.Println("[TRACE-A] about to call clusterSendPing")
	clusterSendPing(link, CLUSTERMSG_TYPE_PING)
	fmt.Println("[TRACE-B] clusterSendPing returned")

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

func clusterMsgBuildHdr(Type int) *clusterMsg {

	var hdr clusterMsg

	myself := serverState.Self.Name

	hdr.Sig = [4]byte{'R', 'C', 'm', 'b'}

	hdr.Type = uint16(Type)
	hdr.sender = myself
	hdr.cport = uint16(serverState.Self.ClusterBusPort)
	hdr.flags = uint16(serverState.Self.flags)
	hdr.State = serverState.State
	hdr.port = uint16(serverState.Self.ClientPort)
	hdr.slots = serverState.Self.OwnedSlots

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
	hdr.Gossip = append(hdr.Gossip, gossip)

}

func clusterSendPing(link *clusterLink, Type int) {

	freshNodes := len(serverState.Nodes) - 2 // all  - (sender + reciever)
	hdr := clusterMsgBuildHdr(Type)

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
		maxIterations--

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
		gossipCount++
		freshNodes--
	}

	var totLen uint32 = 0
	totLen += CLUSTERMSG_HEADER_SIZE
	totLen += uint32(CLUSTERMSG_GOSSIP_SIZE * gossipCount)
	hdr.totLen = totLen
	hdr.count = uint16(gossipCount)

	sendBuf := encodeClusterMsg(hdr)
	fmt.Printf("[TRACE] enqueue bytes len=%d\n", len(sendBuf))

	link.send <- sendBuf
}

func decodeClusterMsg(buf []byte) (*clusterMsg, error) {
	if len(buf) < CLUSTERMSG_HEADER_SIZE {
		return nil, fmt.Errorf("cluster message too short: %d bytes", len(buf))
	}

	msg := &clusterMsg{}
	offset := 0

	// signature: 4 bytes
	copy(msg.Sig[:], buf[offset:offset+4])
	offset += 4

	if string(msg.Sig[:]) != "RCmb" {
		return nil, fmt.Errorf("invalid cluster message signature: %q", msg.Sig)
	}

	// total length: 4 bytes
	msg.totLen = binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4

	if int(msg.totLen) != len(buf) {
		return nil, fmt.Errorf(
			"cluster message length mismatch: header=%d actual=%d",
			msg.totLen,
			len(buf),
		)
	}

	// type: 2 bytes
	msg.Type = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	// client port: 2 bytes
	msg.port = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	// sender: fixed 40 bytes
	senderBytes := buf[offset : offset+40]
	msg.sender = string(bytes.TrimRight(senderBytes, "\x00"))
	offset += 40

	// cluster bus port: 2 bytes
	msg.cport = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	// flags: 2 bytes
	msg.flags = binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2

	// state: 2 bytes
	msg.State = int(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2

	// slots
	for i := 0; i < SLOT_WORDS; i++ {
		msg.slots[i] = binary.BigEndian.Uint64(buf[offset : offset+8])
		offset += 8
	}

	// Everything remaining should be gossip entries.
	remaining := len(buf) - offset

	if remaining%CLUSTERMSG_GOSSIP_SIZE != 0 {
		return nil, fmt.Errorf(
			"invalid gossip payload size: %d bytes",
			remaining,
		)
	}

	gossipCount := remaining / CLUSTERMSG_GOSSIP_SIZE
	msg.count = uint16(gossipCount)
	msg.Gossip = make([]*clusterMsgDataGossip, 0, gossipCount)

	for i := 0; i < gossipCount; i++ {
		gossip := &clusterMsgDataGossip{}

		// node name: 40 bytes
		nodeNameBytes := buf[offset : offset+40]
		gossip.nodeName = string(bytes.TrimRight(nodeNameBytes, "\x00"))
		offset += 40

		// ping sent: 4 bytes
		gossip.pingSent = binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4

		// pong received: 4 bytes
		gossip.pongReceived = binary.BigEndian.Uint32(buf[offset : offset+4])
		offset += 4

		// client port: 2 bytes
		gossip.port = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2

		// cluster bus port: 2 bytes
		gossip.cport = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2

		// flags: 2 bytes
		gossip.flags = binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2

		msg.Gossip = append(msg.Gossip, gossip)
	}

	return msg, nil
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

	// owned slots
	for _, word := range msg.slots {
		buf = binary.BigEndian.AppendUint64(buf, word)
	}

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

func InitClusterState(state *ClusterState) {
	serverState = state
}
func ClusterCron(iterations int) {

	minPong := time.Time{}
	// now := time.Now()
	var minPongNode *ClusterNode

	// Once every second, send a PING to a random node that is NOT Self
	if iterations%10 == 0 {

		serverState.Mu.RLock()
		defer serverState.Mu.RUnlock()

		n := rand.IntN(len(serverState.Nodes))
		counter := 0

		for _, node := range serverState.Nodes {

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

			if minPongNode == nil || minPong.After(node.pongReceived) {
				minPongNode = node
				minPong = node.pongReceived
			}

			break
		}

		if minPongNode != nil {
			fmt.Printf("[CRON] Pinging node %s\n", minPongNode.Name)
			clusterSendPing(minPongNode.link, CLUSTERMSG_TYPE_PING)
		}

	}
}
