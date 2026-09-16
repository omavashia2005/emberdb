package clusters

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type tcp struct {
	host string
	port int
}

type ClusterNode struct {
	mu             sync.RWMutex
	Name           string
	Host           string
	ClientPort     int
	ClusterBusPort int
	OwnedSlots     [SLOT_WORDS]uint64
	NumSlots       int
	flags          int
	TCP            tcp
	outbound       *clusterLink
	inbound        *clusterLink
	pongReceived   time.Time
	pingSent       time.Time
	balance        int32
}

type NodeSnapshot struct {
	Name           string
	Host           string
	ClientPort     int
	ClusterBusPort int
	OwnedSlots     [SLOT_WORDS]uint64
	NumSlots       int
	Flags          int
	TCPPort        int
	Outbound       *clusterLink
	PingSent       time.Time
	PongReceived   time.Time
}

type clusterNodeRehardItem struct {
	node *ClusterNode
	slot uint64
}

func NewNode(port int, host string, flags int, temp bool) *ClusterNode {
	name := uuid.NewString()
	if temp {
		name = "temp_" + name
	}

	node := &ClusterNode{
		Name:           name,
		Host:           host,
		ClientPort:     port,
		ClusterBusPort: port + CLUSTER_BUS_PORT_INCR,
		flags:          flags,
		TCP:            tcp{host: host, port: port},
	}
	return node
}

func (n *ClusterNode) GetName() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Name
}
func (n *ClusterNode) GetClientPort() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ClientPort
}
func (n *ClusterNode) GetClusterBusPort() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.ClusterBusPort
}
func (n *ClusterNode) GetOwnedSlots() [SLOT_WORDS]uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.OwnedSlots
}
func (n *ClusterNode) SetOwnedSlots(slots [SLOT_WORDS]uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.OwnedSlots = slots
	n.NumSlots = countSlots(slots)
}
func (n *ClusterNode) AddSlots(slots []uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, slot := range slots {
		n.OwnedSlots[slot/64] |= uint64(1) << (slot % 64)
	}
	n.NumSlots = countSlots(n.OwnedSlots)
}
func (n *ClusterNode) AddSlotRange(start, end int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for slot := start; slot <= end; slot++ {
		n.OwnedSlots[slot/64] |= uint64(1) << (slot % 64)
	}
	n.NumSlots = countSlots(n.OwnedSlots)
}
func (n *ClusterNode) RemoveSlots(slots []uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, slot := range slots {
		n.OwnedSlots[slot/64] &^= uint64(1) << (slot % 64)
	}
	n.NumSlots = countSlots(n.OwnedSlots)
}
func (n *ClusterNode) GetNumSlots() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.NumSlots
}
func (n *ClusterNode) SetOutbound(link *clusterLink) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.outbound = link
}
func (n *ClusterNode) SetInbound(link *clusterLink) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.inbound = link
}

func (n *ClusterNode) SetPingSent(t time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pingSent = t
}
func (n *ClusterNode) MarkPongReceived(t time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pongReceived = t
	n.pingSent = time.Time{}
}

func (n *ClusterNode) GetBalance() int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.balance
}
func (n *ClusterNode) SetBalance(balance int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.balance = balance
}
func (n *ClusterNode) AdjustBalance(delta int32) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.balance += delta
}

func (n *ClusterNode) UpdateGossip(name string, clientPort, clusterBusPort, flags int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Name = name
	n.ClientPort = clientPort
	n.TCP.port = clientPort
	n.ClusterBusPort = clusterBusPort
	n.flags = flags
}

func (n *ClusterNode) UpdateMessage(name string, clientPort, clusterBusPort, flags int, slots [SLOT_WORDS]uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Name = name
	n.ClientPort = clientPort
	n.TCP.port = clientPort
	n.ClusterBusPort = clusterBusPort
	n.flags = flags
	n.OwnedSlots = slots
	n.NumSlots = countSlots(slots)
}

func (n *ClusterNode) Snapshot() NodeSnapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return NodeSnapshot{
		Name:           n.Name,
		Host:           n.Host,
		ClientPort:     n.ClientPort,
		ClusterBusPort: n.ClusterBusPort,
		OwnedSlots:     n.OwnedSlots,
		NumSlots:       n.NumSlots,
		Flags:          n.flags,
		TCPPort:        n.TCP.port,
		Outbound:       n.outbound,
		PingSent:       n.pingSent,
		PongReceived:   n.pongReceived,
	}
}
