package clusters

import (
	"fmt"
	"sync"
)

type ClusterState struct {
	Self      *ClusterNode
	Nodes     map[string]*ClusterNode // ID to node mapping
	Host      string
	Slots     [CLUSTER_SLOTS]*ClusterNode // Global array for slot ownership
	State     int
	Importing map[int]*ClusterNode
	Migrating map[int]*ClusterNode
	Mu        sync.RWMutex
}

var serverState *ClusterState

func InitClusterState(state *ClusterState) {
	serverState = state
}

func (s *ClusterState) GetSelf() *ClusterNode {
	return s.Self
}

func (s *ClusterState) GetNode(name string) (*ClusterNode, bool) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	node, exists := s.Nodes[name]
	return node, exists
}

func (s *ClusterState) SetNode(node *ClusterNode) {
	name := node.GetName()
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Nodes[name] = node
}

func (s *ClusterState) DeleteNode(name string) {
	s.Mu.Lock()
	delete(s.Nodes, name)
	s.Mu.Unlock()
}

func (s *ClusterState) GetNodes() map[string]*ClusterNode {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	nodes := make(map[string]*ClusterNode, len(s.Nodes))
	for name, node := range s.Nodes {
		nodes[name] = node
	}
	return nodes
}

func (s *ClusterState) GetState() int {
	return s.State
}

func (s *ClusterState) GetSlotOwner(slot int) *ClusterNode {

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	word := slot / 64
	bit := slot % 64
	mask := uint64(1) << bit

	if s.Self != nil && s.Self.GetOwnedSlots()[word]&mask != 0 {
		return s.Self
	}

	for _, node := range s.Nodes {
		if node != nil && node.GetOwnedSlots()[word]&mask != 0 {
			return node
		}
	}

	return nil
}
func (s *ClusterState) SetSlotStable(slot int) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	delete(s.Importing, slot)
	delete(s.Migrating, slot)

	return nil
}
func (s *ClusterState) ImportingSlotsFrom(slot int, node *ClusterNode) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	fmt.Printf("[DEBUG] STATE: %+v\n", s)
	s.Importing[slot] = node

	return nil
}

func (s *ClusterState) MigratingSlotsTo(slot int, node *ClusterNode) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	s.Migrating[slot] = node

	return nil
}

func (s *ClusterState) GetNodeFromSlot(slot uint64) *ClusterNode {

	s.Mu.RLock()
	defer s.Mu.RUnlock()

	node := s.Slots[slot]

	return node
}
