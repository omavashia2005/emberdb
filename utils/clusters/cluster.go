package clusters

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

const (
	CLUSTER_SLOTS = 1 << 14            // 16384
	SLOT_WORDS    = CLUSTER_SLOTS / 64 // 256
)

type ClusterNode struct {
	ID             string
	ClientPort     string
	ClusterBusPort string
	OwnedSlots     [SLOT_WORDS]uint64
	NumSlots       int
}

type ClusterState struct {
	Self  *ClusterNode
	Nodes map[string]*ClusterNode     // ID to node mapping
	Slots [CLUSTER_SLOTS]*ClusterNode // Global array for slot ownership
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

	client, _:= strconv.Atoi(clientPort[1:])
	clusterBP := client + 10000
	clusterBusPort := strconv.Itoa(clusterBP)

	newNode := &ClusterNode{
		ID:             uuid.NewString(),
		ClientPort:     clientPort,
		ClusterBusPort: ":" + clusterBusPort,
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
