package clusters

import "testing"

// Redis mapping: "Different nodes have different IDs".
// Relevant because EmberDB uses node IDs as the ClusterState.Nodes key.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/00-base.tcl#L11-L23
func TestNodesHaveDifferentIDs(t *testing.T) {
	a := NewNode(6379, "127.0.0.1", 0, false)
	b := NewNode(6380, "127.0.0.1", 0, false)

	if a.GetName() == "" || b.GetName() == "" || a.GetName() == b.GetName() {
		t.Fatalf("node IDs must be non-empty and unique: %q, %q", a.GetName(), b.GetName())
	}
}

// Redis mapping: "It is possible to perform slot allocation".
// Relevant because every EmberDB hash slot must resolve to exactly one owner.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/00-base.tcl#L25-L27
func TestSlotAllocationCoversEverySlot(t *testing.T) {
	a := NewNode(6379, "127.0.0.1", 0, false)
	b := NewNode(6380, "127.0.0.1", 0, false)
	a.AddSlotRange(0, 8191)
	b.AddSlotRange(8192, CLUSTER_SLOTS-1)
	state := &ClusterState{Self: a, Nodes: map[string]*ClusterNode{a.GetName(): a, b.GetName(): b}}

	for slot := 0; slot < CLUSTER_SLOTS; slot++ {
		owner := state.GetSlotOwner(slot)
		if owner == nil {
			t.Fatalf("slot %d has no owner", slot)
		}
		if slot < 8192 && owner != a || slot >= 8192 && owner != b {
			t.Fatalf("slot %d resolved to node %q", slot, owner.GetName())
		}
	}
}

// Redis mapping: "Continuous slots distribution".
// Relevant because EmberDB stores the same inclusive 0..16383 slot ranges in node bitmaps.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/19-cluster-nodes-slots.tcl#L16-L30
func TestContinuousSlotDistribution(t *testing.T) {
	a := NewNode(6379, "127.0.0.1", 0, false)
	b := NewNode(6380, "127.0.0.1", 0, false)
	a.AddSlotRange(0, 8191)
	b.AddSlotRange(8192, CLUSTER_SLOTS-1)

	if a.GetNumSlots() != 8192 || b.GetNumSlots() != 8192 {
		t.Fatalf("slot counts = %d, %d; want 8192 each", a.GetNumSlots(), b.GetNumSlots())
	}
	if a.GetOwnedSlots()[8191/64]&(1<<(8191%64)) == 0 || b.GetOwnedSlots()[8192/64]&(1<<(8192%64)) == 0 {
		t.Fatal("continuous slot boundary is not owned")
	}
}

// Redis mapping: "slot must be unbound on the owner when it is deleted".
// Relevant because EmberDB's routing lookup reads the owner's slot bitmap directly.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/15-cluster-slots.tcl#L117-L142
func TestRemovedSlotIsUnboundFromOwner(t *testing.T) {
	node := NewNode(6379, "127.0.0.1", 0, false)
	node.AddSlot(0)
	state := &ClusterState{Self: node, Nodes: map[string]*ClusterNode{node.GetName(): node}}

	node.RemoveSlot(0)

	if owner := state.GetSlotOwner(0); owner != nil {
		t.Fatalf("removed slot still resolves to %q", owner.GetName())
	}
	if node.GetNumSlots() != 0 {
		t.Fatalf("slot count = %d, want 0", node.GetNumSlots())
	}
}

// Redis mapping: "Test cluster responses during migration of slot x" setup.
// Relevant because EmberDB implements Redis's IMPORTING, MIGRATING, and STABLE slot states.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/29-slot-migration-response.tcl#L28-L48
func TestClusterSetSlotTracksMigrationState(t *testing.T) {
	source := NewNode(6379, "127.0.0.1", 0, false)
	target := NewNode(6380, "127.0.0.1", 0, false)
	serverState = &ClusterState{
		Self:      source,
		Nodes:     map[string]*ClusterNode{source.GetName(): source, target.GetName(): target},
		Importing: make(map[int]*ClusterNode),
		Migrating: make(map[int]*ClusterNode),
	}

	if err := ClusterSetSlot([][]byte{[]byte("10"), []byte("importing"), []byte(source.GetName())}); err != nil {
		t.Fatal(err)
	}
	if err := ClusterSetSlot([][]byte{[]byte("10"), []byte("migrating"), []byte(target.GetName())}); err != nil {
		t.Fatal(err)
	}
	if serverState.Importing[10] != source || serverState.Migrating[10] != target {
		t.Fatal("migration state was not recorded")
	}
	if err := ClusterSetSlot([][]byte{[]byte("10"), []byte("stable")}); err != nil {
		t.Fatal(err)
	}
	if serverState.Importing[10] != nil || serverState.Migrating[10] != nil {
		t.Fatal("stable slot retained migration state")
	}
}
