package clusters

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils"
)

const (
	CLUSTER_SLOTS              = 1 << 14            // 16384
	SLOT_WORDS                 = CLUSTER_SLOTS / 64 // 256
	CLUSTER_BUS_PORT_INCR      = 10000
	CLUSTER_SETSLOT_BATCH_SIZE = 10
	MIGRATE_TIMEOUT            = 5000

	// flags
	CLUSTER_HANDSHAKE_NODE = 32
	CLUSTER_MEET_NODE      = 128
)

func countSlots(slots [SLOT_WORDS]uint64) int {
	count := 0

	for _, word := range slots {
		count += bits.OnesCount64(word)
	}

	return count
}

func CreateClusterLink(conn net.Conn, node *ClusterNode, inbound bool) *clusterLink {
	link := newClusterLink(conn, node, inbound)

	if node != nil {
		if inbound {
			node.SetInbound(link)
		} else {
			node.SetOutbound(link)
		}
	}

	go clusterReadLoop(link)
	go clusterWriteLoop(link)

	return link
}

func ClusterStartHandshake(senderHost string, senderPort int) error {
	senderCPort := senderPort + CLUSTER_BUS_PORT_INCR

	node := NewNode(senderPort, senderHost, CLUSTER_HANDSHAKE_NODE|CLUSTER_MEET_NODE, true)

	serverState.SetNode(node)

	conn, err := net.Dial(
		"tcp",
		net.JoinHostPort(senderHost, strconv.Itoa(senderCPort)),
	)
	if err != nil {
		return err
	}

	link := CreateClusterLink(conn, node, false)

	clusterSendPing(link, CLUSTERMSG_TYPE_PING)

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

	return utils.ExpectStringResponse(reader, "OK")
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

func clusterSendPing(link *clusterLink, messageType int) {

	nodes := serverState.GetNodes()
	freshNodes := len(nodes) - 1 // all  - sender
	hdr := clusterMsgBuildHdr(messageType)

	// https://github.com/redis/redis/blob/4602d6e93e030efdc48f94dc2e3d3f9f32e7c72d/src/cluster_legacy.c#L3808-L3833
	wanted := int(math.Floor(float64(len(nodes) / 10)))
	if wanted < 3 {
		wanted = 3
	}

	if wanted == freshNodes {
		wanted = freshNodes
	}

	linkNode := link.GetNode()
	if link.IsInbound() && messageType == CLUSTERMSG_TYPE_PING {
		linkNode.SetPingSent(time.Now())
	}

	gossipCount := 0
	maxIterations := wanted * 3
	selected := make(map[string]bool)

	for freshNodes > 0 && gossipCount < wanted && maxIterations > 0 {
		maxIterations--

		curNode := getRandomNode(nodes)

		if curNode == serverState.GetSelf() || curNode == linkNode {
			continue
		}

		snapshot := curNode.Snapshot()
		if selected[snapshot.Name] {
			continue
		}

		// omitting some states included in redis source
		// if snapshot.Flags&excludeFlags != 0 || snapshot.Outbound == nil {
		// 	continue
		// }

		selected[snapshot.Name] = true
		clusterSetGossipEntry(hdr, snapshot)
		gossipCount++
		freshNodes--
	}

	var totLen uint32 = 0
	totLen += CLUSTERMSG_HEADER_SIZE
	totLen += uint32(CLUSTERMSG_GOSSIP_SIZE * gossipCount)
	hdr.SetTotalLength(totLen)
	hdr.SetCount(uint16(gossipCount))

	sendBuf := encodeClusterMsg(hdr)

	link.Send() <- sendBuf
}

func ClusterRebalanceNodes(stateNodes map[string]*ClusterNode) (int, error) {
	if len(stateNodes) == 0 {
		return 0, fmt.Errorf("no cluster nodes provided")
	}
	fmt.Printf("[rebalance] start: %d nodes\n", len(stateNodes))
	base := CLUSTER_SLOTS / len(stateNodes)
	remainder := CLUSTER_SLOTS % len(stateNodes)
	nodes := make([]*ClusterNode, 0, len(stateNodes))

	for name, node := range stateNodes {
		if node == nil {
			return 0, fmt.Errorf("cannot rebalance: node %q is nil", name)
		}
		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetName() < nodes[j].GetName() // stable criterion
	})

	for i, node := range nodes {

		target := base
		if i < remainder {
			target++
		}

		node.SetBalance(int32(node.GetNumSlots()) - int32(target))

	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetBalance() < nodes[j].GetBalance()
	})

	dstIdx := 0
	srcIdx := len(nodes) - 1

	result := 0

	for dstIdx < srcIdx {

		dstNode := nodes[dstIdx]
		srcNode := nodes[srcIdx]
		var numslots int

		if dstNode.GetBalance() > srcNode.GetBalance() {
			numslots = int(dstNode.GetBalance())
		} else {
			numslots = int(srcNode.GetBalance())
		}

		if numslots > 0 {
			moved := clusterComputeReshardTable(numslots, srcNode)

			if len(moved) != numslots {
				if result == 0 {
					return result, fmt.Errorf("source node %s owns %d eligible slots; rebalance requested %d", srcNode.GetName(), len(moved), numslots)
				}

				dstNode.AdjustBalance(int32(numslots))
				srcNode.AdjustBalance(-int32(numslots))
				if dstNode.GetBalance() == 0 {
					dstNode.AdjustBalance(1)
				}
				if srcNode.GetBalance() == 0 {
					srcNode.AdjustBalance(-1)
				}
			}

			slots := make([]uint64, len(moved))

			for k, item := range moved {
				slots[k] = item.slot
			}

			fmt.Printf("Moving %d slots from %s to %s\n", len(slots), srcNode.GetName(), dstNode.GetName())
			fmt.Printf("[rebalance] begin transfer: %s -> %s (%d slots)\n", srcNode.GetName(), dstNode.GetName(), len(slots))
			moveResult, err := clusterMoveSlots(srcNode, dstNode, slots, len(slots), stateNodes)
			if err != nil {
				return 0, fmt.Errorf("move %d slots from %s to %s: %w", len(slots), srcNode.GetName(), dstNode.GetName(), err)
			}
			if moveResult != 1 {
				return 0, fmt.Errorf("move slots from %s to %s returned result %d", srcNode.GetName(), dstNode.GetName(), moveResult)
			}
			result = moveResult
			fmt.Printf("[rebalance] transfer complete: %s -> %s (%d slots)\n", srcNode.GetName(), dstNode.GetName(), len(slots))

		}

		dstIdx++
	}

	fmt.Println("[rebalance] complete")
	return 1, nil
}

func clusterComputeReshardTable(numslots int, source *ClusterNode) []*clusterNodeRehardItem {

	var moved []*clusterNodeRehardItem
	count := 0
	max := numslots

	ownedSlots := source.GetOwnedSlots()
	for slot := range CLUSTER_SLOTS {
		word := slot / 64
		bit := slot % 64

		if ownedSlots[word]&(uint64(1)<<bit) == 0 {
			continue
		}

		if count >= max || len(moved) >= numslots {
			break
		}

		movedItem := &clusterNodeRehardItem{
			node: source,
			slot: uint64(slot),
		}

		moved = append(moved, movedItem)

		count++
	}

	return moved

}

func clusterMoveSlots(source *ClusterNode, target *ClusterNode, slots []uint64, numslots int, nodes map[string]*ClusterNode) (int, error) {

	if numslots <= 0 {
		return 1, nil
	}
	if numslots > len(slots) {
		return 0, fmt.Errorf("requested %d slots but only %d were selected", numslots, len(slots))
	}

	slices.Sort(slots)

	sourceSnap := source.Snapshot()
	targetSnap := target.Snapshot()
	if sourceSnap.Host == "" || targetSnap.Host == "" {
		if sourceSnap.Host == "" {
			return 0, fmt.Errorf("source node %s has no host", sourceSnap.Name)
		}
		return 0, fmt.Errorf("target node %s has no host", targetSnap.Name)
	}

	targetAddr := net.JoinHostPort(targetSnap.Host, strconv.Itoa(targetSnap.ClientPort))
	fmt.Printf("[rebalance] connecting target %s at %s\n", targetSnap.Name, targetAddr)
	targetConn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		return 0, fmt.Errorf("connect to target node %s at %s: %w", targetSnap.Name, targetAddr, err)
	}
	defer targetConn.Close()

	sourceAddr := net.JoinHostPort(sourceSnap.Host, strconv.Itoa(sourceSnap.ClientPort))
	fmt.Printf("[rebalance] connecting source %s at %s\n", sourceSnap.Name, sourceAddr)
	sourceConn, err := net.Dial("tcp", sourceAddr)
	if err != nil {
		return 0, fmt.Errorf("connect to source node %s at %s: %w", sourceSnap.Name, sourceAddr, err)
	}
	defer sourceConn.Close()

	sourceRConn := resp.NewServer(sourceConn)
	targetRConn := resp.NewServer(targetConn)

	defer targetRConn.Close()
	defer sourceRConn.Close()

	sourceReader := resp3.NewReader(sourceConn)
	targetReader := resp3.NewReader(targetConn)

	slots = slots[:numslots]

	fmt.Printf("[rebalance] connections ready: source=%s target=%s\n", sourceSnap.Name, targetSnap.Name)

	for slotIndex, slot := range slots {
		progress := fmt.Sprintf("slot %d/%d (%d)", slotIndex+1, len(slots), slot)
		fmt.Printf("[rebalance] %s: begin\n", progress)

		fmt.Printf("[rebalance] %s: mark target importing\n", progress)
		if err := targetRConn.WriteArrayString([]string{
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"IMPORTING",
			sourceSnap.Name,
		}); err != nil {
			return 0, fmt.Errorf("send IMPORTING for slot %d to target node %s: %w", slot, targetSnap.Name, err)
		}

		if err := utils.ExpectStringResponse(targetReader, "OK"); err != nil {
			return 0, fmt.Errorf("mark slot %d importing on target node %s: %w", slot, targetSnap.Name, err)
		}
		fmt.Printf("[rebalance] %s: target importing complete\n", progress)

		fmt.Printf("[rebalance] %s: mark source migrating\n", progress)
		if err := sourceRConn.WriteArrayString([]string{
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"MIGRATING",
			targetSnap.Name,
		}); err != nil {
			return 0, fmt.Errorf("send MIGRATING for slot %d to source node %s: %w", slot, sourceSnap.Name, err)
		}

		if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
			return 0, fmt.Errorf("mark slot %d migrating on source %s to %s: %w", slot, sourceSnap.Name, targetSnap.Name, err)
		}
		fmt.Printf("[rebalance] %s: source migrating complete\n", progress)

		batch := 0
		for {
			batch++
			fmt.Printf("[rebalance] %s batch %d: request keys\n", progress, batch)
			if err := sourceRConn.WriteArrayString([]string{
				"GETKEYSINSLOT",
				strconv.FormatUint(slot, 10),
				strconv.Itoa(CLUSTER_SETSLOT_BATCH_SIZE),
			}); err != nil {
				return 0, fmt.Errorf("request keys for slot %d from source node %s: %w", slot, sourceSnap.Name, err)
			}

			v, _, err := sourceReader.ReadValue()
			if err != nil {
				return 0, fmt.Errorf("read keys in slot %d from source %s: %w", slot, sourceSnap.Name, err)
			}

			result := v.SmartResult()

			rawKeys, ok := result.([]interface{})
			if !ok {
				return 0, fmt.Errorf(
					"GETKEYSINSLOT for slot %d on %s returned %T, want []interface{}",
					slot,
					sourceSnap.Name,
					result,
				)
			}

			keys := make([]string, 0, len(rawKeys))

			for _, raw := range rawKeys {
				key, ok := raw.(string)
				if !ok {
					return 0, fmt.Errorf(
						"GETKEYSINSLOT for slot %d returned element %T, want string",
						slot,
						raw,
					)
				}

				keys = append(keys, key)
			}
			fmt.Printf("[rebalance] %s batch %d: received %d keys\n", progress, batch, len(keys))
			if len(keys) == 0 {
				break
			}

			args := []string{
				"MIGRATE",
				targetSnap.Host,
				strconv.Itoa(targetSnap.ClientPort),
				"",
				"0",
				strconv.Itoa(MIGRATE_TIMEOUT),
				"KEYS",
			}

			args = append(args, keys...)

			if err := sourceRConn.WriteArrayString(args); err != nil {
				return 0, fmt.Errorf("send MIGRATE for slot %d from %s to %s: %w", slot, sourceSnap.Name, targetSnap.Name, err)
			}

			if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
				return 0, fmt.Errorf("migrate keys in slot %d from %s to %s: %w", slot, sourceSnap.Name, targetSnap.Name, err)
			}
			fmt.Printf("[rebalance] %s batch %d: migrate complete\n", progress, batch)

			deleteArgs := []string{"DELETE"}
			deleteArgs = append(deleteArgs, keys...)

			if err := sourceRConn.WriteArrayString(deleteArgs); err != nil {
				return 0, fmt.Errorf("delete migrated keys from source node %s for slot %d: %w", sourceSnap.Name, slot, err)
			}

			if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
				return 0, fmt.Errorf("delete migrated keys from %s in slot %d: %w", sourceSnap.Name, slot, err)
			}
			fmt.Printf("[rebalance] %s batch %d: delete complete\n", progress, batch)

		}

		fmt.Printf("[rebalance] %s: assign target ownership\n", progress)
		if err := targetRConn.WriteArrayString([]string{
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"NODE",
			targetSnap.Name,
		}); err != nil {
			return 0, fmt.Errorf("send NODE assignment for slot %d to target node %s: %w", slot, targetSnap.Name, err)
		}
		if err := utils.ExpectStringResponse(targetReader, "OK"); err != nil {
			return 0, fmt.Errorf("set slot %d owner to %s on target node: %w", slot, targetSnap.Name, err)
		}

		if err := sourceRConn.WriteArrayString([]string{
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"NODE",
			targetSnap.Name,
		}); err != nil {
			return 0, fmt.Errorf("send NODE assignment for slot %d to source node %s: %w", slot, sourceSnap.Name, err)
		}
		if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
			return 0, fmt.Errorf("set slot %d owner to %s on source node %s: %w", slot, targetSnap.Name, sourceSnap.Name, err)
		}
		fmt.Printf("[rebalance] %s: ownership updated\n", progress)

		for name, node := range nodes {
			if node == nil {
				return 0, fmt.Errorf("cluster node %q is nil", name)
			}

			node := node.Snapshot()

			if node.Name == targetSnap.Name || node.Name == sourceSnap.Name {
				continue
			}
			if node.Host == "" {
				return 0, fmt.Errorf("cannot update slot %d on node %s: host is empty", slot, node.Name)
			}
			fmt.Printf("[rebalance] %s: update cluster node %s\n", progress, node.Name)

			nodeAddr := net.JoinHostPort(node.Host, strconv.Itoa(node.ClientPort))
			nodeConn, err := net.Dial("tcp", nodeAddr)
			if err != nil {
				return 0, fmt.Errorf("connect to node %s at %s to update slot %d: %w", node.Name, nodeAddr, slot, err)
			}

			nodeRConn := resp.NewServer(nodeConn)

			if err := nodeRConn.WriteArrayString([]string{
				"SETSLOT",
				strconv.FormatUint(slot, 10),
				"NODE",
				targetSnap.Name,
			}); err != nil {
				nodeConn.Close()
				return 0, fmt.Errorf("send NODE assignment for slot %d to cluster node %s: %w", slot, node.Name, err)
			}

			nodeReader := resp3.NewReader(nodeConn)

			err = utils.ExpectStringResponse(nodeReader, "OK")
			nodeConn.Close()
			if err != nil {
				return 0, fmt.Errorf("update slot %d on node %s: %w", slot, node.Name, err)
			}
			fmt.Printf("[rebalance] %s: cluster node %s updated\n", progress, node.Name)
		}
		fmt.Printf("[rebalance] %s: complete\n", progress)

	}

	return 1, nil
}
func ClusterSetSlot(args [][]byte) error {
	if len(args) == 2 {
		if strings.ToLower(string(args[1])) != "stable" {
			return fmt.Errorf("invalid SETSLOT arguments")
		}

		slot, err := strconv.Atoi(string(args[0]))
		if err != nil {
			return fmt.Errorf("invalid slot")
		}

		if slot < 0 || slot > 16383 {
			return fmt.Errorf("invalid slot range")
		}

		serverState.SetSlotStable(slot)
		return nil
	}

	if len(args) != 3 {
		return fmt.Errorf("invalid SETSLOT arguments")
	}

	// SETSLOT <slot> <state> <node-id>
	slot, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return fmt.Errorf("invalid slot")
	}

	if slot < 0 || slot > 16383 {
		return fmt.Errorf("invalid slot range")
	}

	state := strings.ToLower(string(args[1]))
	nodeID := string(args[2])

	node, ok := serverState.GetNode(nodeID)
	if !ok {
		return fmt.Errorf("unknown node %s", nodeID)
	}

	switch state {
	case "importing":
		if err := serverState.ImportingSlotsFrom(slot, node); err != nil {
			return fmt.Errorf("Error importing slots: %s\n", err)
		}
		return nil

	case "migrating":
		if err := serverState.MigratingSlotsTo(slot, node); err != nil {
			return fmt.Errorf("Error migrating slots: %s", err)
		}
		return nil

	case "node":
		serverState.Mu.Lock()
		defer serverState.Mu.Unlock()

		// Everyone updates their routing table.
		serverState.Slots[slot] = node

		if node.Name == serverState.Self.Name {
			// We are the new owner.
			serverState.Self.AddSlot(uint64(slot))
		} else {
			// If we currently own it, we're the source giving it away.
			word := uint64(slot) / 64
			bit := uint64(1) << (uint64(slot) % 64)

			if serverState.Self.OwnedSlots[word]&bit != 0 {
				serverState.Self.RemoveSlot(uint64(slot))
			}
		}

		// Migration is finalized locally.
		delete(serverState.Importing, slot)
		delete(serverState.Migrating, slot)

		return nil
	default:
		return fmt.Errorf("unsupported SETSLOT state %s", state)
	}
}

func ClusterCron(iterations int) {

	minPong := time.Time{}

	// Once every second, send a PING to a random node that is NOT Self
	if iterations%10 == 0 {

		nodes := serverState.GetNodes()
		n := rand.IntN(len(nodes))
		counter := 0

		var pingLink *clusterLink
		for _, node := range nodes {

			if counter != n {
				counter++
				continue
			}

			if node == serverState.Self {
				continue
			}

			snapshot := node.Snapshot()
			if snapshot.Outbound == nil || !snapshot.PingSent.IsZero() {
				continue
			}

			if snapshot.Flags&(CLUSTER_MEET_NODE|CLUSTER_HANDSHAKE_NODE) != 0 {
				continue
			}

			if pingLink == nil || minPong.After(snapshot.PongReceived) {
				pingLink = snapshot.Outbound
				minPong = snapshot.PongReceived
			}

			break
		}

		if pingLink != nil {
			clusterSendPing(pingLink, CLUSTERMSG_TYPE_PING)
		}

	}
}
