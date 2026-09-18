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
	freshNodes := len(nodes) - 2 // all  - (sender + reciever)
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
	excludeFlags := CLUSTER_HANDSHAKE_NODE

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
		if snapshot.Flags&excludeFlags != 0 || snapshot.Outbound == nil || snapshot.NumSlots == 0 {
			continue
		}

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

func ClusterRebalanceNodes() (int, error) {

	stateNodes := serverState.GetNodes()
	base := CLUSTER_SLOTS / len(stateNodes)
	remainder := CLUSTER_SLOTS % len(stateNodes)
	nodes := make([]*ClusterNode, 0, len(stateNodes))

	for _, node := range stateNodes {
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
					return result, fmt.Errorf("[ERROR] RESHARD TABLE DOES NOT MATCH NUMBER OF SLOTS")
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

			result, err := clusterMoveSlots(srcNode, dstNode, slots, len(slots))

			if err != nil || result != 1 {
				if result == 0 {
					return result, fmt.Errorf("[ERROR] MOVING SLOTS %s", err.Error())
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

		}

		dstIdx++
	}

	return result, nil
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

func clusterMoveSlots(source *ClusterNode, target *ClusterNode, slots []uint64, numslots int) (int, error) {

	if numslots <= 0 {
		return 1, nil
	}
	if numslots > len(slots) {
		return 0, fmt.Errorf("numslots exceeds provided slots")
	}

	slices.Sort(slots)

	sourceSnap := source.Snapshot()
	targetSnap := target.Snapshot()

	targetConn, err := net.Dial("tcp", net.JoinHostPort(targetSnap.Host, strconv.Itoa(targetSnap.ClientPort)))
	if err != nil {
		return 0, err
	}
	defer targetConn.Close()

	sourceConn, err := net.Dial("tcp", net.JoinHostPort(sourceSnap.Host, strconv.Itoa(sourceSnap.ClientPort)))
	if err != nil {
		return 0, err
	}
	defer sourceConn.Close()

	sourceRConn := resp.NewServer(sourceConn)
	targetRConn := resp.NewServer(targetConn)

	defer targetRConn.Close()
	defer sourceRConn.Close()

	sourceReader := resp3.NewReader(sourceConn)
	targetReader := resp3.NewReader(targetConn)

	slots = slots[:numslots]

	for _, slot := range slots {

		targetRConn.WriteArrayString([]string{
			"CLUSTER",
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"IMPORTING",
			sourceSnap.Name,
		})

		if err := utils.ExpectStringResponse(targetReader, "OK"); err != nil {
			return 0, err
		}

		sourceRConn.WriteArrayString([]string{
			"CLUSTER",
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"MIGRATING",
			targetSnap.Name,
		})

		if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
			return 0, err
		}

		for {
			sourceRConn.WriteArrayString([]string{
				"CLUSTER",
				"GETKEYSINSLOT",
				strconv.FormatUint(slot, 10),
				strconv.Itoa(CLUSTER_SETSLOT_BATCH_SIZE),
			})

			v, _, err := sourceReader.ReadValue()
			if err != nil {
				return 0, err
			}

			result := v.SmartResult()

			keys, ok := result.([]string)
			if !ok {
				return 0, fmt.Errorf("unexpected GETKEYSINSLOT response")
			}

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

			sourceRConn.WriteArrayString(args)

			if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
				return 0, fmt.Errorf("Error: %w", err)
			}

			// remove if and when cluster migrate command handles this
			sourceRConn.WriteArrayString([]string{
				"DELETE",
				strings.Join(keys, ","),
			})

			if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
				return 0, fmt.Errorf("Error: %w", err)
			}

		}

		targetRConn.WriteArrayString([]string{
			"CLUSTER",
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"NODE",
			targetSnap.Name,
		})
		if err := utils.ExpectStringResponse(targetReader, "OK"); err != nil {
			return 0, fmt.Errorf("Error: %w", err)
		}

		sourceRConn.WriteArrayString([]string{
			"CLUSTER",
			"SETSLOT",
			strconv.FormatUint(slot, 10),
			"NODE",
			targetSnap.Name,
		})
		if err := utils.ExpectStringResponse(sourceReader, "OK"); err != nil {
			return 0, fmt.Errorf("Error: %w", err)
		}

		nodes := serverState.GetNodes()

		for _, node := range nodes {

			node := node.Snapshot()

			if node.Name == targetSnap.Name || node.Name == sourceSnap.Name {
				continue
			}

			nodeConn, err := net.Dial("tcp", net.JoinHostPort(node.Host, strconv.Itoa(node.ClientPort)))
			if err != nil {
				return 0, err
			}

			nodeRConn := resp.NewServer(nodeConn)

			nodeRConn.WriteArrayString([]string{
				"CLUSTER",
				"SETSLOT",
				strconv.FormatUint(slot, 10),
				"NODE",
				targetSnap.Name,
			})

			nodeReader := resp3.NewReader(nodeConn)

			err = utils.ExpectStringResponse(nodeReader, "OK")
			nodeConn.Close()
			if err != nil {
				return 0, fmt.Errorf("Error: %w", err)
			}
		}

	}

	return 1, nil
}

func ClusterSetSlot(args [][]byte) error {

	if len(args) == 3 {

		if string(args[2]) != "stable" {
			return fmt.Errorf("invalid CLUSTER SETSLOT arguments")
		}

		slot, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return fmt.Errorf("invalid slot")
		}

		if slot < 0 || slot > 16383 {
			return fmt.Errorf("invalid slot range")
		}

		serverState.SetSlotStable(slot)

		return nil

	}

	if len(args) != 4 {
		return fmt.Errorf("invalid CLUSTER SETSLOT arguments")
	}

	slot, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return fmt.Errorf("invalid slot")
	}

	if slot < 0 || slot > 16383 {
		return fmt.Errorf("invalid slot range")
	}

	state := strings.ToLower(string(args[2]))
	nodeID := string(args[3])
	node, ok := serverState.GetNode(nodeID)

	if !ok {
		return fmt.Errorf("unknown node %s", nodeID)
	}

	switch state {
	case "importing":
		if err := serverState.ImportingSlotsFrom(slot, node); err != nil {
			return fmt.Errorf("Error importing slots: %s", err)
		}
		return nil

	case "migrating":
		if err := serverState.MigratingSlotsTo(slot, node); err != nil {
			return fmt.Errorf("Error migrating slots: %s", err)
		}
		return nil

	case "node":
		ownerNode := serverState.GetNodeFromSlot(uint64(slot))
		if ownerNode == nil {
			return fmt.Errorf("No node found for slot %d", slot)
		}
		serverState.Mu.Lock()
		node.TransferSlot(ownerNode, uint64(slot))
		delete(serverState.Importing, slot)
		delete(serverState.Migrating, slot)
		serverState.Mu.Unlock()

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
