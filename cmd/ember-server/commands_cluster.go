package server

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func toMoveorNotToMove(key string, rconn *resp.Server, _ *kvstore.KVStore) string {
	slot := kvstore.SlotForKey(key)

	ownerNode := serverState.GetSlotOwner(int(slot))

	if ownerNode == nil {
		rconn.WriteError(fmt.Errorf("CLUSTERDOWN Hash slot not served"))
		return "ERROR"
	}

	if ownerNode != serverState.Self {
		owner := ownerNode.Snapshot()
		rconn.WriteRaw([]byte(fmt.Sprintf("-MOVED %d %s\r\n", slot, net.JoinHostPort(owner.Host, strconv.Itoa(owner.ClientPort)))))

		return "MOVED"
	}

	return "OK"
}

func clusterSlots() []any {
	ranges := make([]any, 0)
	for start := 0; start < clusters.CLUSTER_SLOTS; {
		owner := serverState.GetSlotOwner(start)
		if owner == nil {
			start++
			continue
		}
		end := start
		for end+1 < clusters.CLUSTER_SLOTS && serverState.GetSlotOwner(end+1) == owner {
			end++
		}
		node := owner.Snapshot()
		ranges = append(ranges, []any{start, end, []any{node.Host, node.ClientPort, node.Name}})
		start = end + 1
	}
	return ranges
}

func handleGetKeysInSlot(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte) {
	if len(args) != 2 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'GETKEYSINSLOT' command"))
		return
	}

	s, b := string(args[0]), string(args[1])
	slot, err := strconv.Atoi(s)
	if err != nil {
		rconn.WriteError(fmt.Errorf("Error converting slot to int %s", err))
		return
	}
	batchSize, err := strconv.Atoi(b)
	if err != nil {
		rconn.WriteError(fmt.Errorf("Error converting batchSize to int %s", err))
		return
	}

	keys := make([]string, 0)
	keys = kv.GetKeysInSlot(uint64(slot), int(batchSize))

	rconn.WriteArrayString(keys)
}

func handleRestoreAsking(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte) {
	if len(args) != 3 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'RESTORE-ASKING' command"))
		return
	}

	key, dump := string(args[0]), string(args[3]) // todo ADD TTL as second arg once TTL is implemented

	value, err := clusters.RestoreDataFromBinaryDump(dump)
	if err != nil {
		rconn.WriteError(fmt.Errorf("RESTORE ASKING ERROR: %w", err))
		return
	}

	kv.Set(key, value)

	rconn.WriteOK()
}

func handleSetSlot(rconn *resp.Server, args [][]byte) {
	if err := clusters.ClusterSetSlot(args); err != nil {
		rconn.WriteError(fmt.Errorf("SETSLOT ERROR: %w", err))
		return
	}

	rconn.WriteOK()
}

func handleMigrate(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte) {
	if len(args) != 6 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MIGRATE' command"))
		return
	}

	targetHost, targetPort := string(args[0]), string(args[1])

	if string(args[5]) != "KEYS" {
		rconn.WriteError(fmt.Errorf("INVALID MIGRATE COMMAND SYNTAX"))
		return
	}

	targetKeys := make([]string, len(args[6:]))

	for i, key := range args[6:] {
		targetKeys[i] = string(key)
	}

	for _, key := range targetKeys {
		val := kv.Get(key)
		dump := clusters.EncodeBinaryDump(val)

		targetConn, err := net.Dial("tcp", net.JoinHostPort(targetHost, targetPort))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERROR: %w", err))
			continue
		}
		targetRconn := resp.NewServer(targetConn)
		targetReader := resp3.NewReader(targetConn)
		targetRconn.WriteArrayString([]string{
			"RESTORE-ASKING",
			key,
			"5000",
			dump,
		})

		if err := utils.ExpectStringResponse(targetReader, "OK"); err != nil {
			rconn.WriteError(fmt.Errorf("ERROR: %w", err))
			targetConn.Close()
			continue
		} else {
			if kv.Delete(key) != 1 {
				rconn.WriteError(fmt.Errorf("ERR DELETING KEY"))
				targetConn.Close()
				continue
			}
		}

		targetConn.Close()
	}

	rconn.WriteOK()
}

func handleCluster(rconn *resp.Server, args [][]byte, clusterEnabled bool) error {
	if !clusterEnabled {
		rconn.WriteError(fmt.Errorf("Clustering is not enabled"))
		return nil
	}

	if len(args) < 1 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
		return nil
	}

	switch string(args[0]) {
	case "SLOTS":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER SLOTS' command"))
			return nil
		}
		if err := rconn.WriteArray(clusterSlots()); err != nil {
			return err
		}

	case "NODES":
		if serverState == nil {
			rconn.WriteError(fmt.Errorf("cluster state is not initialized"))
			return nil
		}
		nodes := serverState.GetNodes()
		snapshots := make(map[string]clusters.NodeSnapshot, len(nodes))
		var nilNode string
		nilNodeFound := false
		for name, node := range nodes {
			if node == nil {
				nilNode = name
				nilNodeFound = true
				break
			}
			snapshots[name] = node.Snapshot()
		}
		if nilNodeFound {
			rconn.WriteError(fmt.Errorf("cluster node %q is nil", nilNode))
			return nil
		}
		payload, err := json.Marshal(snapshots)
		if err != nil {
			rconn.WriteError(fmt.Errorf("encode cluster nodes: %w", err))
			return nil
		}
		if err := rconn.WriteString(string(payload)); err != nil {
			return err
		}

	case "ADDSLOTSRANGE":

		if len(args) != 3 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTSRANGE' command"))
			return nil
		}

		slotStart, err := strconv.Atoi(string(args[1]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR invalid start slot: %v", err))
			return nil
		}
		slotEnd, err := strconv.Atoi(string(args[2]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR invalid end slot: %v", err))
			return nil
		}
		if slotStart < 0 || slotEnd < slotStart || slotEnd >= clusters.CLUSTER_SLOTS {
			rconn.WriteError(fmt.Errorf("ERR invalid slot range %d-%d", slotStart, slotEnd))
			return nil
		}

		self := serverState.Self

		serverState.Mu.Lock()
		for slot := slotStart; slot <= slotEnd; slot++ {
			serverState.Slots[slot] = self
		}
		self.AddSlotRange(slotStart, slotEnd)
		serverState.Mu.Unlock()

		rconn.WriteOK()

	case "MYADDR":
		self := serverState.Self.Snapshot()
		rconn.WriteArrayString([]string{
			self.Host,
			strconv.Itoa(self.ClientPort),
		})

	case "MEET":
		if len(args) != 3 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER MEET' command"))
			return nil
		}

		senderHost := string(args[1])
		senderPort, err := strconv.Atoi(string(args[2]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR invalid cluster port: %v", err))
			return nil
		}

		if err := clusters.ClusterStartHandshake(senderHost, senderPort); err != nil {
			rconn.WriteError(fmt.Errorf("ERR cluster meet: %v", err))
			return nil
		}

		rconn.WriteOK()

	default:
		rconn.WriteError(fmt.Errorf("NO SUCH COMMAND"))
		return nil
	}
	return nil
}
