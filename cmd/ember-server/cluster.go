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

var serverState *clusters.ClusterState

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

func handleGetkeysinslot(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'GETKEYSINSLOT' command"))
		return true
	}

	s, b := string(args[0]), string(args[1])
	slot, err := strconv.Atoi(s)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("Error converting slot to int %s", err))
		return true
	}
	batchSize, err := strconv.Atoi(b)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("Error converting batchSize to int %s", err))
		return true
	}

	keys := make([]string, 0)
	keys = c.kv.GetKeysInSlot(uint64(slot), int(batchSize))

	c.rconn.WriteArrayString(keys)
	return true
}

func handleRestoreasking(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 3 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'RESTORE-ASKING' command"))
		return true
	}

	key, dump := string(args[0]), string(args[3]) // todo ADD TTL as second arg once TTL is implemented

	value, err := clusters.RestoreDataFromBinaryDump(dump)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("RESTORE ASKING ERROR: %w", err))
		return true
	}

	c.kv.Set(key, value)

	c.rconn.WriteOK()
	return true
}

func handleSetslot(c *connection, cmd string, args [][]byte) bool {
	if err := clusters.ClusterSetSlot(args); err != nil {
		c.rconn.WriteError(fmt.Errorf("SETSLOT ERROR: %w", err))
		return true
	}

	c.rconn.WriteOK()
	return true
}

func handleMigrate(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 6 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MIGRATE' command"))
		return true
	}

	targetHost, targetPort := string(args[0]), string(args[1])

	if string(args[5]) != "KEYS" {
		c.rconn.WriteError(fmt.Errorf("INVALID MIGRATE COMMAND SYNTAX"))
		return true
	}

	targetKeys := make([]string, len(args[6:]))

	for i, key := range args[6:] {
		targetKeys[i] = string(key)
	}

	for _, key := range targetKeys {
		val := c.kv.Get(key)
		dump := clusters.EncodeBinaryDump(val)

		targetConn, err := net.Dial("tcp", net.JoinHostPort(targetHost, targetPort))
		if err != nil {
			c.rconn.WriteError(fmt.Errorf("ERROR: %w", err))
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
			c.rconn.WriteError(fmt.Errorf("ERROR: %w", err))
			targetConn.Close()
			continue
		} else {
			if c.kv.Delete(key) != 1 {
				c.rconn.WriteError(fmt.Errorf("ERR DELETING KEY"))
				targetConn.Close()
				continue
			}
		}

		targetConn.Close()
	}

	c.rconn.WriteOK()
	return true
}

func handleCluster(c *connection, cmd string, args [][]byte) bool {
	if !c.clusterEnabled {
		c.rconn.WriteError(fmt.Errorf("Clustering is not enabled"))
		return true
	}

	if len(args) < 1 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
		return true
	}

	switch string(args[0]) {
	case "SLOTS":
		if len(args) != 1 {
			c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER SLOTS' command"))
			return true
		}
		if err := c.rconn.WriteArray(clusterSlots()); err != nil {
			return false
		}

	case "NODES":
		if serverState == nil {
			c.rconn.WriteError(fmt.Errorf("cluster state is not initialized"))
			return true
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
			c.rconn.WriteError(fmt.Errorf("cluster node %q is nil", nilNode))
			return true
		}
		payload, err := json.Marshal(snapshots)
		if err != nil {
			c.rconn.WriteError(fmt.Errorf("encode cluster nodes: %w", err))
			return true
		}
		if err := c.rconn.WriteString(string(payload)); err != nil {
			return false
		}

	case "ADDSLOTSRANGE":

		if len(args) != 3 {
			c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTSRANGE' command"))
			return true
		}

		slotStart, err := strconv.Atoi(string(args[1]))
		if err != nil {
			c.rconn.WriteError(fmt.Errorf("ERR invalid start slot: %v", err))
			return true
		}
		slotEnd, err := strconv.Atoi(string(args[2]))
		if err != nil {
			c.rconn.WriteError(fmt.Errorf("ERR invalid end slot: %v", err))
			return true
		}
		if slotStart < 0 || slotEnd < slotStart || slotEnd >= clusters.CLUSTER_SLOTS {
			c.rconn.WriteError(fmt.Errorf("ERR invalid slot range %d-%d", slotStart, slotEnd))
			return true
		}

		self := serverState.Self

		serverState.Mu.Lock()
		for slot := slotStart; slot <= slotEnd; slot++ {
			serverState.Slots[slot] = self
		}
		self.AddSlotRange(slotStart, slotEnd)
		serverState.Mu.Unlock()

		c.rconn.WriteOK()

	case "MYADDR":
		self := serverState.Self.Snapshot()
		c.rconn.WriteArrayString([]string{
			self.Host,
			strconv.Itoa(self.ClientPort),
		})

	case "MEET":
		if len(args) != 3 {
			c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER MEET' command"))
			return true
		}

		senderHost := string(args[1])
		senderPort, err := strconv.Atoi(string(args[2]))
		if err != nil {
			c.rconn.WriteError(fmt.Errorf("ERR invalid cluster port: %v", err))
			return true
		}

		if err := clusters.ClusterStartHandshake(senderHost, senderPort); err != nil {
			c.rconn.WriteError(fmt.Errorf("ERR cluster meet: %v", err))
			return true
		}

		c.rconn.WriteOK()

	default:
		c.rconn.WriteError(fmt.Errorf("NO SUCH COMMAND"))
		return true
	}
	return true
}
