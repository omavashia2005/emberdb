package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/lobaro/crc16"
	"github.com/omavashia2005/emberdb/utils"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
	"github.com/omavashia2005/emberdb/utils/pubsub"
)

var serverState *clusters.ClusterState

func BytesToLower(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return b
}

func bstring(bs []byte) string {
	p := unsafe.SliceData(bs)
	return unsafe.String(p, len(bs))
}

var ps = pubsub.NewPubSub()

func toMoveorNotToMove(key string, rconn *resp.Server, kv *kvstore.KVStore) string {

	start := strings.Index(key, "{")
	end := strings.LastIndex(key, "}")

	var hash uint16

	if start == -1 || end <= start+1 || end == -1 {
		hash = crc16.ChecksumXModem([]byte(key))
	} else {
		hash = crc16.ChecksumXModem([]byte(key[start+1 : end]))
	}

	slot := hash % 16384

	ownerNode := serverState.GetSlotOwner(int(slot))

	if ownerNode == nil {
		rconn.WriteError(fmt.Errorf("CLUSTERDOWN Hash slot not served"))
		return "ERROR"
	}

	if ownerNode != serverState.Self {
		rconn.WriteStatusString(
			fmt.Sprintf("MOVED %d %s", slot, ownerNode.GetName()),
		)

		return "MOVED"
	}

	if err := kv.SetSlotKey(slot, key); err != nil {
		return "ERROR"
	}

	return "OK"
}

func handleConnection(conn net.Conn, kv *kvstore.KVStore, clusterEnabled bool) {
	defer conn.Close()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

	if err := rconn.SetOptions(resp.ServerOptions{
		MaxMultiBulkLength: resp.Pointer(1024),
		MaxBulkLength:      resp.Pointer(65536),
		MaxBufferSize:      resp.Pointer(1048576),
	}); err != nil {
		rconn.CloseWithError(err)
	}

	kv.Clients[conn.RemoteAddr().String()] = conn

	for {
		args, err := rconn.Next()
		if err != nil {
			rconn.CloseWithError(err)
			log.Printf("closed connection from %s during read: %v", conn.RemoteAddr(), err)
			return
		}

		cmd := bstring(BytesToLower(args[0]))
		args = args[1:]

		switch cmd {
		case "flushall":
			kv.FlushAll()
			rconn.WriteOK()

		case "ping":
			if clusterEnabled {
				self := serverState.Self.Snapshot()
				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", self.Name))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClientPort))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClusterBusPort))
			} else {
				rconn.WriteStatusString("PONG")
			}
		case "echo":
			if len(args) == 0 {
				rconn.WriteError(fmt.Errorf("wrong number of arguments for 'ECHO' command"))
				continue
			}
			if len(args) == 1 {
				// Write a bulk string response
				rconn.WriteBytes(args[0])
				continue
			}

			rconn.WriteArrayBytes(args)
		case "set":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'SET' command"))
				continue
			}

			key := string(args[0])
			val := string(args[1])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			kv.Set(key, val)
			rconn.WriteOK()

		case "get":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'GET' command"))
				continue
			}

			key := string(args[0])
			val := kv.Get(key)

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			if val == "(nil)" {
				rconn.WriteStatusString("No such key")
				continue
			}

			rconn.WriteString(val)

		case "append":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'APPEND' command"))
				continue
			}

			key := string(args[0])
			valueToAppend := string(args[1])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			kv.Append(key, valueToAppend)

			rconn.WriteOK()

		case "incr":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCR' command"))
				continue
			}

			key := string(args[0])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			err := kv.Incr(key)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "incrby":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCRBY' command"))
				continue
			}

			key := string(args[0])
			incrByVal := string(args[1])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			err := kv.IncrBy(key, incrByVal)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "decr":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECR' command"))
				continue
			}

			key := string(args[0])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			err := kv.Decr(key)

			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "decrby":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECRBY' command"))
				continue
			}

			key := string(args[0])

			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}

			decrByVal := string(args[1])

			err := kv.DecrBy(key, decrByVal)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()
		case "mset":
			if len(args) == 0 || len(args)%2 != 0 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'MSET' command"))
				continue
			}

			firstKey := string(args[0])

			if clusterEnabled {
				firstSlot := crc16.ChecksumXModem([]byte(firstKey)) % 16384

				validQuery := true

				for i := 2; i < len(args); i += 2 {
					key := string(args[i])
					slot := crc16.ChecksumXModem([]byte(key)) % 16384

					if slot != firstSlot {
						rconn.WriteError(
							fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
						)
						validQuery = false
						break
					}
				}

				if !validQuery {
					continue
				}

				// Since every key hashes to the same slot,
				// checking the first key is sufficient.
				if toMoveorNotToMove(firstKey, rconn, kv) != "OK" {
					continue
				}
			}

			for i := 0; i < len(args); i += 2 {
				key, val := string(args[i]), string(args[i+1])
				kv.Set(key, val)
			}

			rconn.WriteOK()

		case "mget":
			if len(args) == 0 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MGET' command"))
				continue
			}

			validQuery := true

			if clusterEnabled {
				firstKey := string(args[0])

				hash := crc16.ChecksumXModem([]byte(firstKey))
				firstSlot := hash % 16384

				for i := 1; i < len(args); i++ {
					key := string(args[i])

					if (crc16.ChecksumXModem([]byte(key)) % 16384) != firstSlot {
						rconn.WriteError(
							fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
						)
						validQuery = false
						break
					}
				}

				if !validQuery {
					continue
				}

				if toMoveorNotToMove(firstKey, rconn, kv) != "OK" {
					continue
				}
			}

			var resp []string

			for i := 0; i < len(args); i++ {
				key := string(args[i])
				resp = append(resp, kv.Get(key))
			}

			rconn.WriteArrayString(resp)
		case "publish":
			if len(args) == 3 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
				continue
			}

			for i := range len(args) - 1 {
				channel := string(args[i])
				msg := string(args[i+1])
				count := pubsub.Publish(channel, msg, ps)
				rconn.WriteInt(count)
			}

		case "subscribe":
			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
				continue
			}

			for i := range args {

				channel := string(args[i])
				ch := pubsub.Subscribe(channel, ps)

				go func() {

					for message := range ch {
						fmt.Printf("Received message on channel %s: %s\n", channel, message)
						rconn.WriteString(message)
					}

				}()

				rconn.WriteOK()
			}

		case "unsubscribe":
			if len(args) == 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'UNSUBSCRIBE' command"))
				continue
			}

		case "delete":
			if len(args) == 1 {
				key := string(args[0])

				if kv.Delete(key) != 1 {
					rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
					continue
				}

			} else {
				for _, k := range args {
					key := string(k)

					if kv.Delete(key) != 1 {
						rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
						continue
					}

				}
			}

			rconn.WriteOK()

		case "getkeysinslot":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'GETKEYSINSLOT' command"))
				continue
			}

			s, b := string(args[0]), string(args[1])
			slot, err := strconv.Atoi(s)
			if err != nil {
				rconn.WriteError(fmt.Errorf("Error converting slot to int %s", err))
				continue
			}
			batchSize, err := strconv.Atoi(b)
			if err != nil {
				rconn.WriteError(fmt.Errorf("Error converting batchSize to int %s", err))
				continue
			}

			keys := make([]string, 0)
			keys = kv.GetKeysInSlot(uint64(slot), int(batchSize))

			rconn.WriteArrayString(keys)

		case "restore-asking":
			if len(args) != 3 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'RESTORE-ASKING' command"))
				continue
			}

			key, dump := string(args[0]), string(args[3]) // todo ADD TTL as second arg once TTL is implemented

			value, err := clusters.RestoreDataFromBinaryDump(dump)
			if err != nil {
				rconn.WriteError(fmt.Errorf("RESTORE ASKING ERROR: %w", err))
				continue
			}

			kv.Set(key, value)

			rconn.WriteOK()

		case "setslot":
			if err := clusters.ClusterSetSlot(args); err != nil {
				fmt.Printf("[DEBUG] STATE: %+v\n", serverState)
				rconn.WriteError(fmt.Errorf("SETSLOT ERROR: %w", err))
				continue
			}

			rconn.WriteOK()

		case "migrate":
			if len(args) != 6 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MIGRATE' command"))
				continue
			}

			targetHost, targetPort := string(args[0]), string(args[1])

			if string(args[5]) != "KEYS" {
				rconn.WriteError(fmt.Errorf("INVALID MIGRATE COMMAND SYNTAX"))
				continue
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

		case "cluster":
			if !clusterEnabled {
				rconn.WriteError(fmt.Errorf("Clustering is not enabled"))
				continue
			}

			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
				continue
			}

			switch string(args[0]) {

			case "NODES":
				if serverState == nil {
					rconn.WriteError(fmt.Errorf("cluster state is not initialized"))
					continue
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
					continue
				}
				payload, err := json.Marshal(snapshots)
				if err != nil {
					rconn.WriteError(fmt.Errorf("encode cluster nodes: %w", err))
					continue
				}
				if err := rconn.WriteString(string(payload)); err != nil {
					return
				}

			case "ADDSLOTSRANGE":

				if len(args) != 3 {
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTSRANGE' command"))
					continue
				}

				slotStart, err := strconv.Atoi(string(args[1]))
				if err != nil {
					panic(fmt.Errorf("[ERROR] %e", err))
				}
				slotEnd, err := strconv.Atoi(string(args[2]))
				if err != nil {
					panic(fmt.Errorf("[ERROR] %e", err))

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
					continue
				}

				senderHost := string(args[1])
				senderPort, err := strconv.Atoi(string(args[2]))
				if err != nil {
					panic(fmt.Errorf("[ERROR] %e", err))
				}

				if err := clusters.ClusterStartHandshake(senderHost, senderPort); err != nil {
					panic(fmt.Errorf("[ERROR] %+e", err))
				}

				rconn.WriteOK()

			default:
				rconn.WriteError(fmt.Errorf("NO SUCH COMMAND"))
				continue
			}

		default:
			rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
		}
	}

}

func Run(port string, clusterHost string, clusterEnabled bool) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(fmt.Errorf("[ERROR] %e", err))

	}
	defer listener.Close()

	kv := kvstore.NewKVStore()

	if clusterEnabled {
		serverState = &clusters.ClusterState{
			Nodes: make(map[string]*clusters.ClusterNode),
			Migrating: make(map[int]*clusters.ClusterNode),
			Importing: make(map[int]*clusters.ClusterNode),
		}

		clusters.InitClusterState(serverState)

		clientPort, err := strconv.Atoi(port)
		if err != nil {
			panic(fmt.Errorf("[ERROR] %e", err))
		}
		self := clusters.NewNode(clientPort, clusterHost, 0, false)
		serverState.SetNode(self)
		serverState.Self = self

		// Cluster bus listener
		clusterBusListener, err := net.Listen(
			"tcp",
			fmt.Sprintf(":%d", self.GetClusterBusPort()),
		)
		if err != nil {
			panic(fmt.Errorf("[ERROR] %e", err))
		}

		go func() {
			defer clusterBusListener.Close()

			for {
				busConn, err := clusterBusListener.Accept()
				if err != nil {
					return
				}

				clusters.CreateClusterLink(busConn, nil, true)
			}
		}()

		// Cluster cron
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			iterations := 0

			for range ticker.C {
				clusters.ClusterCron(iterations)
				iterations++
			}
		}()
	}

	// Normal client connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(fmt.Errorf("[ERROR] %e", err))
		}

		log.Printf("opened connection from %s", conn.RemoteAddr())

		go handleConnection(conn, kv, clusterEnabled)
	}
}
