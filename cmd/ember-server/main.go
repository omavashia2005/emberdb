package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
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

func toMoveorNotToMove(key string, rconn *resp.Server, _ *kvstore.KVStore) string {
	slot := kvstore.SlotForKey(key)

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

	return "OK"
}

func handleConnection(conn net.Conn, kv *kvstore.KVStore, clusterEnabled bool) {
	defer conn.Close()
	subscriptions := make(map[string][]chan string)
	defer func() {
		for channel, subscribers := range subscriptions {
			for _, subscriber := range subscribers {
				pubsub.Unsubscribe(channel, subscriber, ps)
			}
		}
	}()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

	if err := rconn.SetOptions(resp.ServerOptions{
		MaxMultiBulkLength: resp.Pointer(1024),
		MaxBulkLength:      resp.Pointer(65536),
		MaxBufferSize:      resp.Pointer(1048576),
	}); err != nil {
		rconn.CloseWithError(err)
	}

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
		case "save":
			if len(args) != 0 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'SAVE' command"))
				continue
			}
			if err := kv.SaveRDB(); err != nil {
				rconn.WriteError(err)
				continue
			}
			rconn.WriteOK()
		case "bgsave":
			if len(args) != 0 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'BGSAVE' command"))
				continue
			}
			go func() {
				if err := kv.SaveRDB(); err != nil {
					utils.PrintError(err)
				}
			}()
			rconn.WriteStatusString("Background saving started")
		case "bgrewriteaof":
			if len(args) != 0 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'BGREWRITEAOF' command"))
				continue
			}
			go func() {
				if err := kv.RewriteAOF(); err != nil {
					utils.PrintError(err)
				}
			}()
			rconn.WriteStatusString("Background append only file rewriting started")

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
		case "lpush", "rpush":
			if len(args) < 2 {
				rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			values := make([]string, len(args)-1)
			for i := range values {
				values[i] = string(args[i+1])
			}
			if cmd == "lpush" {
				rconn.WriteInt(kv.LPush(key, values...))
			} else {
				rconn.WriteInt(kv.RPush(key, values...))
			}

		case "lpop", "rpop":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR %s requires 1 argument", strings.ToUpper(cmd)))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			var value string
			var ok bool
			if cmd == "lpop" {
				value, ok = kv.LPop(key)
			} else {
				value, ok = kv.RPop(key)
			}
			if !ok {
				rconn.WriteNullString()
			} else {
				rconn.WriteString(value)
			}

		case "lrange":
			if len(args) != 3 {
				rconn.WriteError(fmt.Errorf("ERR LRANGE requires 3 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			start, err := strconv.Atoi(string(args[1]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR LRANGE start index must be an integer"))
				continue
			}
			end, err := strconv.Atoi(string(args[2]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR LRANGE end index must be an integer"))
				continue
			}
			rconn.WriteArrayString(kv.LRange(key, start, end))

		case "llen":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR LLEN requires 1 argument"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			rconn.WriteInt(kv.LLen(key))

		case "hset":
			if len(args) != 3 {
				rconn.WriteError(fmt.Errorf("ERR HSET requires 3 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			kv.HSet(key, string(args[1]), string(args[2]))
			rconn.WriteOK()

		case "hget":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR HGET requires 2 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			value, ok := kv.HGet(key, string(args[1]))
			if !ok {
				rconn.WriteNullString()
			} else {
				rconn.WriteString(value)
			}

		case "hmset":
			if len(args) < 3 || len(args)%2 == 0 {
				rconn.WriteError(fmt.Errorf("ERR HMSET requires field/value pairs"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			fields := make(map[string]string, (len(args)-1)/2)
			for i := 1; i < len(args); i += 2 {
				fields[string(args[i])] = string(args[i+1])
			}
			kv.HMSet(key, fields)
			rconn.WriteOK()

		case "hmget":
			if len(args) < 2 {
				rconn.WriteError(fmt.Errorf("ERR HMGET requires at least 2 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			fields := make([]string, len(args)-1)
			for i := range fields {
				fields[i] = string(args[i+1])
			}
			rconn.WriteArray(kv.HMGet(key, fields...))

		case "hgetall":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR HGETALL requires 1 argument"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			rconn.WriteArrayString(kv.HGetAll(key))

		case "hdel":
			if len(args) < 2 {
				rconn.WriteError(fmt.Errorf("ERR HDEL requires at least 2 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			fields := make([]string, len(args)-1)
			for i := range fields {
				fields[i] = string(args[i+1])
			}
			rconn.WriteInt(kv.HDel(key, fields...))

		case "sadd", "srem":
			if len(args) < 2 {
				rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			members := make([]string, len(args)-1)
			for i := range members {
				members[i] = string(args[i+1])
			}
			if cmd == "sadd" {
				rconn.WriteInt(kv.SAdd(key, members...))
			} else {
				rconn.WriteInt(kv.SRem(key, members...))
			}

		case "smembers":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR SMEMBERS requires 1 argument"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			rconn.WriteArrayString(kv.SMembers(key))

		case "sismember":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR SISMEMBER requires 2 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			if kv.SIsMember(key, string(args[1])) {
				rconn.WriteInt(1)
			} else {
				rconn.WriteInt(0)
			}

		case "zadd":
			if len(args) < 3 || len(args)%2 == 0 {
				rconn.WriteError(fmt.Errorf("ERR ZADD requires score/member pairs"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			pairs := make([]string, len(args)-1)
			for i := range pairs {
				pairs[i] = string(args[i+1])
			}
			added, err := kv.ZAdd(key, pairs...)
			if err != nil {
				rconn.WriteError(err)
				continue
			}
			rconn.WriteInt(added)

		case "zrange":
			if len(args) != 3 {
				rconn.WriteError(fmt.Errorf("ERR ZRANGE requires 3 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			start, err := strconv.Atoi(string(args[1]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid start index"))
				continue
			}
			end, err := strconv.Atoi(string(args[2]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid stop index"))
				continue
			}
			rconn.WriteArrayString(kv.ZRange(key, start, end))

		case "zrem":
			if len(args) < 2 {
				rconn.WriteError(fmt.Errorf("ERR ZREM requires at least 2 arguments"))
				continue
			}
			key := string(args[0])
			if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
				continue
			}
			members := make([]string, len(args)-1)
			for i := range members {
				members[i] = string(args[i+1])
			}
			rconn.WriteInt(kv.ZRem(key, members...))

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
				firstSlot := kvstore.SlotForKey(firstKey)

				validQuery := true

				for i := 2; i < len(args); i += 2 {
					key := string(args[i])
					slot := kvstore.SlotForKey(key)

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

				firstSlot := kvstore.SlotForKey(firstKey)

				for i := 1; i < len(args); i++ {
					key := string(args[i])

					if kvstore.SlotForKey(key) != firstSlot {
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
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
				continue
			}

			channel, message := string(args[0]), string(args[1])
			rconn.WriteInt(pubsub.Publish(channel, message, ps))

		case "subscribe":
			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
				continue
			}

			for i := range args {

				channel := string(args[i])
				ch := pubsub.Subscribe(channel, ps)
				subscriptions[channel] = append(subscriptions[channel], ch)

				go func() {

					for message := range ch {
						fmt.Printf("Received message on channel %s: %s\n", channel, message)
						rconn.WriteString(message)
					}

				}()

				rconn.WriteOK()
			}

		case "unsubscribe":
			channels := make([]string, len(args))
			for i := range args {
				channels[i] = string(args[i])
			}
			if len(channels) == 0 {
				for channel := range subscriptions {
					channels = append(channels, channel)
				}
			}
			for _, channel := range channels {
				for _, subscriber := range subscriptions[channel] {
					pubsub.Unsubscribe(channel, subscriber, ps)
				}
				delete(subscriptions, channel)
			}
			rconn.WriteOK()

		case "del", "delete":
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
					rconn.WriteError(fmt.Errorf("ERR invalid start slot: %v", err))
					continue
				}
				slotEnd, err := strconv.Atoi(string(args[2]))
				if err != nil {
					rconn.WriteError(fmt.Errorf("ERR invalid end slot: %v", err))
					continue
				}
				if slotStart < 0 || slotEnd < slotStart || slotEnd >= clusters.CLUSTER_SLOTS {
					rconn.WriteError(fmt.Errorf("ERR invalid slot range %d-%d", slotStart, slotEnd))
					continue
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
					rconn.WriteError(fmt.Errorf("ERR invalid cluster port: %v", err))
					continue
				}

				if err := clusters.ClusterStartHandshake(senderHost, senderPort); err != nil {
					rconn.WriteError(fmt.Errorf("ERR cluster meet: %v", err))
					continue
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
		utils.PrintError(fmt.Errorf("%w: listen on port %s: %v", utils.ErrStartup, port, err))
		return
	}
	defer listener.Close()

	dataDir := os.Getenv("EMBERDB_DATA_DIR")
	if dataDir == "" {
		dataDir = "."
	}
	kv, err := kvstore.OpenPersistent(
		filepath.Join(dataDir, "emberdb-"+strings.TrimPrefix(port, ":")),
		clusterEnabled,
	)
	if err != nil {
		utils.PrintError(fmt.Errorf("%w: load persistence: %v", utils.ErrStartup, err))
		return
	}
	defer func() {
		if err := kv.Close(); err != nil {
			utils.PrintError(fmt.Errorf("%w: close persistence: %v", utils.ErrStartup, err))
		}
	}()

	if clusterEnabled {
		serverState = &clusters.ClusterState{
			Nodes:     make(map[string]*clusters.ClusterNode),
			Migrating: make(map[int]*clusters.ClusterNode),
			Importing: make(map[int]*clusters.ClusterNode),
		}

		clusters.InitClusterState(serverState)

		clientPort, err := strconv.Atoi(port)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: invalid client port %q: %v", utils.ErrInvalidInput, port, err))
			return
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
			utils.PrintError(fmt.Errorf("%w: listen on cluster bus: %v", utils.ErrStartup, err))
			return
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
			utils.PrintError(fmt.Errorf("%w: accept client: %v", utils.ErrConnection, err))
			return
		}

		log.Printf("opened connection from %s", conn.RemoteAddr())

		go handleConnection(conn, kv, clusterEnabled)
	}
}
