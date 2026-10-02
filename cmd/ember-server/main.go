package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/panjf2000/gnet/v2"

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

// connState is per-connection state attached to each gnet.Conn. Replies are
// encoded into out, then flushed to the socket in a single Write at the end of
// OnTraffic — gnet.Conn.Write issues a syscall per call, so batching the whole
// read's replies into one buffer is what keeps multi-reply commands (MGET) and
// pipelines to one write. args is reused scratch for the command parser.
type connState struct {
	c    gnet.Conn
	out  bytes.Buffer
	enc  *resp.Server
	subs map[string]struct{}
	args [][]byte
}

// emberHandler is the single-threaded event loop. All kvstore, serverState and
// pubsub access happens inside these callbacks, so none of them need a mutex.
type emberHandler struct {
	gnet.BuiltinEventEngine
	kv             *kvstore.KVStore
	clusterEnabled bool
	cronIter       int
}

func (h *emberHandler) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	cs := &connState{c: c, subs: make(map[string]struct{})}
	cs.enc = resp.NewWriter(&cs.out)
	c.SetContext(cs)
	return nil, gnet.None
}

func (h *emberHandler) OnClose(c gnet.Conn, _ error) gnet.Action {
	if cs, ok := c.Context().(*connState); ok {
		for channel := range cs.subs {
			pubsub.Unsubscribe(channel, c, ps)
		}
	}
	return gnet.None
}

func (h *emberHandler) OnTraffic(c gnet.Conn) gnet.Action {
	cs := c.Context().(*connState)
	action := gnet.None
	for {
		buf, _ := c.Peek(-1)
		if len(buf) == 0 {
			break
		}
		args, consumed, err := resp.ParseCommand(buf, cs.args)
		if errors.Is(err, resp.ErrIncomplete) {
			break
		}
		if err != nil {
			cs.enc.WriteError(err)
			action = gnet.Close
			break
		}
		if len(args) > 0 {
			cs.args = args[:0] // keep the backing array for the next parse
			h.dispatch(cs, args)
		}
		c.Discard(consumed) // after dispatch: args alias the peeked buffer
	}
	// One syscall for every reply produced by this read; gnet copies any tail it
	// can't write immediately, so resetting out here is safe.
	if cs.out.Len() > 0 {
		c.Write(cs.out.Bytes())
		cs.out.Reset()
	}
	return action
}

// OnTick is the cluster serverCron, replacing the old 100ms ticker goroutine. gnet
// runs it on its own goroutine (not the event loop), so it only touches
// serverState, which is RWMutex-guarded — never the lock-free kvstore.
func (h *emberHandler) OnTick() (time.Duration, gnet.Action) {
	clusters.ClusterCron(h.cronIter)
	h.cronIter++
	return 100 * time.Millisecond, gnet.None
}

func (h *emberHandler) dispatch(cs *connState, args [][]byte) {
	kv := h.kv
	clusterEnabled := h.clusterEnabled
	rconn := cs.enc

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
			return
		}
		if len(args) == 1 {
			rconn.WriteBytes(args[0])
			return
		}
		rconn.WriteArrayBytes(args)
	case "lpush", "rpush":
		if len(args) < 2 {
			rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
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
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
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
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		start, err := strconv.Atoi(string(args[1]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR LRANGE start index must be an integer"))
			return
		}
		end, err := strconv.Atoi(string(args[2]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR LRANGE end index must be an integer"))
			return
		}
		rconn.WriteArrayString(kv.LRange(key, start, end))

	case "llen":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("ERR LLEN requires 1 argument"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		rconn.WriteInt(kv.LLen(key))

	case "hset":
		if len(args) != 3 {
			rconn.WriteError(fmt.Errorf("ERR HSET requires 3 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		kv.HSet(key, string(args[1]), string(args[2]))
		rconn.WriteOK()

	case "hget":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR HGET requires 2 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
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
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
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
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		fields := make([]string, len(args)-1)
		for i := range fields {
			fields[i] = string(args[i+1])
		}
		rconn.WriteArray(kv.HMGet(key, fields...))

	case "hgetall":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("ERR HGETALL requires 1 argument"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		rconn.WriteArrayString(kv.HGetAll(key))

	case "hdel":
		if len(args) < 2 {
			rconn.WriteError(fmt.Errorf("ERR HDEL requires at least 2 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		fields := make([]string, len(args)-1)
		for i := range fields {
			fields[i] = string(args[i+1])
		}
		rconn.WriteInt(kv.HDel(key, fields...))

	case "sadd", "srem":
		if len(args) < 2 {
			rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
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
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		rconn.WriteArrayString(kv.SMembers(key))

	case "sismember":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR SISMEMBER requires 2 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		if kv.SIsMember(key, string(args[1])) {
			rconn.WriteInt(1)
		} else {
			rconn.WriteInt(0)
		}

	case "zadd":
		if len(args) < 3 || len(args)%2 == 0 {
			rconn.WriteError(fmt.Errorf("ERR ZADD requires score/member pairs"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		pairs := make([]string, len(args)-1)
		for i := range pairs {
			pairs[i] = string(args[i+1])
		}
		added, err := kv.ZAdd(key, pairs...)
		if err != nil {
			rconn.WriteError(err)
			return
		}
		rconn.WriteInt(added)

	case "zrange":
		if len(args) != 3 {
			rconn.WriteError(fmt.Errorf("ERR ZRANGE requires 3 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		start, err := strconv.Atoi(string(args[1]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid start index"))
			return
		}
		end, err := strconv.Atoi(string(args[2]))
		if err != nil {
			rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid stop index"))
			return
		}
		rconn.WriteArrayString(kv.ZRange(key, start, end))

	case "zrem":
		if len(args) < 2 {
			rconn.WriteError(fmt.Errorf("ERR ZREM requires at least 2 arguments"))
			return
		}
		key := string(args[0])
		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}
		members := make([]string, len(args)-1)
		for i := range members {
			members[i] = string(args[i+1])
		}
		rconn.WriteInt(kv.ZRem(key, members...))

	case "set":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'SET' command"))
			return
		}

		key := string(args[0])
		val := string(args[1])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		kv.Set(key, val)
		rconn.WriteOK()

	case "get":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'GET' command"))
			return
		}

		key := string(args[0])
		val := kv.Get(key)

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		if val == "(nil)" {
			rconn.WriteStatusString("No such key")
			return
		}

		rconn.WriteString(val)

	case "append":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'APPEND' command"))
			return
		}

		key := string(args[0])
		valueToAppend := string(args[1])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		kv.Append(key, valueToAppend)

		rconn.WriteOK()

	case "incr":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCR' command"))
			return
		}

		key := string(args[0])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		if err := kv.Incr(key); err != nil {
			rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			return
		}

		rconn.WriteOK()

	case "incrby":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCRBY' command"))
			return
		}

		key := string(args[0])
		incrByVal := string(args[1])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		if err := kv.IncrBy(key, incrByVal); err != nil {
			rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			return
		}

		rconn.WriteOK()

	case "decr":
		if len(args) != 1 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECR' command"))
			return
		}

		key := string(args[0])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		if err := kv.Decr(key); err != nil {
			rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			return
		}

		rconn.WriteOK()

	case "decrby":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECRBY' command"))
			return
		}

		key := string(args[0])

		if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
			return
		}

		decrByVal := string(args[1])

		if err := kv.DecrBy(key, decrByVal); err != nil {
			rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			return
		}

		rconn.WriteOK()
	case "mset":
		if len(args) == 0 || len(args)%2 != 0 {
			rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'MSET' command"))
			return
		}

		if clusterEnabled {
			// Slot checks don't retain keys, so compare with zero-copy strings.
			firstSlot := kvstore.SlotForKey(bstring(args[0]))

			validQuery := true

			for i := 2; i < len(args); i += 2 {
				if kvstore.SlotForKey(bstring(args[i])) != firstSlot {
					rconn.WriteError(
						fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
					)
					validQuery = false
					break
				}
			}

			if !validQuery {
				return
			}

			// Since every key hashes to the same slot,
			// checking the first key is sufficient.
			if toMoveorNotToMove(bstring(args[0]), rconn, kv) != "OK" {
				return
			}
		}

		keys := make([]string, 0, len(args)/2)
		values := make([]string, 0, len(args)/2)

		for i := 0; i+1 < len(args); i += 2 {
			keys = append(keys, string(args[i]))
			values = append(values, string(args[i+1]))
		}

		kv.Mset(keys, values)

		rconn.WriteOK()

	case "mget":
		if len(args) == 0 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MGET' command"))
			return
		}

		validQuery := true

		if clusterEnabled {
			firstSlot := kvstore.SlotForKey(bstring(args[0]))

			for i := 1; i < len(args); i++ {
				if kvstore.SlotForKey(bstring(args[i])) != firstSlot {
					rconn.WriteError(
						fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
					)
					validQuery = false
					break
				}
			}

			if !validQuery {
				return
			}

			if toMoveorNotToMove(bstring(args[0]), rconn, kv) != "OK" {
				return
			}
		}

		// A missing key is a null element, not the literal "(nil)" string.
		rconn.WriteArrayHeader(len(args))
		for i := 0; i < len(args); i++ {
			if val, ok := kv.GetString(bstring(args[i])); ok {
				rconn.WriteString(val)
			} else {
				rconn.WriteNullString()
			}
		}
	case "publish":
		if len(args) != 2 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
			return
		}

		channel, message := string(args[0]), string(args[1])
		rconn.WriteInt(pubsub.Publish(channel, message, ps))

	case "subscribe":
		if len(args) < 1 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
			return
		}

		for i := range args {
			channel := string(args[i])
			pubsub.Subscribe(channel, cs.c, ps)
			cs.subs[channel] = struct{}{}
			rconn.WriteOK()
		}

	case "unsubscribe":
		channels := make([]string, 0, len(args))
		for i := range args {
			channels = append(channels, string(args[i]))
		}
		if len(channels) == 0 {
			for channel := range cs.subs {
				channels = append(channels, channel)
			}
		}
		for _, channel := range channels {
			pubsub.Unsubscribe(channel, cs.c, ps)
			delete(cs.subs, channel)
		}
		rconn.WriteOK()

	case "del", "delete":
		if len(args) == 1 {
			key := string(args[0])

			if kv.Delete(key) != 1 {
				rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
				return
			}

		} else {
			for _, k := range args {
				key := string(k)

				if kv.Delete(key) != 1 {
					rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
					return
				}

			}
		}

		rconn.WriteOK()

	case "getkeysinslot":
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

	case "restore-asking":
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

	case "setslot":
		if err := clusters.ClusterSetSlot(args); err != nil {
			rconn.WriteError(fmt.Errorf("SETSLOT ERROR: %w", err))
			return
		}

		rconn.WriteOK()

	case "migrate":
		// ponytail: this dials peers and does synchronous request/response on the
		// event loop, blocking other clients for the duration. Acceptable because
		// MIGRATE only runs during manual resharding (a rare admin operation);
		// move it to a helper goroutine + task queue if resharding must be online.
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

	case "cluster":
		if !clusterEnabled {
			rconn.WriteError(fmt.Errorf("Clustering is not enabled"))
			return
		}

		if len(args) < 1 {
			rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
			return
		}

		switch string(args[0]) {
		case "SLOTS":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER SLOTS' command"))
				return
			}
			if err := rconn.WriteArray(clusterSlots()); err != nil {
				return
			}

		case "NODES":
			if serverState == nil {
				rconn.WriteError(fmt.Errorf("cluster state is not initialized"))
				return
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
				return
			}
			payload, err := json.Marshal(snapshots)
			if err != nil {
				rconn.WriteError(fmt.Errorf("encode cluster nodes: %w", err))
				return
			}
			if err := rconn.WriteString(string(payload)); err != nil {
				return
			}

		case "ADDSLOTSRANGE":

			if len(args) != 3 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTSRANGE' command"))
				return
			}

			slotStart, err := strconv.Atoi(string(args[1]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR invalid start slot: %v", err))
				return
			}
			slotEnd, err := strconv.Atoi(string(args[2]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR invalid end slot: %v", err))
				return
			}
			if slotStart < 0 || slotEnd < slotStart || slotEnd >= clusters.CLUSTER_SLOTS {
				rconn.WriteError(fmt.Errorf("ERR invalid slot range %d-%d", slotStart, slotEnd))
				return
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
				return
			}

			senderHost := string(args[1])
			senderPort, err := strconv.Atoi(string(args[2]))
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR invalid cluster port: %v", err))
				return
			}

			if err := clusters.ClusterStartHandshake(senderHost, senderPort); err != nil {
				rconn.WriteError(fmt.Errorf("ERR cluster meet: %v", err))
				return
			}

			rconn.WriteOK()

		default:
			rconn.WriteError(fmt.Errorf("NO SUCH COMMAND"))
			return
		}

	default:
		rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
	}
}

func Run(port string, clusterHost string, clusterEnabled bool) {
	pprofAddr := os.Getenv("EMBERDB_PPROF_ADDR")
	if pprofAddr == "" {
		pprofAddr = "127.0.0.1:6060"
	}
	pprofListener, err := net.Listen("tcp", pprofAddr)
	if err != nil {
		log.Printf("pprof listen on %s: %v", pprofAddr, err)
	} else {
		defer pprofListener.Close()
		log.Printf("pprof listening on %s", pprofListener.Addr())
		go func() {
			if err := http.Serve(pprofListener, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Printf("pprof server: %v", err)
			}
		}()
	}

	kv := kvstore.NewKVStore(clusterEnabled)

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

		// Cluster bus listener: pure transport. Each link's read loop deserializes
		// frames and enqueues state mutations onto the event loop (see tasks.go);
		// it never touches serverState or kv directly.
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
	}

	h := &emberHandler{kv: kv, clusterEnabled: clusterEnabled}
	// Single-threaded reactor: Multicore(false) => one event loop; Ticker(true)
	// enables OnTick (cluster cron + task-queue drain).
	if err := gnet.Run(
		h,
		"tcp://:"+port,
		gnet.WithMulticore(false),
		gnet.WithReuseAddr(true),
		gnet.WithTicker(clusterEnabled),
	); err != nil {
		utils.PrintError(fmt.Errorf("%w: gnet run on port %s: %v", utils.ErrStartup, port, err))
	}
}
