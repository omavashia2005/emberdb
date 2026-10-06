package server

import (
	"fmt"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handleSet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleGet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleAppend(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleIncr(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 1 {
		rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCR' command"))
		return
	}

	key := string(args[0])

	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}

	err := kv.Incr(key)
	if err != nil {
		rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	rconn.WriteOK()
}

func handleIncrBy(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 2 {
		rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCRBY' command"))
		return
	}

	key := string(args[0])
	incrByVal := string(args[1])

	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}

	err := kv.IncrBy(key, incrByVal)
	if err != nil {
		rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	rconn.WriteOK()
}

func handleDecr(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 1 {
		rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECR' command"))
		return
	}

	key := string(args[0])

	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}

	err := kv.Decr(key)

	if err != nil {
		rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	rconn.WriteOK()
}

func handleDecrBy(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 2 {
		rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECRBY' command"))
		return
	}

	key := string(args[0])

	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}

	decrByVal := string(args[1])

	err := kv.DecrBy(key, decrByVal)
	if err != nil {
		rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	rconn.WriteOK()
}

func handleMSet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) == 0 || len(args)%2 != 0 {
		rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'MSET' command"))
		return
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
			return
		}

		// Since every key hashes to the same slot,
		// checking the first key is sufficient.
		if toMoveorNotToMove(firstKey, rconn, kv) != "OK" {
			return
		}
	}

	keys := make([]string, 0, len(args)/2)
	vals := make([]string, 0, len(args)/2)

	for i := 0; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
		vals = append(vals, string(args[i+1]))
	}

	kv.Mset(keys, vals)

	rconn.WriteOK()
}

func handleMGet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) == 0 {
		rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MGET' command"))
		return
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
			return
		}

		if toMoveorNotToMove(firstKey, rconn, kv) != "OK" {
			return
		}
	}

	keys := make([]string, len(args))
	for i := range args {
		keys[i] = string(args[i])
	}

	rconn.WriteArrayString(kv.Mget(keys))
}
