package server

import (
	"fmt"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handleHSet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleHGet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleHMSet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleHMGet(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleHGetAll(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 1 {
		rconn.WriteError(fmt.Errorf("ERR HGETALL requires 1 argument"))
		return
	}
	key := string(args[0])
	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}
	rconn.WriteArrayString(kv.HGetAll(key))
}

func handleHDel(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}
