package server

import (
	"fmt"
	"strconv"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handleZAdd(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleZRange(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleZRem(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}
