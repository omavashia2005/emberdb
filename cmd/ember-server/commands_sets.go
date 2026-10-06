package server

import (
	"fmt"
	"strings"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handleSetMembers(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool, cmd string) {
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
}

func handleSMembers(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 1 {
		rconn.WriteError(fmt.Errorf("ERR SMEMBERS requires 1 argument"))
		return
	}
	key := string(args[0])
	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}
	rconn.WriteArrayString(kv.SMembers(key))
}

func handleSIsMember(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}
