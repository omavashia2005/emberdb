package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handlePush(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool, cmd string) {
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
}

func handlePop(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool, cmd string) {
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
}

func handleLRange(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
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
}

func handleLLen(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte, clusterEnabled bool) {
	if len(args) != 1 {
		rconn.WriteError(fmt.Errorf("ERR LLEN requires 1 argument"))
		return
	}
	key := string(args[0])
	if clusterEnabled && toMoveorNotToMove(key, rconn, kv) != "OK" {
		return
	}
	rconn.WriteInt(kv.LLen(key))
}
