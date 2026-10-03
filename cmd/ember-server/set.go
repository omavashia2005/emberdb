package server

import (
	"fmt"
	"strings"
)

func handleSadd(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 2 {
		c.rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	members := make([]string, len(args)-1)
	for i := range members {
		members[i] = string(args[i+1])
	}
	if cmd == "sadd" {
		c.rconn.WriteInt(c.kv.SAdd(key, members...))
	} else {
		c.rconn.WriteInt(c.kv.SRem(key, members...))
	}
	return true
}

func handleSmembers(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR SMEMBERS requires 1 argument"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	c.rconn.WriteArrayString(c.kv.SMembers(key))
	return true
}

func handleSismember(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR SISMEMBER requires 2 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	if c.kv.SIsMember(key, string(args[1])) {
		c.rconn.WriteInt(1)
	} else {
		c.rconn.WriteInt(0)
	}
	return true
}
