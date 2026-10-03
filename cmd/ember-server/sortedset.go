package server

import (
	"fmt"
	"strconv"
)

func handleZadd(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 3 || len(args)%2 == 0 {
		c.rconn.WriteError(fmt.Errorf("ERR ZADD requires score/member pairs"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	pairs := make([]string, len(args)-1)
	for i := range pairs {
		pairs[i] = string(args[i+1])
	}
	added, err := c.kv.ZAdd(key, pairs...)
	if err != nil {
		c.rconn.WriteError(err)
		return true
	}
	c.rconn.WriteInt(added)
	return true
}

func handleZrange(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 3 {
		c.rconn.WriteError(fmt.Errorf("ERR ZRANGE requires 3 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid start index"))
		return true
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR ZRANGE invalid stop index"))
		return true
	}
	c.rconn.WriteArrayString(c.kv.ZRange(key, start, end))
	return true
}

func handleZrem(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 2 {
		c.rconn.WriteError(fmt.Errorf("ERR ZREM requires at least 2 arguments"))
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
	c.rconn.WriteInt(c.kv.ZRem(key, members...))
	return true
}
