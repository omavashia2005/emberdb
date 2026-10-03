package server

import (
	"fmt"
	"strconv"
	"strings"
)

func handleLpush(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 2 {
		c.rconn.WriteError(fmt.Errorf("ERR %s requires at least 2 arguments", strings.ToUpper(cmd)))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	values := make([]string, len(args)-1)
	for i := range values {
		values[i] = string(args[i+1])
	}
	if cmd == "lpush" {
		c.rconn.WriteInt(c.kv.LPush(key, values...))
	} else {
		c.rconn.WriteInt(c.kv.RPush(key, values...))
	}
	return true
}

func handleLpop(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR %s requires 1 argument", strings.ToUpper(cmd)))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	var value string
	var ok bool
	if cmd == "lpop" {
		value, ok = c.kv.LPop(key)
	} else {
		value, ok = c.kv.RPop(key)
	}
	if !ok {
		c.rconn.WriteNullString()
	} else {
		c.rconn.WriteString(value)
	}
	return true
}

func handleLrange(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 3 {
		c.rconn.WriteError(fmt.Errorf("ERR LRANGE requires 3 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR LRANGE start index must be an integer"))
		return true
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR LRANGE end index must be an integer"))
		return true
	}
	c.rconn.WriteArrayString(c.kv.LRange(key, start, end))
	return true
}

func handleLlen(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR LLEN requires 1 argument"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	c.rconn.WriteInt(c.kv.LLen(key))
	return true
}
