package server

import "fmt"

func handleHset(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 3 {
		c.rconn.WriteError(fmt.Errorf("ERR HSET requires 3 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	c.kv.HSet(key, string(args[1]), string(args[2]))
	c.rconn.WriteOK()
	return true
}

func handleHget(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR HGET requires 2 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	value, ok := c.kv.HGet(key, string(args[1]))
	if !ok {
		c.rconn.WriteNullString()
	} else {
		c.rconn.WriteString(value)
	}
	return true
}

func handleHmset(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 3 || len(args)%2 == 0 {
		c.rconn.WriteError(fmt.Errorf("ERR HMSET requires field/value pairs"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	fields := make(map[string]string, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		fields[string(args[i])] = string(args[i+1])
	}
	c.kv.HMSet(key, fields)
	c.rconn.WriteOK()
	return true
}

func handleHmget(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 2 {
		c.rconn.WriteError(fmt.Errorf("ERR HMGET requires at least 2 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	fields := make([]string, len(args)-1)
	for i := range fields {
		fields[i] = string(args[i+1])
	}
	c.rconn.WriteArray(c.kv.HMGet(key, fields...))
	return true
}

func handleHgetall(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR HGETALL requires 1 argument"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	c.rconn.WriteArrayString(c.kv.HGetAll(key))
	return true
}

func handleHdel(c *connection, cmd string, args [][]byte) bool {
	if len(args) < 2 {
		c.rconn.WriteError(fmt.Errorf("ERR HDEL requires at least 2 arguments"))
		return true
	}
	key := string(args[0])
	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}
	fields := make([]string, len(args)-1)
	for i := range fields {
		fields[i] = string(args[i+1])
	}
	c.rconn.WriteInt(c.kv.HDel(key, fields...))
	return true
}
