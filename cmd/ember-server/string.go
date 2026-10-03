package server

import (
	"fmt"

	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handleSet(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'SET' command"))
		return true
	}

	key := string(args[0])
	val := string(args[1])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	c.kv.Set(key, val)
	c.rconn.WriteOK()
	return true
}

func handleGet(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'GET' command"))
		return true
	}

	key := string(args[0])
	val := c.kv.Get(key)

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	if val == "(nil)" {
		c.rconn.WriteStatusString("No such key")
		return true
	}

	c.rconn.WriteString(val)
	return true
}

func handleAppend(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'APPEND' command"))
		return true
	}

	key := string(args[0])
	valueToAppend := string(args[1])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	c.kv.Append(key, valueToAppend)

	c.rconn.WriteOK()
	return true
}

func handleIncr(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCR' command"))
		return true
	}

	key := string(args[0])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	err := c.kv.Incr(key)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	c.rconn.WriteOK()
	return true
}

func handleIncrby(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCRBY' command"))
		return true
	}

	key := string(args[0])
	incrByVal := string(args[1])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	err := c.kv.IncrBy(key, incrByVal)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	c.rconn.WriteOK()
	return true
}

func handleDecr(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 1 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECR' command"))
		return true
	}

	key := string(args[0])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	err := c.kv.Decr(key)

	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	c.rconn.WriteOK()
	return true
}

func handleDecrby(c *connection, cmd string, args [][]byte) bool {
	if len(args) != 2 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECRBY' command"))
		return true
	}

	key := string(args[0])

	if c.clusterEnabled && toMoveorNotToMove(key, c.rconn, c.kv) != "OK" {
		return true
	}

	decrByVal := string(args[1])

	err := c.kv.DecrBy(key, decrByVal)
	if err != nil {
		c.rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
	}

	c.rconn.WriteOK()
	return true
}

func handleMset(c *connection, cmd string, args [][]byte) bool {
	if len(args) == 0 || len(args)%2 != 0 {
		c.rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'MSET' command"))
		return true
	}

	firstKey := string(args[0])

	if c.clusterEnabled {
		firstSlot := kvstore.SlotForKey(firstKey)

		validQuery := true

		for i := 2; i < len(args); i += 2 {
			key := string(args[i])
			slot := kvstore.SlotForKey(key)

			if slot != firstSlot {
				c.rconn.WriteError(
					fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
				)
				validQuery = false
				break
			}
		}

		if !validQuery {
			return true
		}

		// Since every key hashes to the same slot,
		// checking the first key is sufficient.
		if toMoveorNotToMove(firstKey, c.rconn, c.kv) != "OK" {
			return true
		}
	}

	for i := 0; i < len(args); i += 2 {
		key, val := string(args[i]), string(args[i+1])
		c.kv.Set(key, val)
	}

	c.rconn.WriteOK()
	return true
}

func handleMget(c *connection, cmd string, args [][]byte) bool {
	if len(args) == 0 {
		c.rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MGET' command"))
		return true
	}

	validQuery := true

	if c.clusterEnabled {
		firstKey := string(args[0])

		firstSlot := kvstore.SlotForKey(firstKey)

		for i := 1; i < len(args); i++ {
			key := string(args[i])

			if kvstore.SlotForKey(key) != firstSlot {
				c.rconn.WriteError(
					fmt.Errorf("CROSSSLOT Keys in request don't hash to the same slot"),
				)
				validQuery = false
				break
			}
		}

		if !validQuery {
			return true
		}

		if toMoveorNotToMove(firstKey, c.rconn, c.kv) != "OK" {
			return true
		}
	}

	var resp []string

	for i := 0; i < len(args); i++ {
		key := string(args[i])
		resp = append(resp, c.kv.Get(key))
	}

	c.rconn.WriteArrayString(resp)
	return true
}
