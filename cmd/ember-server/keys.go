package server

import "fmt"

func handleFlushall(c *connection, cmd string, args [][]byte) bool {
	c.kv.FlushAll()
	c.rconn.WriteOK()
	return true
}

func handleDel(c *connection, cmd string, args [][]byte) bool {
	if len(args) == 1 {
		key := string(args[0])

		if c.kv.Delete(key) != 1 {
			c.rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
			return true
		}

	} else {
		for _, k := range args {
			key := string(k)

			if c.kv.Delete(key) != 1 {
				c.rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
				continue
			}

		}
	}

	c.rconn.WriteOK()
	return true
}
