package server

import (
	"fmt"
	"log"
	"net"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
	"github.com/omavashia2005/emberdb/utils/pubsub"
)

func BytesToLower(b []byte) []byte {
	for i := 0; i < len(b); i++ {
		c := b[i]
		if c >= 'A' && c <= 'Z' {
			b[i] = c + 32
		}
	}
	return b
}

func bstring(bs []byte) string {
	p := unsafe.SliceData(bs)
	return unsafe.String(p, len(bs))
}

type connection struct {
	rconn          *resp.Server
	kv             *kvstore.KVStore
	clusterEnabled bool
	subscriptions  map[string][]chan string
}

func handleConnection(conn net.Conn, kv *kvstore.KVStore, clusterEnabled bool) {
	defer conn.Close()
	subscriptions := make(map[string][]chan string)
	defer func() {
		for channel, subscribers := range subscriptions {
			for _, subscriber := range subscribers {
				pubsub.Unsubscribe(channel, subscriber, ps)
			}
		}
	}()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

	if err := rconn.SetOptions(resp.ServerOptions{
		MaxMultiBulkLength: resp.Pointer(1024),
		MaxBulkLength:      resp.Pointer(65536),
		MaxBufferSize:      resp.Pointer(1048576),
	}); err != nil {
		rconn.CloseWithError(err)
	}

	c := &connection{rconn: rconn, kv: kv, clusterEnabled: clusterEnabled, subscriptions: subscriptions}

	for {
		args, err := rconn.Next()
		if err != nil {
			rconn.CloseWithError(err)
			log.Printf("closed connection from %s during read: %v", conn.RemoteAddr(), err)
			return
		}

		if len(args) == 0 {
			rconn.WriteError(fmt.Errorf("ERR empty command"))
			continue
		}
		cmd := bstring(BytesToLower(args[0]))
		handler, ok := commandMap[cmd]
		if !ok {
			rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
			continue
		}
		if !handler(c, cmd, args[1:]) {
			return
		}
	}
}

func handlePing(c *connection, cmd string, args [][]byte) bool {
	if c.clusterEnabled {
		self := serverState.Self.Snapshot()
		c.rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", self.Name))
		c.rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClientPort))
		c.rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClusterBusPort))
	} else {
		c.rconn.WriteStatusString("PONG")
	}
	return true
}

func handleEcho(c *connection, cmd string, args [][]byte) bool {
	if len(args) == 0 {
		c.rconn.WriteError(fmt.Errorf("wrong number of arguments for 'ECHO' command"))
		return true
	}
	if len(args) == 1 {
		// Write a bulk string response
		c.rconn.WriteBytes(args[0])
		return true
	}

	c.rconn.WriteArrayBytes(args)
	return true
}
