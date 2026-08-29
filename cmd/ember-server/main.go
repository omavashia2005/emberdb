package server

import (
	"fmt"
	"log"
	"net"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/clusters"
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

var ps = pubsub.NewPubSub()
var startTime = time.Now()

func handleConnection(conn net.Conn, kv *kvstore.KVStore, clusterEnabled bool, clusterState *clusters.ClusterState) {
	defer conn.Close()

	rconn := resp.NewServer(conn)
	defer rconn.Close()

	if err := rconn.SetOptions(resp.ServerOptions{
		MaxMultiBulkLength: resp.Pointer(1024),
		MaxBulkLength:      resp.Pointer(65536),
		MaxBufferSize:      resp.Pointer(1048576),
	}); err != nil {
		rconn.CloseWithError(err)
	}

	kv.Clients[conn.RemoteAddr().String()] = conn

	for {
		args, err := rconn.Next()

		if err != nil {
			rconn.CloseWithError(err)
			log.Printf("closed connection from %s during read: %v", conn.RemoteAddr(), err)
			return
		}

		cmd := bstring(BytesToLower(args[0]))
		args = args[1:]

		fmt.Printf("%s ", cmd)
		for i := range len(args) {
			fmt.Printf("%s\n", args[i])
		}

		switch cmd {
		case "flushall":
			kv.FlushAll()
			rconn.WriteOK()

		case "ping":
			if clusterEnabled {

				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", clusterState.Self.ID))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", clusterState.Self.ClientPort))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", clusterState.Self.ClusterBusPort))

			} else {
				rconn.WriteStatusString("PONG")
			}
		case "echo":
			if len(args) == 0 {
				rconn.WriteError(fmt.Errorf("wrong number of arguments for 'ECHO' command"))
				continue
			}
			if len(args) == 1 {
				// Write a bulk string response
				rconn.WriteBytes(args[0])
				continue
			}

			rconn.WriteArrayBytes(args)
		case "set":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'SET' command"))
				continue
			}

			key := string(args[0])
			val := string(args[1])

			// if clusterEnabled {
			// 	// hash the key
			// 	// calculate slot
			// 	// MOVE or execute
			//
			// 	/*
			// 		Each node needs:
			// 			* Slots it owns
			// 			* Which nodes own which slots
			// 	*/
			//
			// 	clusters.GetNodeFromHash(key, clusterState)
			//
			// } else {
			// 	kv.Set(key, val)
			// }

			kv.Set(key, val)

			rconn.WriteOK()
		case "get":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'GET' command"))
				continue
			}

			key := string(args[0])
			val := kv.Get(key)

			if val == "(nil)" {
				rconn.WriteStatusString("No such key")
				continue
			}

			rconn.WriteString(val)
		case "append":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'APPEND' command"))
				continue
			}

			key := string(args[0])
			valueToAppend := string(args[1])

			kv.Append(key, valueToAppend)

			rconn.WriteOK()

		case "incr":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCR' command"))
				continue
			}

			key := string(args[0])

			err := kv.Incr(key)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "incrby":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'INCRBY' command"))
				continue
			}

			key := string(args[0])
			incrByVal := string(args[1])

			err := kv.IncrBy(key, incrByVal)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "decr":
			if len(args) != 1 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECR' command"))
				continue
			}

			key := string(args[0])
			err := kv.Decr(key)

			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()

		case "decrby":
			if len(args) != 2 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'DECRBY' command"))
				continue
			}

			key := string(args[0])
			decrByVal := string(args[1])

			err := kv.DecrBy(key, decrByVal)
			if err != nil {
				rconn.WriteError(fmt.Errorf("ERR value is not an integer"))
			}

			rconn.WriteOK()
		case "mset":
			if len(args)%2 != 0 {
				rconn.WriteError(fmt.Errorf("ERR Wrong number of arguments for 'MSET' command"))
				continue
			}

			for i := 0; i < len(args); i += 2 {
				kv.Set(string(args[i]), string(args[i+1]))
			}

			rconn.WriteOK()

		case "mget":
			if len(args) == 0 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'MGET' command"))
				continue
			}

			var resp []string

			for i := 0; i < len(args); i += 1 {
				resp = append(resp, kv.Get(string(args[i])))
			}

			rconn.WriteArrayString(resp)

		case "publish":
			if len(args) == 3 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'PUBLISH' command"))
				continue
			}

			for i := range len(args) - 1 {
				channel := string(args[i])
				msg := string(args[i+1])
				count := pubsub.Publish(channel, msg, ps)
				rconn.WriteInt(count)
			}

		case "subscribe":
			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SUBSCRIBE' command"))
				continue
			}

			for i := range args {

				channel := string(args[i])
				ch := pubsub.Subscribe(channel, ps)

				go func() {

					for message := range ch {
						fmt.Printf("Received message on channel %s: %s\n", channel, message)
						rconn.WriteString(message)
					}

				}()

				rconn.WriteOK()
			}

		case "unsubscribe":
			if len(args) == 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'UNSUBSCRIBE' command"))
				continue
			}

		case "cluster":
			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
				continue
			}

			switch string(args[1]) {

			case "ADDSLOTS":
				if len(args) != 4 {
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTS' command"))
					continue
				}

				// slotStart := strings(args[2])
				// slotEnd := strings(args[3])

				fmt.Printf("Adding slots to node on port %s\n", clusterState.Self.ClientPort)

				rconn.WriteOK()

			default:

				rconn.WriteError(fmt.Errorf("NO SUCH COMMAND"))
				continue
			}

		default:
			rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
		}
	}

}
func Run(port string, clusterEnabled bool) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}

	defer listener.Close()
	kv := kvstore.NewKVStore()

	clusterState := &clusters.ClusterState{
		Nodes: make(map[string]*clusters.ClusterNode),
	}

	initNode := clusters.NewNode(port, clusterState)
	clusterState.Nodes[initNode.ID] = initNode
	clusterState.Self = initNode

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		log.Printf("opened connection from %s", conn.RemoteAddr())

		go handleConnection(conn, kv, clusterEnabled, clusterState)
	}

}
