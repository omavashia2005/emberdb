package server

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
	"github.com/omavashia2005/emberdb/utils/pubsub"
)

var serverState *clusters.ClusterState

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

func handleConnection(conn net.Conn, kv *kvstore.KVStore, clusterEnabled bool) {
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

		switch cmd {
		case "flushall":
			kv.FlushAll()
			rconn.WriteOK()

		case "ping":
			if clusterEnabled {

				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", serverState.Self.Name))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", serverState.Self.ClientPort))
				rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", serverState.Self.ClusterBusPort))

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
			if !clusterEnabled {
				rconn.WriteError(fmt.Errorf("Clustering is not enabled"))
				continue
			}

			if len(args) < 1 {
				rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER' command"))
				continue
			}

			switch string(args[0]) {

			case "ADDSLOTSRANGE":
				if len(args) != 3 {
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER ADDSLOTSRANGE' command"))
					continue
				}

				slotStart, err := strconv.Atoi(string(args[1]))
				if err != nil {
					panic(err)
				}
				slotEnd, err := strconv.Atoi(string(args[2]))
				if err != nil {
					panic(err)
				}

				fmt.Printf("[DEBUG] Adding slots %d - %d to node on port %s\n", slotStart, slotEnd, serverState.Self.ClientPort)

				serverState.Self.NumSlots = 0
				for slot := slotStart; slot <= slotEnd; slot++ {
					word := slot / 64
					bit := slot % 64
					serverState.Self.OwnedSlots[word] |= uint64(1) << bit
					serverState.Self.NumSlots++
				}

				rconn.WriteOK()

			case "MEET":
				if len(args) != 3 {
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'CLUSTER MEET' command"))
					continue
				}

				senderHost := string(args[1])
				senderPort, err := strconv.Atoi(string(args[2]))
				if err != nil {
					panic(err)
				}

				if err := clusters.ClusterStartHandshake(senderHost, senderPort, serverState); err != nil {
					panic(err)
				}

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
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	kv := kvstore.NewKVStore()

	serverState = &clusters.ClusterState{
		Nodes: make(map[string]*clusters.ClusterNode),
	}

	self := clusters.NewNode(port, serverState)
	serverState.Nodes[self.Name] = self
	serverState.Self = self

	if clusterEnabled {
		// Cluster bus listener
		clusterListener, err := net.Listen(
			"tcp",
			fmt.Sprintf(":%d", self.ClusterBusPort),
		)
		if err != nil {
			panic(err)
		}

		go func() {
			defer clusterListener.Close()

			for {
				conn, err := clusterListener.Accept()
				if err != nil {
					return
				}

				fmt.Printf(
					"[CLUSTER] inbound connection from %s\n",
					conn.RemoteAddr(),
				)

				clusters.CreateClusterLink(conn, nil, true)
			}
		}()

		// Cluster cron
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			iterations := 0

			for range ticker.C {
				clusters.ClusterCron(serverState, iterations)
				iterations++
			}
		}()
	}

	// Normal client connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		log.Printf("opened connection from %s", conn.RemoteAddr())

		go handleConnection(conn, kv, clusterEnabled)
	}
}
