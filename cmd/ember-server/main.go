package server

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"time"
	"unsafe"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils"
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
			handlePing(rconn, clusterEnabled)
		case "echo":
			handleEcho(rconn, args)
		case "lpush", "rpush":
			handlePush(rconn, kv, args, clusterEnabled, cmd)
		case "lpop", "rpop":
			handlePop(rconn, kv, args, clusterEnabled, cmd)
		case "lrange":
			handleLRange(rconn, kv, args, clusterEnabled)
		case "llen":
			handleLLen(rconn, kv, args, clusterEnabled)
		case "hset":
			handleHSet(rconn, kv, args, clusterEnabled)
		case "hget":
			handleHGet(rconn, kv, args, clusterEnabled)
		case "hmset":
			handleHMSet(rconn, kv, args, clusterEnabled)
		case "hmget":
			handleHMGet(rconn, kv, args, clusterEnabled)
		case "hgetall":
			handleHGetAll(rconn, kv, args, clusterEnabled)
		case "hdel":
			handleHDel(rconn, kv, args, clusterEnabled)
		case "sadd", "srem":
			handleSetMembers(rconn, kv, args, clusterEnabled, cmd)
		case "smembers":
			handleSMembers(rconn, kv, args, clusterEnabled)
		case "sismember":
			handleSIsMember(rconn, kv, args, clusterEnabled)
		case "zadd":
			handleZAdd(rconn, kv, args, clusterEnabled)
		case "zrange":
			handleZRange(rconn, kv, args, clusterEnabled)
		case "zrem":
			handleZRem(rconn, kv, args, clusterEnabled)
		case "set":
			handleSet(rconn, kv, args, clusterEnabled)
		case "get":
			handleGet(rconn, kv, args, clusterEnabled)
		case "append":
			handleAppend(rconn, kv, args, clusterEnabled)
		case "incr":
			handleIncr(rconn, kv, args, clusterEnabled)
		case "incrby":
			handleIncrBy(rconn, kv, args, clusterEnabled)
		case "decr":
			handleDecr(rconn, kv, args, clusterEnabled)
		case "decrby":
			handleDecrBy(rconn, kv, args, clusterEnabled)
		case "mset":
			handleMSet(rconn, kv, args, clusterEnabled)
		case "mget":
			handleMGet(rconn, kv, args, clusterEnabled)
		case "publish":
			handlePublish(rconn, args)
		case "subscribe":
			handleSubscribe(rconn, args, subscriptions)
		case "unsubscribe":
			handleUnsubscribe(rconn, args, subscriptions)
		case "del", "delete":
			handleDelete(rconn, kv, args)
		case "getkeysinslot":
			handleGetKeysInSlot(rconn, kv, args)
		case "restore-asking":
			handleRestoreAsking(rconn, kv, args)
		case "setslot":
			handleSetSlot(rconn, args)
		case "migrate":
			handleMigrate(rconn, kv, args)
		case "cluster":
			if err := handleCluster(rconn, args, clusterEnabled); err != nil {
				return
			}
		default:
			rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
		}
	}
}

func Run(port string, clusterHost string, clusterEnabled bool) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		utils.PrintError(fmt.Errorf("%w: listen on port %s: %v", utils.ErrStartup, port, err))
		return
	}
	defer listener.Close()

	pprofAddr := os.Getenv("EMBERDB_PPROF_ADDR")
	if pprofAddr == "" {
		pprofAddr = "127.0.0.1:6060"
	}
	pprofListener, err := net.Listen("tcp", pprofAddr)
	if err != nil {
		log.Printf("pprof listen on %s: %v", pprofAddr, err)
	} else {
		defer pprofListener.Close()
		log.Printf("pprof listening on %s", pprofListener.Addr())
		go func() {
			if err := http.Serve(pprofListener, nil); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Printf("pprof server: %v", err)
			}
		}()
	}

	kv := kvstore.NewKVStore(clusterEnabled)

	if clusterEnabled {
		serverState = &clusters.ClusterState{
			Nodes:     make(map[string]*clusters.ClusterNode),
			Migrating: make(map[int]*clusters.ClusterNode),
			Importing: make(map[int]*clusters.ClusterNode),
		}

		clusters.InitClusterState(serverState)

		clientPort, err := strconv.Atoi(port)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: invalid client port %q: %v", utils.ErrInvalidInput, port, err))
			return
		}
		self := clusters.NewNode(clientPort, clusterHost, 0, false)
		serverState.SetNode(self)
		serverState.Self = self

		// Cluster bus listener
		clusterBusListener, err := net.Listen(
			"tcp",
			fmt.Sprintf(":%d", self.GetClusterBusPort()),
		)
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: listen on cluster bus: %v", utils.ErrStartup, err))
			return
		}

		go func() {
			defer clusterBusListener.Close()

			for {
				busConn, err := clusterBusListener.Accept()
				if err != nil {
					return
				}

				clusters.CreateClusterLink(busConn, nil, true)
			}
		}()

		// Cluster cron
		go func() {
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			iterations := 0

			for range ticker.C {
				clusters.ClusterCron(iterations)
				iterations++
			}
		}()
	}

	// Normal client connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			utils.PrintError(fmt.Errorf("%w: accept client: %v", utils.ErrConnection, err))
			return
		}

		log.Printf("opened connection from %s", conn.RemoteAddr())

		go handleConnection(conn, kv, clusterEnabled)
	}
}
