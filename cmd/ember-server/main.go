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

	"github.com/omavashia2005/emberdb/utils"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

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
