package server

import (
	"fmt"

	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

func handlePing(rconn *resp.Server, clusterEnabled bool) {
	if clusterEnabled {
		self := serverState.Self.Snapshot()
		rconn.WriteStatusString(fmt.Sprintf("PONG from %s\n", self.Name))
		rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClientPort))
		rconn.WriteStatusString(fmt.Sprintf("PONG from %d\n", self.ClusterBusPort))
	} else {
		rconn.WriteStatusString("PONG")
	}
}

func handleEcho(rconn *resp.Server, args [][]byte) {
	if len(args) == 0 {
		rconn.WriteError(fmt.Errorf("wrong number of arguments for 'ECHO' command"))
		return
	}
	if len(args) == 1 {
		rconn.WriteBytes(args[0])
		return
	}

	rconn.WriteArrayBytes(args)
}

func handleDelete(rconn *resp.Server, kv *kvstore.KVStore, args [][]byte) {
	if len(args) == 1 {
		key := string(args[0])

		if kv.Delete(key) != 1 {
			rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
			return
		}

	} else {
		for _, k := range args {
			key := string(k)

			if kv.Delete(key) != 1 {
				rconn.WriteError(fmt.Errorf("ERROR deleting key\n"))
				continue
			}

		}
	}

	rconn.WriteOK()
}
