package main

import (
	"fmt"
	"github.com/Fusl/go-resp"
	"github.com/omavashia2005/emberdb/utils/kvstore"
	"log"
	"net"
	"unsafe"
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
func main() {
	listener, err := net.Listen("tcp", ":6739")
	if err != nil {
		panic(err)
	}

	defer listener.Close()
	kv := kvstore.NewKVStore()

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		rconn := resp.NewServer(conn)
		defer rconn.Close()

		log.Printf("opened connection from %s", conn.RemoteAddr())

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

			fmt.Println(args)

			switch cmd {
			case "ping":
				rconn.WriteStatusString("PONG")
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
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'SET' command"))
					continue
				}

				key := string(args[0])
				val := string(args[1])

				kv.Set(key, val)

				rconn.WriteOK()
			case "get":
				if len(args) != 1 {
					rconn.WriteError(fmt.Errorf("Wrong number of arguments for 'GET' command"))
					continue
				}

				key := string(args[0])
				val := kv.Get(key)

				if val == "(nil)" {
					rconn.WriteStatusString("No such key")
					continue
				}

				rconn.WriteString(val)

			default:
				rconn.WriteError(fmt.Errorf("unknown command '%s'", cmd))
			}
		}

	}

}
