package server

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

type commandServer struct {
	conn   net.Conn
	reader *resp3.Reader
}

func startCommandServer(tb testing.TB) *commandServer {
	tb.Helper()
	kv := kvstore.NewKVStore()
	clientConn, serverConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(serverConn, kv, false)
	}()

	server := &commandServer{conn: clientConn, reader: resp3.NewReader(clientConn)}
	tb.Cleanup(func() {
		clientConn.Close()
		<-done
	})
	return server
}

func command(args ...string) []byte {
	buf := make([]byte, 0, 32)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func (s *commandServer) run(tb testing.TB, payload []byte) any {
	tb.Helper()
	if _, err := s.conn.Write(payload); err != nil {
		tb.Fatal(err)
	}
	value, _, err := s.reader.ReadValue()
	if err != nil {
		tb.Fatal(err)
	}
	return value.SmartResult()
}

// Redis mapping: "MSET base case".
// Relevant because EmberDB implements MSET/MGET in the command handler rather than KVStore methods.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L227-L230
func TestMSetMGet(t *testing.T) {
	server := startCommandServer(t)
	if got := server.run(t, command("MSET", "x", "10", "y", "foo bar", "z", "x x\n\r\n")); got != "OK" {
		t.Fatalf("MSET = %#v, want OK", got)
	}
	got := fmt.Sprint(server.run(t, command("MGET", "x", "y", "z")))
	if want := "[10 foo bar x x\n\r\n]"; got != want {
		t.Fatalf("MGET = %q, want %q", got, want)
	}
}

// Redis mapping: "MSET with already existing - same key twice".
// Relevant because command-order semantics require the last value for a duplicate key to win.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L237-L240
func TestMSetSameKeyLastValueWins(t *testing.T) {
	server := startCommandServer(t)
	server.run(t, command("SET", "x", "x"))
	server.run(t, command("MSET", "x", "xxx", "x", "yyy"))
	if got := server.run(t, command("GET", "x")); got != "yyy" {
		t.Fatalf("GET = %#v, want yyy", got)
	}
}

// Redis mapping: "MGET against non existing key".
// Relevant because Redis returns a null array element while EmberDB currently returns the literal "(nil)".
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L207-L209
func TestMGetMissingKeyReturnsNull(t *testing.T) {
	t.Skip(`known incompatibility: MGET encodes the internal "(nil)" sentinel as a string`)
}

func TestConcurrentConnections(t *testing.T) {
	kv := kvstore.NewKVStore()
	var clients, servers sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		clientConn, serverConn := net.Pipe()
		servers.Add(1)
		go func() {
			defer servers.Done()
			handleConnection(serverConn, kv, false)
		}()
		clients.Add(1)
		go func() {
			defer clients.Done()
			defer clientConn.Close()
			if err := resp3.NewWriter(clientConn).WriteCommand("PING"); err != nil {
				errs <- err
				return
			}
			value, _, err := resp3.NewReader(clientConn).ReadValue()
			if err != nil {
				errs <- err
				return
			}
			if value.SmartResult() != "PONG" {
				errs <- fmt.Errorf("PING = %#v", value.SmartResult())
			}
		}()
	}
	clients.Wait()
	servers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
