package server

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Fusl/go-resp"
	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

// commandServer drives dispatch directly: replies are written into a buffer by a
// writer-only resp.Server, then decoded. This exercises the single-threaded command
// path without a socket or event loop.
type commandServer struct {
	h   *emberHandler
	cs  *connState
	buf *bytes.Buffer
}

func newCommandServer(kv *kvstore.KVStore, clusterEnabled bool) *commandServer {
	buf := &bytes.Buffer{}
	return &commandServer{
		h:   &emberHandler{kv: kv, clusterEnabled: clusterEnabled},
		cs:  &connState{enc: resp.NewWriter(buf), subs: make(map[string]struct{})},
		buf: buf,
	}
}

func startClusterCommandServer(tb testing.TB, state *clusters.ClusterState) *commandServer {
	tb.Helper()
	serverState = state
	return newCommandServer(kvstore.NewKVStore(true), true)
}

func startCommandServer(tb testing.TB) *commandServer {
	tb.Helper()
	return newCommandServer(kvstore.NewKVStore(), false)
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
	s.buf.Reset()
	args, _, err := resp.ParseCommand(payload)
	if err != nil {
		tb.Fatal(err)
	}
	s.h.dispatch(s.cs, args)
	value, _, err := resp3.NewReader(s.buf).ReadValue()
	if err != nil {
		tb.Fatal(err)
	}
	return value.SmartResult()
}

func TestDisabledPersistenceCommands(t *testing.T) {
	t.Setenv("EMBERDB_PERSISTENCE", "0")
	kv, err := kvstore.OpenPersistent(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatal(err)
	}
	server := newCommandServer(kv, false)
	for _, name := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF"} {
		server.buf.Reset()
		args, _, err := resp.ParseCommand(command(name))
		if err != nil {
			t.Fatal(err)
		}
		server.h.dispatch(server.cs, args)
		value, _, err := resp3.NewReader(server.buf).ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if value.Err == "" {
			t.Fatalf("%s succeeded with persistence disabled", name)
		}
	}
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

func TestPingPong(t *testing.T) {
	server := startCommandServer(t)
	if got := server.run(t, command("PING")); got != "PONG" {
		t.Fatalf("PING = %#v, want PONG", got)
	}
}

// Redis mapping: CLUSTER SLOTS reports slot ranges with routable primary addresses.
// Relevant because cluster clients discover primaries from this response and follow MOVED errors.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/commands/cluster-slots.md
func TestClusterSlotsAndMovedAddress(t *testing.T) {
	self := clusters.NewNode(6379, "ember-1", 0, false)
	self.AddSlotRange(0, 8191)
	other := clusters.NewNode(6379, "ember-2", 0, false)
	other.AddSlotRange(8192, clusters.CLUSTER_SLOTS-1)
	state := &clusters.ClusterState{
		Self:      self,
		Nodes:     map[string]*clusters.ClusterNode{self.GetName(): self, other.GetName(): other},
		Importing: make(map[int]*clusters.ClusterNode),
		Migrating: make(map[int]*clusters.ClusterNode),
	}
	server := startClusterCommandServer(t, state)

	wantSlots := fmt.Sprintf("[[0 8191 [ember-1 6379 %s]] [8192 16383 [ember-2 6379 %s]]]", self.GetName(), other.GetName())
	if got := fmt.Sprint(server.run(t, command("CLUSTER", "SLOTS"))); got != wantSlots {
		t.Fatalf("CLUSTER SLOTS = %s, want %s", got, wantSlots)
	}

	key := ""
	for i := 0; ; i++ {
		candidate := "moved:" + strconv.Itoa(i)
		if kvstore.SlotForKey(candidate) >= 8192 {
			key = candidate
			break
		}
	}
	wantMoved := fmt.Sprintf("MOVED %d ember-2:6379", kvstore.SlotForKey(key))
	if got := server.run(t, command("GET", key)); got != wantMoved {
		t.Fatalf("GET redirect = %q, want %q", got, wantMoved)
	}
}
