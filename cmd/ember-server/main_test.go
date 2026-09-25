package server

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"testing"

	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/clusters"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

type commandServer struct {
	conn   net.Conn
	reader *resp3.Reader
}

func startCommandServer(tb testing.TB, clusterEnabled bool) *commandServer {
	tb.Helper()
	kv := kvstore.NewKVStore(clusterEnabled)
	clientConn, serverConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		handleConnection(serverConn, kv, clusterEnabled)
	}()

	if clusterEnabled {
		self := clusters.NewNode(6379, "127.0.0.1", 0, false)
		self.AddSlotRange(0, clusters.CLUSTER_SLOTS-1)
		serverState = &clusters.ClusterState{
			Self:      self,
			Nodes:     map[string]*clusters.ClusterNode{self.GetName(): self},
			Importing: make(map[int]*clusters.ClusterNode),
			Migrating: make(map[int]*clusters.ClusterNode),
		}
	}

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
	server := startCommandServer(t, false)
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
	server := startCommandServer(t, false)
	server.run(t, command("SET", "x", "x"))
	server.run(t, command("MSET", "x", "xxx", "x", "yyy"))
	if got := server.run(t, command("GET", "x")); got != "yyy" {
		t.Fatalf("GET = %#v, want yyy", got)
	}
}

// Redis mapping: "client can handle keys with hash tag".
// Relevant because the cluster command path must route and store a hash-tagged key successfully.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/15-cluster-slots.tcl#L46-L50
func TestClusterSetGetHashTaggedKey(t *testing.T) {
	server := startCommandServer(t, true)
	if got := server.run(t, command("SET", "foo{tag}", "bar")); got != "OK" {
		t.Fatalf("SET = %#v, want OK", got)
	}
	if got := server.run(t, command("GET", "foo{tag}")); got != "bar" {
		t.Fatalf("GET = %#v, want bar", got)
	}
}

// Redis mapping: "MGET against non existing key".
// Relevant because Redis returns a null array element while EmberDB currently returns the literal "(nil)".
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L207-L209
func TestMGetMissingKeyReturnsNull(t *testing.T) {
	t.Skip(`known incompatibility: MGET encodes the internal "(nil)" sentinel as a string`)
}

// Redis mapping: clustered MSET in "MSET base case" uses {t} to keep every key in one slot.
// Relevant because Redis permits same-slot multi-key commands when the keys share a hash tag.
// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L227-L230
func TestClusterMSetAcceptsSharedHashTag(t *testing.T) {
	server := startCommandServer(t, true)
	if got := server.run(t, command("MSET", "x{tag}", "10", "y{tag}", "foo")); got != "OK" {
		t.Fatalf("MSET = %#v, want OK", got)
	}
	if got := fmt.Sprint(server.run(t, command("MGET", "x{tag}", "y{tag}"))); got != "[10 foo]" {
		t.Fatalf("MGET = %q, want %q", got, "[10 foo]")
	}
}

func runCommandBenchmark(b *testing.B, clusterEnabled bool, setup, payload []byte) {
	b.Helper()
	b.StopTimer()
	logOutput := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(logOutput) })
	server := startCommandServer(b, clusterEnabled)
	if setup != nil {
		server.run(b, setup)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		server.run(b, payload)
	}
	b.StopTimer()
	elapsed := b.Elapsed()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}

func BenchmarkCommands(b *testing.B) {
	// Redis mapping: default SET workload. Same fixed key and three-byte value; one request per response.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1912-L1916
	b.Run("single/SET", func(b *testing.B) {
		runCommandBenchmark(b, false, nil, command("SET", "key", "xxx"))
	})

	// Redis mapping: default GET workload. Relevant as EmberDB's single-key read throughput baseline.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1918-L1921
	b.Run("single/GET", func(b *testing.B) {
		runCommandBenchmark(b, false, command("SET", "key", "xxx"), command("GET", "key"))
	})

	mset := command("MSET", "key:0", "xxx", "key:1", "xxx", "key:2", "xxx", "key:3", "xxx", "key:4", "xxx", "key:5", "xxx", "key:6", "xxx", "key:7", "xxx", "key:8", "xxx", "key:9", "xxx")
	mget := command("MGET", "key:0", "key:1", "key:2", "key:3", "key:4", "key:5", "key:6", "key:7", "key:8", "key:9")

	// Redis mapping: default MSET (10 keys) workload. Relevant as EmberDB's implemented bulk-write variant.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L2024-L2035
	b.Run("single/MSET_10", func(b *testing.B) {
		runCommandBenchmark(b, false, nil, mset)
	})

	// Redis mapping: redis-benchmark arbitrary-command mode applied to MGET with ten keys.
	// Relevant because MGET is EmberDB's implemented bulk-read variant but is absent from Redis's default suite.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1878-L1888
	b.Run("single/MGET_10", func(b *testing.B) {
		runCommandBenchmark(b, false, mset, mget)
	})

	// Redis mapping: default SET workload with --cluster, which adds hash tags and routes to a slot owner.
	// Relevant because EmberDB's cluster-enabled handler adds the same routing and per-slot storage work.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L395-L414
	b.Run("cluster/SET", func(b *testing.B) {
		runCommandBenchmark(b, true, nil, command("SET", "key{tag}", "xxx"))
	})

	// Redis mapping: default GET workload with --cluster and a hash-tagged key.
	// Relevant because it measures EmberDB's routed cluster-read command path.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L395-L414
	b.Run("cluster/GET", func(b *testing.B) {
		runCommandBenchmark(b, true, command("SET", "key{tag}", "xxx"), command("GET", "key{tag}"))
	})

	clusterMSet := command("MSET", "key:0{tag}", "xxx", "key:1{tag}", "xxx", "key:2{tag}", "xxx", "key:3{tag}", "xxx", "key:4{tag}", "xxx", "key:5{tag}", "xxx", "key:6{tag}", "xxx", "key:7{tag}", "xxx", "key:8{tag}", "xxx", "key:9{tag}", "xxx")
	clusterMGet := command("MGET", "key:0{tag}", "key:1{tag}", "key:2{tag}", "key:3{tag}", "key:4{tag}", "key:5{tag}", "key:6{tag}", "key:7{tag}", "key:8{tag}", "key:9{tag}")

	// Redis mapping: default MSET (10 keys) workload with --cluster and one shared hash tag.
	// Relevant because Redis Cluster permits multi-key writes only when all keys resolve to one slot.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L2024-L2035
	b.Run("cluster/MSET_10", func(b *testing.B) {
		runCommandBenchmark(b, true, nil, clusterMSet)
	})

	// Redis mapping: redis-benchmark arbitrary-command mode with --cluster and hash-tagged MGET keys.
	// Relevant because MGET is valid in Redis Cluster only when every requested key shares a slot.
	// Source: https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1610-L1613
	b.Run("cluster/MGET_10", func(b *testing.B) {
		runCommandBenchmark(b, true, clusterMSet, clusterMGet)
	})
}
