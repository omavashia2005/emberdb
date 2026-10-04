package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

// Redis mapping: "It is possible to write and read from the cluster" and
// hash-tagged MSET coverage. This test runs only against the Docker clusters
// started by `make test-cluster`.
// Sources:
//   - https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/cluster/tests/00-base.tcl#L62-L64
//   - https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/type/string.tcl#L227-L230
func TestDockerClusters(t *testing.T) {
	ember := os.Getenv("EMBER_CLUSTER_ADDR")
	redis := os.Getenv("REDIS_CLUSTER_ADDR")
	if ember == "" || redis == "" {
		t.Skip("run with: make test-cluster")
	}

	for name, addr := range map[string]string{"EmberDB": ember, "Redis": redis} {
		t.Run(name+"/SET_GET", func(t *testing.T) {
			if got := run(t, addr, "SET", "docker:test{bar}", "value"); got != "OK" {
				t.Fatalf("SET = %#v", got)
			}
			if got := run(t, addr, "GET", "docker:test{bar}"); got != "value" {
				t.Fatalf("GET = %#v", got)
			}
		})

		t.Run(name+"/MSET_MGET", func(t *testing.T) {
			if got := run(t, addr, "MSET", "docker:a{bar}", "one", "docker:b{bar}", "two"); got != "OK" {
				t.Fatalf("MSET = %#v", got)
			}
			if got := fmt.Sprint(run(t, addr, "MGET", "docker:a{bar}", "docker:b{bar}")); got != "[one two]" {
				t.Fatalf("MGET = %s", got)
			}
		})

		// Concurrent load against the live cluster: each worker owns disjoint
		// keys, so any failure here is either a data race (caught by -race on
		// the ember-server build when RACE=1) or corrupted/lost writes, not
		// legitimate contention.
		t.Run(name+"/SET_GET_concurrent", func(t *testing.T) {
			const workers, itersPerWorker = 50, 20
			var wg sync.WaitGroup
			errs := make(chan string, workers*itersPerWorker)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < itersPerWorker; i++ {
						key := fmt.Sprintf("docker:race:set:%d:%d{bar}", w, i)
						val := fmt.Sprintf("v%d-%d", w, i)
						if got, err := runErr(addr, "SET", key, val); err != nil || got != "OK" {
							errs <- fmt.Sprintf("SET(%s) = %#v, err=%v", key, got, err)
							continue
						}
						if got, err := runErr(addr, "GET", key); err != nil || got != val {
							errs <- fmt.Sprintf("GET(%s) = %#v, err=%v, want %s", key, got, err, val)
						}
					}
				}(w)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				t.Error(e)
			}
		})

		t.Run(name+"/MSET_MGET_concurrent", func(t *testing.T) {
			const workers, itersPerWorker = 50, 20
			var wg sync.WaitGroup
			errs := make(chan string, workers*itersPerWorker)
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < itersPerWorker; i++ {
						// Fixed {bar} tag keeps every key on the slot the test
						// dials directly; the harness doesn't follow MOVED.
						keyA := fmt.Sprintf("docker:race:a:%d:%d{bar}", w, i)
						keyB := fmt.Sprintf("docker:race:b:%d:%d{bar}", w, i)
						valA, valB := fmt.Sprintf("a%d-%d", w, i), fmt.Sprintf("b%d-%d", w, i)
						if got, err := runErr(addr, "MSET", keyA, valA, keyB, valB); err != nil || got != "OK" {
							errs <- fmt.Sprintf("MSET(%d:%d) = %#v, err=%v", w, i, got, err)
							continue
						}
						want := fmt.Sprintf("[%s %s]", valA, valB)
						if got, err := runErr(addr, "MGET", keyA, keyB); err != nil || fmt.Sprint(got) != want {
							errs <- fmt.Sprintf("MGET(%d:%d) = %#v, err=%v, want %s", w, i, got, err, want)
						}
					}
				}(w)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				t.Error(e)
			}
		})
	}

	t.Run("EmberDB/topology", func(t *testing.T) {
		payload, ok := run(t, ember, "CLUSTER", "NODES").(string)
		if !ok {
			t.Fatal("CLUSTER NODES did not return a string")
		}
		var nodes map[string]struct{ NumSlots int }
		if err := json.Unmarshal([]byte(payload), &nodes); err != nil {
			t.Fatal(err)
		}
		total := 0
		for _, node := range nodes {
			total += node.NumSlots
		}
		if len(nodes) != 3 || total != 16384 {
			t.Fatalf("cluster topology = %d nodes, %d slots", len(nodes), total)
		}
	})

	t.Run("Redis/topology", func(t *testing.T) {
		info, ok := run(t, redis, "CLUSTER", "INFO").(string)
		if !ok || !strings.Contains(info, "cluster_state:ok") {
			t.Fatalf("CLUSTER INFO = %#v", info)
		}
	})
}
func TestMGetIsAtomic(t *testing.T) {
	kv := kvstore.NewKVStore(true)
	kv.Mset([]string{"{t}a", "{t}b"}, []string{"0", "0"})

	stop := make(chan struct{})
	go func() {
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			v := strconv.Itoa(i)
			kv.Mset([]string{"{t}a", "{t}b"}, []string{v, v})
		}
	}()
	defer close(stop)

	for n := 0; n < 1_000_000; n++ {
		got := kv.Mget([]string{"{t}a", "{t}b"})
		if got[0] != got[1] {
			t.Fatalf("torn MGET after %d reads: a=%s b=%s", n, got[0], got[1])
		}
	}
}
func run(t *testing.T, addr string, args ...string) any {
	t.Helper()
	got, err := runErr(addr, args...)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// runErr is run's error-returning twin, safe to call from worker goroutines
// where t.Fatal must not be used.
func runErr(addr string, args ...string) (any, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := resp3.NewWriter(conn).WriteCommand(args...); err != nil {
		return nil, err
	}
	value, _, err := resp3.NewReader(conn).ReadValue()
	if err != nil {
		return nil, err
	}
	if value.Err != "" {
		return nil, fmt.Errorf("%s: %s", args[0], value.Err)
	}
	return value.SmartResult(), nil
}
