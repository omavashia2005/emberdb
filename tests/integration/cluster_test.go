package integration

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/bytechan/resp3"
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

func run(t *testing.T, addr string, args ...string) any {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := resp3.NewWriter(conn).WriteCommand(args...); err != nil {
		t.Fatal(err)
	}
	value, _, err := resp3.NewReader(conn).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if value.Err != "" {
		t.Fatalf("%s: %s", args[0], value.Err)
	}
	return value.SmartResult()
}
