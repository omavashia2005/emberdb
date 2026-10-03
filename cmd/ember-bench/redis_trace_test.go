package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedisCPUProfile(t *testing.T) {
	addr := os.Getenv("REDIS_PROFILE_ADDR")
	if addr == "" {
		t.Skip("set REDIS_PROFILE_ADDR to a profiled Redis container")
	}
	var entries []traceEntry
	recorder := traceRecorder{dir: t.TempDir(), mode: "standalone", addrs: []string{addr}, entries: &entries}
	workers := newWorkers(4)
	defer closeWorkers(workers)
	batch := profileBatch{recorder: recorder, requests: 4000, cases: []traceCase{{
		command: "GET", variant: "100% miss", concurrency: 4,
		gen: getGen(newStandaloneRouter(addr), "cpu-profile", 0),
	}}}
	if err := batch.runRedis(workers); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(recorder.dir, "redis", "standalone", "GET_miss_c4_standalone.trace")
	if len(entries) != 1 || entries[0].Product != "Redis" || entries[0].Node != "standalone" {
		t.Fatalf("unexpected trace index: %+v", entries)
	}
	if stat, err := os.Stat(path); err != nil || stat.Size() == 0 {
		t.Fatalf("missing CPU profile %s: %v", path, err)
	}
	top, err := exec.Command("pprof", "-top", path).CombinedOutput()
	if err != nil || !strings.Contains(string(top), "aeProcessEvents") {
		t.Fatalf("Redis CPU profile lacks symbolized server stacks: %s (%v)", top, err)
	}
}
