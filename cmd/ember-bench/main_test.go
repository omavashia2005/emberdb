package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bytechan/resp3"
)

func TestWorkerTimesOutWithoutReply(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	go io.Copy(io.Discard, server)
	before := requestTimeout
	requestTimeout = 20 * time.Millisecond
	defer func() { requestTimeout = before }()
	w := &worker{conns: map[string]*peer{"pipe": {conn: client, writer: resp3.NewWriter(client), reader: resp3.NewReader(client)}}}
	if err := w.do("pipe", []string{"GET", "missing"}); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("missing reply returned %v, want deadline exceeded", err)
	}
}

func TestRouterFromSlots(t *testing.T) {
	ranges := []interface{}{
		[]interface{}{int64(0), int64(8191), []interface{}{"ember-1", int64(6379)}},
		[]interface{}{int64(8192), int64(16383), []interface{}{"ember-2", int64(6379)}},
	}
	r, err := routerFromSlots(ranges)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.addrFor("bench:get:0"); got != "ember-1:6379" && got != "ember-2:6379" {
		t.Fatalf("addrFor returned unrouted address %q", got)
	}
	if r.slotAddr[0] != "ember-1:6379" {
		t.Fatalf("slot 0 = %q, want ember-1:6379", r.slotAddr[0])
	}
	if r.slotAddr[16383] != "ember-2:6379" {
		t.Fatalf("slot 16383 = %q, want ember-2:6379", r.slotAddr[16383])
	}
}

func TestHitRatio(t *testing.T) {
	if !hit(0, 1) || !hit(1, 1) {
		t.Fatal("ratio 1 must always hit")
	}
	if hit(0, 0) || hit(1, 0) {
		t.Fatal("ratio 0 must never hit")
	}
	if !hit(0, 0.5) || hit(1, 0.5) {
		t.Fatal("ratio 0.5 must alternate starting with a hit")
	}
}

func TestPercentileAndMedian(t *testing.T) {
	ms := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if p := percentile(ms, 50); p != 5 {
		t.Fatalf("p50 = %v, want 5", p)
	}
	if p := percentile(ms, 99); p != 10 {
		t.Fatalf("p99 = %v, want 10", p)
	}
	if m := median([]float64{5, 1, 3, 2, 4}); m != 3 {
		t.Fatalf("median = %v, want 3", m)
	}
}

func TestMedianRun(t *testing.T) {
	runs := []runMetric{
		{OpsSec: 100, P50Ms: 1, P95Ms: 2, P99Ms: 3, MaxMs: 4},
		{OpsSec: 300, P50Ms: 3, P95Ms: 4, P99Ms: 5, MaxMs: 6},
		{OpsSec: 200, P50Ms: 2, P95Ms: 3, P99Ms: 4, MaxMs: 5},
		{OpsSec: 500, P50Ms: 5, P95Ms: 6, P99Ms: 7, MaxMs: 8},
		{OpsSec: 400, P50Ms: 4, P95Ms: 5, P99Ms: 6, MaxMs: 7},
	}
	got := medianRun(runs)
	want := runMetric{OpsSec: 300, P50Ms: 3, P95Ms: 4, P99Ms: 5, MaxMs: 6}
	if got != want {
		t.Fatalf("medianRun = %+v, want %+v", got, want)
	}
}

func TestKeyGeneration(t *testing.T) {
	if k := singleKey("get", 5); k != "bench:get:5" {
		t.Fatalf("singleKey = %q", k)
	}
	keys := groupKeys("mget", 7)
	if len(keys) != keysPerGroup {
		t.Fatalf("groupKeys returned %d keys, want %d", len(keys), keysPerGroup)
	}
	for _, k := range keys {
		if !strings.Contains(k, "{7}") {
			t.Fatalf("key %q missing shared hash tag", k)
		}
	}
}

func TestGetGenHitAndMiss(t *testing.T) {
	r := newStandaloneRouter("x:1")
	gen := getGen(r, "get", 1)
	_, args := gen(0)
	if idx := args[1]; idx != "bench:get:0" {
		t.Fatalf("hit key = %q", idx)
	}
	gen = getGen(r, "get", 0)
	_, args = gen(0)
	if idx := args[1]; idx != "bench:get:50000" {
		t.Fatalf("miss key = %q", idx)
	}
}

func TestDownloadTrace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/debug/pprof/trace" || r.URL.Query().Get("seconds") != "5" {
			t.Errorf("unexpected trace request: %s", r.URL)
		}
		_, _ = w.Write([]byte("trace data"))
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "node1.trace")
	if err := downloadTrace(server.URL+"/debug/pprof/trace?seconds=5", path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "trace data" {
		t.Fatalf("trace = %q, error = %v", data, err)
	}

	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failed.Close()
	badPath := filepath.Join(filepath.Dir(path), "bad.trace")
	if err := downloadTrace(failed.URL, badPath); err == nil {
		t.Fatal("expected trace download error")
	}
	if _, err := os.Stat(badPath); !os.IsNotExist(err) {
		t.Fatalf("failed trace left output file: %v", err)
	}
}
