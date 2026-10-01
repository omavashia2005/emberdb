// Command ember-bench compares EmberDB against Redis using one Go RESP
// client against standalone and 3-node cluster deployments of each. It
// covers GET/SET/MGET/MSET across fixed concurrency levels and a
// deterministic keyspace, using CLUSTER SLOTS for cluster-aware routing.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fusl/go-resp/doublebuffer"
	"github.com/bytechan/resp3"
	"github.com/omavashia2005/emberdb/utils/kvstore"
)

const (
	keysPerGroup   = 10
	hitRangeSize   = 50_000
	missRangeSize  = 50_000
	missStart      = hitRangeSize
	hitGroupCount  = hitRangeSize / keysPerGroup
	missGroupCount = missRangeSize / keysPerGroup
	missGroupStart = hitGroupCount

	requestsDefault = 2000
	warmupDefault   = 200
	repeats         = 5
	maxConcurrency  = 100
)

var concurrencies = []int{1, 10, 50, 100}

func main() {
	emberStandalone := flag.String("ember-standalone", "ember-standalone:6379", "EmberDB standalone address")
	redisStandalone := flag.String("redis-standalone", "redis-standalone:6379", "Redis standalone address")
	emberCluster := flag.String("ember-cluster", "ember-1:6379,ember-2:6379,ember-3:6379", "EmberDB cluster seed addresses")
	redisCluster := flag.String("redis-cluster", "redis-1:6379,redis-2:6379,redis-3:6379", "Redis cluster seed addresses")
	mode := flag.String("mode", "all", "all, standalone, or cluster")
	requests := flag.Int("requests", requestsDefault, "measured requests per repeat")
	warmup := flag.Int("warmup", warmupDefault, "warmup requests before each measured set")
	out := flag.String("out", "benchmark-results/results.json", "raw results JSON path")
	traceDir := flag.String("trace-dir", "", "capture one 5-second trace per product, case, and node in this directory")
	flag.Parse()

	if *requests < 1 || *warmup < 0 {
		fmt.Fprintln(os.Stderr, "requests must be positive and warmup non-negative")
		os.Exit(2)
	}

	report := report{Config: config{
		TotalKeys:      hitRangeSize + missRangeSize,
		PreloadKeys:    hitRangeSize,
		KeysPerGroup:   keysPerGroup,
		Concurrencies:  concurrencies,
		RequestsPerRun: *requests,
		WarmupRequests: *warmup,
		Repeats:        repeats,
	}}
	var traces []traceEntry
	var profileBatches []profileBatch

	if *mode == "all" || *mode == "standalone" {
		fmt.Println("=== STANDALONE ===")
		report.Standalone = append(report.Standalone, runProduct("EmberDB", "standalone", []string{*emberStandalone}, *requests, *warmup, *traceDir, &traces, &profileBatches)...)
		report.Standalone = append(report.Standalone, runProduct("Redis", "standalone", []string{*redisStandalone}, *requests, *warmup, *traceDir, &traces, &profileBatches)...)
		printTable(report.Standalone)
	}
	if *mode == "all" || *mode == "cluster" {
		fmt.Println("=== 3-NODE CLUSTER ===")
		report.Cluster = append(report.Cluster, runProduct("EmberDB", "cluster", split(*emberCluster), *requests, *warmup, *traceDir, &traces, &profileBatches)...)
		report.Cluster = append(report.Cluster, runProduct("Redis", "cluster", split(*redisCluster), *requests, *warmup, *traceDir, &traces, &profileBatches)...)
		printTable(report.Cluster)
	}

	if err := writeReport(*out, report); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, batch := range profileBatches {
		if err := batch.run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if len(traces) > 0 {
		var total doublebuffer.WriteStats
		for _, trace := range traces {
			if stats := trace.WriteMetrics; stats != nil {
				total.LogicalWrites += stats.LogicalWrites
				total.LogicalBytes += stats.LogicalBytes
				total.Wakeups += stats.Wakeups
				total.Flushes += stats.Flushes
				total.FlushBytes += stats.FlushBytes
				total.SocketWrites += stats.SocketWrites
				total.SocketBytes += stats.SocketBytes
			}
		}
		fmt.Printf("RESP write summary: logical=%d/%dB (%.1f B/write) → wakeups=%d → flushes=%d/%dB (%.1f B/flush) → socket=%d/%dB (%.1f B/write)\n",
			total.LogicalWrites, total.LogicalBytes, average(total.LogicalBytes, total.LogicalWrites),
			total.Wakeups, total.Flushes, total.FlushBytes, average(total.FlushBytes, total.Flushes),
			total.SocketWrites, total.SocketBytes, average(total.SocketBytes, total.SocketWrites))
	}
	if *traceDir != "" {
		if err := writeTraceIndex(*traceDir, traces); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println("\nRaw results:", *out)
}

// --- routing ---

// router maps a key to the address of the node that owns it. Standalone
// mode always returns the single configured address; cluster mode
// discovers real ownership via CLUSTER SLOTS, the same command a real
// Redis cluster client uses.
type router struct {
	slotAddr [16384]string
}

func newStandaloneRouter(addr string) *router {
	r := &router{}
	for i := range r.slotAddr {
		r.slotAddr[i] = addr
	}
	return r
}

func discoverClusterRouter(seeds []string) (*router, error) {
	var lastErr error
	for _, seed := range seeds {
		r, err := clusterSlots(seed)
		if err != nil {
			lastErr = err
			continue
		}
		return r, nil
	}
	return nil, fmt.Errorf("discover cluster topology from %v: %w", seeds, lastErr)
}

func clusterSlots(seed string) (*router, error) {
	p, err := dial(seed)
	if err != nil {
		return nil, err
	}
	defer p.conn.Close()
	if err := p.writer.WriteCommand("CLUSTER", "SLOTS"); err != nil {
		return nil, err
	}
	value, _, err := p.reader.ReadValue()
	if err != nil {
		return nil, err
	}
	if value.Err != "" {
		return nil, fmt.Errorf("CLUSTER SLOTS: %s", value.Err)
	}
	ranges, ok := value.SmartResult().([]interface{})
	if !ok {
		return nil, fmt.Errorf("CLUSTER SLOTS: unexpected reply shape")
	}
	return routerFromSlots(ranges)
}

func routerFromSlots(ranges []interface{}) (*router, error) {
	r := &router{}
	for _, raw := range ranges {
		row, ok := raw.([]interface{})
		if !ok || len(row) < 3 {
			return nil, fmt.Errorf("CLUSTER SLOTS: malformed range %#v", raw)
		}
		start, sok := row[0].(int64)
		end, eok := row[1].(int64)
		addrInfo, aok := row[2].([]interface{})
		if !sok || !eok || !aok || len(addrInfo) < 2 {
			return nil, fmt.Errorf("CLUSTER SLOTS: malformed range %#v", raw)
		}
		host, hok := addrInfo[0].(string)
		port, pok := addrInfo[1].(int64)
		if !hok || !pok {
			return nil, fmt.Errorf("CLUSTER SLOTS: malformed address %#v", addrInfo)
		}
		addr := net.JoinHostPort(host, strconv.FormatInt(port, 10))
		for slot := start; slot <= end; slot++ {
			r.slotAddr[slot] = addr
		}
	}
	return r, nil
}

func (r *router) addrFor(key string) string {
	return r.slotAddr[kvstore.SlotForKey(key)]
}

// --- persistent RESP connections ---

// worker simulates one concurrent client: it keeps one persistent
// connection per node address it happens to need and reuses it, sending
// one request at a time (pipeline depth 1).
type worker struct {
	conns map[string]*peer
}

type peer struct {
	conn   net.Conn
	writer *resp3.Writer
	reader *resp3.Reader
}

func newWorkers(n int) []*worker {
	workers := make([]*worker, n)
	for i := range workers {
		workers[i] = &worker{conns: make(map[string]*peer)}
	}
	return workers
}

// dial opens a connection and switches it to RESP3 so null replies arrive
// as a proper RESP3 null instead of Redis's legacy RESP2 "$-1" bulk string,
// which this client's reader does not accept.
func dial(addr string) (*peer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	p := &peer{conn: conn, writer: resp3.NewWriter(conn), reader: resp3.NewReader(conn)}
	if err := p.writer.WriteCommand("HELLO", "3"); err != nil {
		conn.Close()
		return nil, err
	}
	if _, _, err := p.reader.ReadValue(); err != nil {
		conn.Close()
		return nil, err
	}
	return p, nil
}

func (w *worker) do(addr string, args []string) error {
	p, ok := w.conns[addr]
	if !ok {
		var err error
		p, err = dial(addr)
		if err != nil {
			return err
		}
		w.conns[addr] = p
	}
	if err := p.writer.WriteCommand(args...); err != nil {
		return err
	}
	value, _, err := p.reader.ReadValue()
	if err != nil {
		return err
	}
	if value.Err != "" {
		return fmt.Errorf("server error: %s", value.Err)
	}
	return nil
}

func (w *worker) close() {
	for _, p := range w.conns {
		_ = p.conn.Close()
	}
}

func closeWorkers(workers []*worker) {
	for _, w := range workers {
		w.close()
	}
}

// --- keyspace ---

func singleKey(ns string, i int) string { return fmt.Sprintf("bench:%s:%d", ns, i) }

func groupKeys(ns string, g int) []string {
	keys := make([]string, keysPerGroup)
	for j := range keys {
		keys[j] = fmt.Sprintf("bench:%s:{%d}:%d", ns, g, j)
	}
	return keys
}

// hit reports whether request seq should target the preloaded (hit) range
// for the given hit ratio: 1 = always, 0 = never, otherwise alternate.
func hit(seq int64, ratio float64) bool {
	switch ratio {
	case 1:
		return true
	case 0:
		return false
	default:
		return seq%2 == 0
	}
}

// reqGen produces the node address and command args for request seq.
type reqGen func(seq int64) (addr string, args []string)

func getGen(r *router, ns string, hitRatio float64) reqGen {
	return func(seq int64) (string, []string) {
		i := int(seq) % hitRangeSize
		if !hit(seq, hitRatio) {
			i = missStart + int(seq)%missRangeSize
		}
		key := singleKey(ns, i)
		return r.addrFor(key), []string{"GET", key}
	}
}

func mgetGen(r *router, ns string, hitRatio float64) reqGen {
	return func(seq int64) (string, []string) {
		g := int(seq) % hitGroupCount
		if !hit(seq, hitRatio) {
			g = missGroupStart + int(seq)%missGroupCount
		}
		keys := groupKeys(ns, g)
		return r.addrFor(keys[0]), append([]string{"MGET"}, keys...)
	}
}

func setGen(r *router, ns string, existing bool, newSeq *atomic.Int64) reqGen {
	return func(seq int64) (string, []string) {
		i := int(seq) % hitRangeSize
		if !existing {
			i = missStart + missRangeSize + int(newSeq.Add(1))
		}
		key := singleKey(ns, i)
		return r.addrFor(key), []string{"SET", key, "xxx"}
	}
}

func msetGen(r *router, ns string, existing bool, newSeq *atomic.Int64) reqGen {
	return func(seq int64) (string, []string) {
		g := int(seq) % hitGroupCount
		if !existing {
			g = missGroupStart + missGroupCount + int(newSeq.Add(1))
		}
		keys := groupKeys(ns, g)
		args := make([]string, 0, 1+2*len(keys))
		args = append(args, "MSET")
		for _, k := range keys {
			args = append(args, k, "xxx")
		}
		return r.addrFor(keys[0]), args
	}
}

// --- execution ---

func runPhase(count int, workers []*worker, gen reqGen) ([]time.Duration, time.Duration, error) {
	var seq atomic.Int64
	seq.Store(-1)
	perWorker := make([][]time.Duration, len(workers))
	errs := make(chan error, len(workers))
	var wg sync.WaitGroup
	start := time.Now()
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			local := make([]time.Duration, 0, count/len(workers)+1)
			for {
				s := seq.Add(1)
				if s >= int64(count) {
					break
				}
				addr, args := gen(s)
				t0 := time.Now()
				if err := workers[i].do(addr, args); err != nil {
					errs <- err
					break
				}
				local = append(local, time.Since(t0))
			}
			perWorker[i] = local
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return nil, 0, err
	}
	durations := make([]time.Duration, 0, count)
	for _, d := range perWorker {
		durations = append(durations, d...)
	}
	return durations, elapsed, nil
}

func percentile(sortedMs []float64, p float64) float64 {
	if len(sortedMs) == 0 {
		return 0
	}
	idx := int((p/100)*float64(len(sortedMs)) + 0.9999999)
	idx--
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedMs) {
		idx = len(sortedMs) - 1
	}
	return sortedMs[idx]
}

func metricsFromRun(durs []time.Duration, elapsed time.Duration) runMetric {
	ms := make([]float64, len(durs))
	for i, d := range durs {
		ms[i] = float64(d) / float64(time.Millisecond)
	}
	sort.Float64s(ms)
	return runMetric{
		OpsSec: float64(len(durs)) / elapsed.Seconds(),
		P50Ms:  percentile(ms, 50),
		P95Ms:  percentile(ms, 95),
		P99Ms:  percentile(ms, 99),
		MaxMs:  ms[len(ms)-1],
	}
}

func median(vals []float64) float64 {
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func medianRun(runs []runMetric) runMetric {
	pick := func(f func(runMetric) float64) float64 {
		vals := make([]float64, len(runs))
		for i, r := range runs {
			vals[i] = f(r)
		}
		return median(vals)
	}
	return runMetric{
		OpsSec: pick(func(r runMetric) float64 { return r.OpsSec }),
		P50Ms:  pick(func(r runMetric) float64 { return r.P50Ms }),
		P95Ms:  pick(func(r runMetric) float64 { return r.P95Ms }),
		P99Ms:  pick(func(r runMetric) float64 { return r.P99Ms }),
		MaxMs:  pick(func(r runMetric) float64 { return r.MaxMs }),
	}
}

func runTestCase(workersMax []*worker, concurrency, requests, warmup int, gen reqGen) (runMetric, []runMetric, error) {
	workers := workersMax[:concurrency]
	if warmup > 0 {
		if _, _, err := runPhase(warmup, workers, gen); err != nil {
			return runMetric{}, nil, fmt.Errorf("warmup: %w", err)
		}
	}
	runs := make([]runMetric, 0, repeats)
	for r := 0; r < repeats; r++ {
		durs, elapsed, err := runPhase(requests, workers, gen)
		if err != nil {
			return runMetric{}, nil, fmt.Errorf("repeat %d: %w", r+1, err)
		}
		runs = append(runs, metricsFromRun(durs, elapsed))
	}
	return medianRun(runs), runs, nil
}

func preload(workers []*worker, r *router, ns string, groups bool) error {
	if groups {
		gen := func(seq int64) (string, []string) {
			g := int(seq)
			keys := groupKeys(ns, g)
			args := make([]string, 0, 1+2*len(keys))
			args = append(args, "MSET")
			for _, k := range keys {
				args = append(args, k, "xxx")
			}
			return r.addrFor(keys[0]), args
		}
		_, _, err := runPhase(hitGroupCount, workers, gen)
		return err
	}
	gen := func(seq int64) (string, []string) {
		key := singleKey(ns, int(seq))
		return r.addrFor(key), []string{"SET", key, "xxx"}
	}
	_, _, err := runPhase(hitRangeSize, workers, gen)
	return err
}

// --- orchestration ---

type runMetric struct {
	OpsSec float64 `json:"ops_sec"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

type testResult struct {
	Product     string      `json:"product"`
	Command     string      `json:"command"`
	Variant     string      `json:"variant"`
	Concurrency int         `json:"concurrency"`
	runMetric               // median, inlined
	Runs        []runMetric `json:"runs"`
}

type config struct {
	TotalKeys      int   `json:"total_keys"`
	PreloadKeys    int   `json:"preload_keys"`
	KeysPerGroup   int   `json:"keys_per_group"`
	Concurrencies  []int `json:"concurrencies"`
	RequestsPerRun int   `json:"requests_per_run"`
	WarmupRequests int   `json:"warmup_requests"`
	Repeats        int   `json:"repeats"`
}

type report struct {
	GeneratedAt string       `json:"generated_at"`
	Config      config       `json:"config"`
	Standalone  []testResult `json:"standalone"`
	Cluster     []testResult `json:"cluster"`
}

type traceEntry struct {
	Product      string                   `json:"product"`
	Mode         string                   `json:"mode"`
	Command      string                   `json:"command"`
	Variant      string                   `json:"variant"`
	Concurrency  int                      `json:"concurrency"`
	Node         string                   `json:"node"`
	File         string                   `json:"file"`
	WriteMetrics *doublebuffer.WriteStats `json:"write_metrics,omitempty"`
}

type traceRecorder struct {
	product string
	dir     string
	mode    string
	addrs   []string
	entries *[]traceEntry
}

type traceCase struct {
	command     string
	variant     string
	concurrency int
	gen         reqGen
}

type profileBatch struct {
	recorder traceRecorder
	requests int
	cases    []traceCase
}

func (b profileBatch) run() error {
	workers := newWorkers(maxConcurrency)
	defer closeWorkers(workers)
	if b.recorder.product == "Redis" {
		return b.runRedis(workers)
	}
	for _, c := range b.cases {
		if err := b.recorder.capture(c.command, c.variant, c.concurrency, b.requests, workers, c.gen); err != nil {
			return fmt.Errorf("%s %s %s profile %s c=%d: %w", b.recorder.product, b.recorder.mode, c.command, c.variant, c.concurrency, err)
		}
	}
	return nil
}

var getVariants = []struct {
	name  string
	ratio float64
}{
	{"100% hit", 1},
	{"50% hit / 50% miss", 0.5},
	{"100% miss", 0},
}

var writeVariants = []struct {
	name     string
	existing bool
}{
	{"existing", true},
	{"new", false},
}

func runProduct(product, mode string, addrs []string, requests, warmup int, traceDir string, traces *[]traceEntry, batches *[]profileBatch) []testResult {
	var r *router
	var err error
	if mode == "standalone" {
		r = newStandaloneRouter(addrs[0])
	} else {
		r, err = discoverClusterRouter(addrs)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}

	workers := newWorkers(maxConcurrency)
	defer closeWorkers(workers)

	if err := preload(workers, r, "get", false); err != nil {
		fatal(product, mode, "preload get", err)
	}
	if err := preload(workers, r, "mget", true); err != nil {
		fatal(product, mode, "preload mget", err)
	}
	if err := preload(workers, r, "set", false); err != nil {
		fatal(product, mode, "preload set", err)
	}
	if err := preload(workers, r, "mset", true); err != nil {
		fatal(product, mode, "preload mset", err)
	}

	var results []testResult
	var setNewSeq, msetNewSeq atomic.Int64
	var cases []traceCase
	var caseList *[]traceCase
	if traceDir != "" {
		caseList = &cases
	}

	for _, v := range getVariants {
		for _, c := range concurrencies {
			results = append(results, runOne(product, "GET", v.name, c, workers, requests, warmup, getGen(r, "get", v.ratio), caseList))
		}
	}
	for _, v := range writeVariants {
		for _, c := range concurrencies {
			results = append(results, runOne(product, "SET", v.name, c, workers, requests, warmup, setGen(r, "set", v.existing, &setNewSeq), caseList))
		}
	}
	for _, v := range getVariants {
		for _, c := range concurrencies {
			results = append(results, runOne(product, "MGET", v.name, c, workers, requests, warmup, mgetGen(r, "mget", v.ratio), caseList))
		}
	}
	for _, v := range writeVariants {
		for _, c := range concurrencies {
			results = append(results, runOne(product, "MSET", v.name, c, workers, requests, warmup, msetGen(r, "mset", v.existing, &msetNewSeq), caseList))
		}
	}
	if caseList != nil {
		*batches = append(*batches, profileBatch{traceRecorder{product: product, dir: traceDir, mode: mode, addrs: addrs, entries: traces}, requests, cases})
	}
	return results
}

func runOne(product, command, variant string, concurrency int, workers []*worker, requests, warmup int, gen reqGen, cases *[]traceCase) testResult {
	median, runs, err := runTestCase(workers, concurrency, requests, warmup, gen)
	if err != nil {
		fatal(product, command, fmt.Sprintf("%s c=%d", variant, concurrency), err)
	}
	if cases != nil {
		*cases = append(*cases, traceCase{command, variant, concurrency, gen})
	}
	return testResult{Product: product, Command: command, Variant: variant, Concurrency: concurrency, runMetric: median, Runs: runs}
}

func (r *traceRecorder) capture(command, variant string, concurrency, requests int, workers []*worker, gen reqGen) error {
	variantID := map[string]string{
		"100% hit": "hit", "50% hit / 50% miss": "mixed", "100% miss": "miss",
		"existing": "existing", "new": "new",
	}[variant]
	if variantID == "" {
		return fmt.Errorf("unknown trace variant %q", variant)
	}
	fmt.Printf("Profiling %s %s %s %s c=%d\n", r.product, r.mode, command, variant, concurrency)
	dir := filepath.Join(r.dir, strings.ToLower(r.product), r.mode)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type pendingTrace struct {
		entry traceEntry
		url   string
		path  string
	}
	pending := make([]pendingTrace, len(r.addrs))
	for i, addr := range r.addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		node := "standalone"
		if r.mode == "cluster" {
			node = fmt.Sprintf("node%d", i+1)
		}
		name := fmt.Sprintf("%s_%s_c%d_%s.trace", command, variantID, concurrency, node)
		path := filepath.Join(dir, name)
		rel, err := filepath.Rel(filepath.Dir(filepath.Clean(r.dir)), path)
		if err != nil {
			return err
		}
		pending[i] = pendingTrace{
			entry: traceEntry{Product: r.product, Mode: r.mode, Command: command, Variant: variant, Concurrency: concurrency, Node: node, File: filepath.ToSlash(rel)},
			url:   (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, "6060"), Path: "/debug/pprof/trace", RawQuery: "seconds=5"}).String(),
			path:  path,
		}
	}
	before := make([]doublebuffer.WriteStats, len(pending))
	for i, p := range pending {
		var err error
		before[i], err = fetchWriteStats(strings.Replace(p.url, "/debug/pprof/trace?seconds=5", "/debug/resp-writes", 1))
		if err != nil {
			return fmt.Errorf("%s write counters before trace: %w", p.entry.Node, err)
		}
	}

	done := make(chan error, len(pending))
	for _, p := range pending {
		go func() { done <- downloadTrace(p.url, p.path) }()
	}
	var firstErr error
	for finished := 0; finished < len(pending); {
		select {
		case err := <-done:
			finished++
			if err != nil && firstErr == nil {
				firstErr = err
			}
		default:
			if firstErr == nil {
				_, _, firstErr = runPhase(requests, workers[:concurrency], gen)
			} else {
				<-done
				finished++
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	for i, p := range pending {
		after, err := fetchWriteStats(strings.Replace(p.url, "/debug/pprof/trace?seconds=5", "/debug/resp-writes", 1))
		if err != nil {
			return fmt.Errorf("%s write counters after trace: %w", p.entry.Node, err)
		}
		delta := subtractWriteStats(after, before[i])
		p.entry.WriteMetrics = &delta
		*r.entries = append(*r.entries, p.entry)
	}
	return nil
}

func fetchWriteStats(endpoint string) (doublebuffer.WriteStats, error) {
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(endpoint)
	if err != nil {
		return doublebuffer.WriteStats{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return doublebuffer.WriteStats{}, fmt.Errorf("%s: %s", endpoint, response.Status)
	}
	var stats doublebuffer.WriteStats
	err = json.NewDecoder(response.Body).Decode(&stats)
	return stats, err
}

func subtractWriteStats(after, before doublebuffer.WriteStats) doublebuffer.WriteStats {
	return doublebuffer.WriteStats{
		LogicalWrites: after.LogicalWrites - before.LogicalWrites, LogicalBytes: after.LogicalBytes - before.LogicalBytes,
		Wakeups: after.Wakeups - before.Wakeups, Flushes: after.Flushes - before.Flushes, FlushBytes: after.FlushBytes - before.FlushBytes,
		SocketWrites: after.SocketWrites - before.SocketWrites, SocketBytes: after.SocketBytes - before.SocketBytes,
	}
}

func average(bytes, calls uint64) float64 {
	if calls == 0 {
		return 0
	}
	return float64(bytes) / float64(calls)
}

func downloadTrace(traceURL, path string) error {
	client := http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(traceURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", traceURL, resp.Status)
	}
	return writeAtomic(path, resp.Body)
}

func writeAtomic(path string, data io.Reader) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := io.Copy(f, data); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func writeTraceIndex(dir string, entries []traceEntry) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(append([]traceEntry{}, entries...), "", "  ")
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, "index.json"), bytes.NewReader(data))
}

func fatal(product, command, context string, err error) {
	fmt.Fprintf(os.Stderr, "%s %s %s: %v\n", product, command, context, err)
	os.Exit(1)
}

func printTable(rows []testResult) {
	fmt.Printf("%-8s %-6s %-20s %5s %14s %10s %10s %10s %10s\n",
		"PRODUCT", "CMD", "VARIANT", "CONC", "OPS/SEC", "P50 MS", "P95 MS", "P99 MS", "MAX MS")
	for _, row := range rows {
		fmt.Printf("%-8s %-6s %-20s %5d %14.0f %10.3f %10.3f %10.3f %10.3f\n",
			row.Product, row.Command, row.Variant, row.Concurrency,
			row.OpsSec, row.P50Ms, row.P95Ms, row.P99Ms, row.MaxMs)
	}
	fmt.Println()
}

func writeReport(path string, rep report) error {
	rep.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func split(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}
