package main

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func (b profileBatch) runRedis(workers []*worker) error {
	if len(b.cases) == 0 {
		return nil
	}
	tmp, err := os.MkdirTemp("", "emberdb-redis-profile-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	host, _, err := net.SplitHostPort(b.recorder.addrs[0])
	if err != nil {
		return err
	}
	binary := filepath.Join(tmp, "redis-server")
	if err := downloadTrace((&url.URL{Scheme: "http", Host: net.JoinHostPort(host, "6060"), Path: "/debug/binary"}).String(), binary); err != nil {
		return fmt.Errorf("download Redis executable: %w", err)
	}
	for _, c := range b.cases {
		if err := b.recorder.captureRedis(c, b.requests, workers, binary, tmp); err != nil {
			return fmt.Errorf("Redis %s %s profile %s c=%d: %w", b.recorder.mode, c.command, c.variant, c.concurrency, err)
		}
	}
	return nil
}

func (r *traceRecorder) captureRedis(c traceCase, requests int, workers []*worker, binary, tmp string) error {
	variantID := map[string]string{
		"100% hit": "hit", "50% hit / 50% miss": "mixed", "100% miss": "miss",
		"existing": "existing", "new": "new",
	}[c.variant]
	if variantID == "" {
		return fmt.Errorf("unknown trace variant %q", c.variant)
	}
	fmt.Printf("Profiling Redis %s %s %s c=%d\n", r.mode, c.command, c.variant, c.concurrency)
	dir := filepath.Join(r.dir, "redis", r.mode)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type pendingProfile struct {
		entry traceEntry
		url   string
		path  string
		raw   string
	}
	pending := make([]pendingProfile, len(r.addrs))
	for i, addr := range r.addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		node := "standalone"
		if r.mode == "cluster" {
			node = fmt.Sprintf("node%d", i+1)
		}
		name := fmt.Sprintf("%s_%s_c%d_%s.trace", c.command, variantID, c.concurrency, node)
		path := filepath.Join(dir, name)
		rel, err := filepath.Rel(filepath.Dir(filepath.Clean(r.dir)), path)
		if err != nil {
			return err
		}
		pending[i] = pendingProfile{
			entry: traceEntry{Product: "Redis", Mode: r.mode, Command: c.command, Variant: c.variant, Concurrency: c.concurrency, Node: node, File: filepath.ToSlash(rel)},
			url:   (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, "6060"), Path: "/debug/profile", RawQuery: "seconds=5"}).String(),
			path:  path,
			raw:   filepath.Join(tmp, fmt.Sprintf("%s_%d.cpu", name, i)),
		}
	}
	results := make(chan error, len(pending))
	for _, p := range pending {
		go func() { results <- downloadTrace(p.url, p.raw) }()
	}
	var firstErr error
	for done := 0; done < len(pending); {
		select {
		case err := <-results:
			done++
			if err != nil && firstErr == nil {
				firstErr = err
			}
		default:
			if firstErr == nil {
				_, _, firstErr = runPhase(requests, workers[:c.concurrency], c.gen)
			} else {
				<-results
				done++
			}
		}
	}
	if firstErr != nil {
		return firstErr
	}
	for _, p := range pending {
		cmd := exec.Command("pprof", "-proto", binary, p.raw)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		profile, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("convert %s CPU profile: %w: %s", p.entry.Node, err, strings.TrimSpace(stderr.String()))
		}
		if err := writeAtomic(p.path, bytes.NewReader(profile)); err != nil {
			return err
		}
		*r.entries = append(*r.entries, p.entry)
	}
	return nil
}
