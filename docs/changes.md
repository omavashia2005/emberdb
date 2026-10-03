# Changes

## Features

- Data types: added Radish-compatible list, hash, set, and sorted-set storage and commands. Standalone mode uses per-type maps; cluster mode uses tagged values per hash slot. (`utils/kvstore/kvstore.go:15-753`, `cmd/ember-server/main.go:120-378`)
- Unsubscribe: added connection-scoped channel removal and cleanup. (`utils/pubsub/pubsub.go:7-95`, `cmd/ember-server/main.go:607-659`)
- Storage: removed disk recovery, journaling, snapshots, maintenance, and persistence commands. All server data lives in memory.

## Tests and benchmarks

- Added Redis-mapped KV regressions plus Docker-only cluster integration tests, with the exact upstream test, reason, and source link beside each case. (`utils/kvstore/kvstore_test.go`, `cmd/ember-server/main_test.go`, `integration/cluster_test.go`)
- Added focused datatype and pub/sub checks. (`utils/kvstore/data_types_test.go`, `utils/pubsub/pubsub_test.go`, `cmd/ember-server/data_types_test.go`)
- Added `make bench` (with `make bench-standalone` / `make bench-cluster` variants), a Go RESP benchmark harness comparing EmberDB and Redis standalone and 3-node clusters on GET/SET/MGET/MSET, across 1/10/50/100 clients and hit/miss and existing/new-key variants, using real `CLUSTER SLOTS` routing, 5 repeats with median ops/sec and p50/p95/p99/max latency, raw output in `benchmark-results/results.json`, and a static browser dashboard. Added standard `CLUSTER SLOTS` and `MOVED host:port` responses to EmberDB for real cluster-client routing. Replaces the earlier `make bench-memtier` harness. (`Makefile`, `compose.benchmark.yaml`, `scripts/docker-clusters.sh`, `scripts/bench-dashboard.html`, `cmd/ember-bench/main.go`, `cmd/ember-server/main.go`, `cmd/ember-server/main_test.go`, `redis-benchmarks.md`)

## Bugs and improvements

- Fixed the duplicate/wrong server prompt, local node argument count, displayed default port, and custom-port startup. (`main.go:20-169`)
- Added local `--cluster-add-node` support, cleanup when Docker setup fails, and a built-in 10-second convergence wait before rebalance. (`cmd/ember-cli/main.go:186-256`)
- Centralized non-RESP error categories/output and replaced panics with returned, printed, or RESP-encoded errors. (`utils/errors.go:1-17`, `main.go`, `cmd/ember-cli/main.go`, `cmd/ember-server/main.go`, `utils/clusters/message.go:144-179`)
- Cluster storage now uses `SlotKeys [16384]map[string]Value`; non-cluster storage retains classical per-type maps. (`utils/kvstore/kvstore.go:32-90`)
