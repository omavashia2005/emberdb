# Redis benchmark mapping

Redis source is pinned to commit `20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e`.

## Run the comparison

```sh
make bench             # EmberDB and Redis clusters
make bench-cluster     # same cluster benchmark
```

Each command builds 3-node EmberDB and Redis clusters, runs the harness (`cmd/ember-bench`) inside a container on the same Docker network, writes raw results to `benchmark-results/results.json`, then serves a browser dashboard over the results at `http://127.0.0.1:8080/` (set `DASHBOARD_PORT` to change the port). Press Ctrl+C to stop the dashboard once done; the Docker environment is already torn down by then.

Run `./scripts/profile-bench.sh` to capture one 5-second `.trace` per product, case, and node. The files appear under `benchmark-results/profiled/traces/emberdb/` and `benchmark-results/profiled/traces/redis/`, and the dashboard's Profiling tab links to both. EmberDB files are Go runtime traces, opened with `go tool trace`. Redis files are sampled server CPU profiles, opened with the pprof web viewer to inspect hot functions, call graphs, and flame graphs. Redis cannot emit Go runtime traces, so its profile does not include goroutine or garbage-collector timelines.

Each measured case runs 5 repeats of 20,000 requests (100,000 measured requests per case). Set `REQUESTS` to change the per-repeat count for `make bench` or `./scripts/profile-bench.sh`, for example `REQUESTS=10000 make bench`. Profiling sends additional requests during each 5-second capture.

The profiling run uses an instrumented Redis image to collect CPU samples; use a regular `make bench` run for throughput comparisons. Both benchmark clusters use memory only for data. Redis still writes its required cluster topology file.

## What it measures

The same Go RESP client (persistent connections, pipeline depth 1) runs against each target one at a time:

- Commands: `GET`, `SET`, `MGET`, `MSET` (10 keys per multi-key request).
- Concurrency: 1, 10, 50, and 100 clients.
- `GET`/`MGET`: 100% hit, 50% hit / 50% miss, and 100% miss variants.
- `SET`/`MSET`: existing-key and new-key variants.

The keyspace is deterministic: 100,000 keys per command, with keys `0`-`49,999` preloaded (hits) and `50,000`-`99,999` never written (misses). New-key `SET`/`MSET` variants use a run-scoped counter so every write is guaranteed novel, without reusing or deleting keys between repeats.

For every (command, variant, concurrency) combination: preload the dataset once, run a short warmup, then run the measured benchmark 5 times and report the median ops/sec, p50, p95, p99, and max latency. All 5 repeats are kept in `results.json` alongside the median.

Cluster mode routes with real `CLUSTER SLOTS` discovery, exactly like a production Redis cluster client: single-key `GET`/`SET` land on whatever node owns that key's hash slot (spreading naturally across all three nodes), and `MGET`/`MSET` keys share a `{tag}` hash tag so all 10 keys in one request land on the same slot, per Redis's cluster hash-tag requirement. [Redis cluster hash-tag requirement](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/commands/cluster-slots.md).

## Docker cluster tests

```sh
make test-cluster
```

This creates both three-node Docker clusters and runs the Redis-mapped SET/GET, same-slot MSET/MGET, and topology checks in `tests/integration/cluster_test.go`. Ordinary `make test` skips this integration test when the Docker addresses are absent.

## Relevant benchmarks intentionally not implemented

These remain inventory only, as requested in `NEW-FEATURES.md`.

| Area | Redis workload retained for later | Source |
| --- | --- | --- |
| Numeric strings | `INCR` | [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1924-L1927). |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE_100/300/500/600` | [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1930-L1951). |
| Hashes | `HSET`; custom `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL` | [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1961-L1965). |
| Sets | `SADD`; custom `SMEMBERS`, `SISMEMBER`, `SREM` | [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1954-L1958). |
| Sorted sets | `ZADD`; custom `ZRANGE`, `ZREM` | [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1974-L1980). |
| Pub/sub | Custom `PUBLISH` plus paired subscribers | [Redis correctness source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/pubsub.tcl#L44-L99). |

## Redis incompatibilities exposed by mapped tests

- MGET encodes a missing element as the literal string `(nil)` instead of a RESP null. The source-linked regression remains skipped under `tests/`.
- DECRBY with the minimum signed integer wraps during negation instead of returning Redis's overflow error. The source-linked regression remains skipped under `tests/`.
