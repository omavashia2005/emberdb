# Redis benchmark mapping

Redis source is pinned to commit `20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e`.

## Run the comparison

```sh
make bench
```

For a shorter or larger run:

```sh
REQUESTS=10000 CLIENTS=10 make bench
```

The command builds fresh Docker environments, creates standalone EmberDB and Redis servers plus three-node clusters for both, runs the same Go RESP client against each product, prints two labeled `EMBERDB vs REDIS` tables, then removes the containers. Every row identifies the command and shows EmberDB ops/s, Redis ops/s, and the Redis/EmberDB ratio.

Both products use AOF with one-second fsync. Cluster traffic is distributed across all three Docker nodes with keys selected for each node's slot range. Process startup and cluster convergence happen before timing.

## Implemented workloads

| Output row | Redis workload mapped | Why |
| --- | --- | --- |
| `SET` | Default three-byte SET | Core single-key write throughput. [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1912-L1916). |
| `GET` | Default GET | Core single-key read throughput. [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1918-L1921). |
| `MSET_10` | Default ten-pair MSET | EmberDB's implemented bulk-write variant. [Redis source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L2024-L2035). |
| `MGET_10` | Ten-key arbitrary MGET | EmberDB's implemented bulk-read variant; Redis has no default MGET benchmark. [Redis arbitrary-command source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1878-L1888). |

The cluster variants use a shared hash tag for each multi-key command, matching Redis's cluster requirement. [Redis cluster hash-tag requirement](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1610-L1613).

## Docker cluster tests

```sh
make test-cluster
```

This creates both three-node Docker clusters and runs the Redis-mapped SET/GET, same-slot MSET/MGET, and topology checks in `integration/cluster_test.go`. Ordinary `go test ./...` skips this integration test when the Docker addresses are absent.

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

- MGET encodes a missing element as the literal string `(nil)` instead of a RESP null. The source-linked regression remains skipped in `cmd/ember-server/main_test.go`.
- DECRBY with the minimum signed integer wraps during negation instead of returning Redis's overflow error. The source-linked regression remains skipped in `utils/kvstore/kvstore_test.go`.
