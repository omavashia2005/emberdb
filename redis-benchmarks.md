# Redis benchmark mapping

Redis source is pinned to commit `20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e`.

## Implemented harness

Run EmberDB's command-path benchmarks with:

```sh
go test ./cmd/ember-server -run '^$' -bench '^BenchmarkCommands$' -benchmem
```

The harness reports `ops/s` for RESP requests over one in-memory `net.Pipe`, with no pipelining and a fixed three-byte value. It exercises EmberDB's real connection handler and response parser but excludes TCP/IP cost. This maps to Redis's default SET and GET command shape because Redis uses a three-byte payload, one pending request per client, and fixed keys unless `-r` is supplied. Redis sources: [SET/GET workload definitions](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1912-L1921), [benchmark options](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1602-L1626), and [official benchmark guidance](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/).

The Redis commands below are workload references, not direct numeric comparisons: they include TCP and use a different client. Published Redis numbers are also hardware-specific. A direct comparison requires running both servers through the same external client on the same host.

### Cluster topology measured here

Each in-process `cluster/*` case creates one cluster-enabled EmberDB node that owns all 16,384 slots. The result measures cluster routing, hash-slot calculation, and per-slot storage overhead on one node; it is not aggregate multi-node cluster throughput.

The deterministic `go test` harness does not start or discover multiple OS processes because process startup, port allocation, topology convergence, and cleanup would become part of the benchmark lifecycle instead of the command measurement. Redis's `--cluster` mode discovers primary nodes, requires at least as many clients as nodes, and distributes benchmark clients among those primaries. Sources: [client requirement](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1602-L1613), [client-to-node distribution](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L633-L647), and [cluster discovery](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1092-L1107).

| EmberDB benchmark | Matching official Redis workload | Why |
| --- | --- | --- |
| `single/SET` | `redis-benchmark -c 1 -n 100000 -P 1 -t set -q` | Same standalone SET command, fixed key, three-byte value, one connection, no pipeline. |
| `single/GET` | `redis-benchmark -c 1 -n 100000 -P 1 -t get -q` | Same standalone GET command and client settings. |
| `single/MSET_10` | `redis-benchmark -c 1 -n 100000 -P 1 -t mset -q` | Redis's default MSET benchmark writes ten key/value pairs; [source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L2024-L2035). |
| `single/MGET_10` | `redis-benchmark -c 1 -n 100000 -P 1 -q MGET key:0 key:1 key:2 key:3 key:4 key:5 key:6 key:7 key:8 key:9` | `redis-benchmark` has no default MGET case, so its documented arbitrary-command mode is used; [official instructions](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/#running-only-a-subset-of-the-tests). |
| `cluster/SET`, `cluster/GET` | Add `--cluster`; use at least as many clients as nodes. | Redis distributes clients among primaries and inserts hash tags into default keys; [cluster option contract](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1602-L1613) and [hash-tag routing](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L395-L414). |
| `cluster/MSET_10` | `redis-benchmark --cluster -c 3 -n 100000 -P 1 -q MSET key:0{tag} xxx ... key:9{tag} xxx` | Every key shares one Redis Cluster slot; Redis requires a `{tag}` for custom cluster commands ([source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1610-L1613)). |
| `cluster/MGET_10` | `redis-benchmark --cluster -c 3 -n 100000 -P 1 -q MGET key:0{tag} ... key:9{tag}` | Same-slot bulk-read counterpart, using arbitrary-command mode. |

## Relevant benchmarks intentionally not implemented

These are the remaining Redis workloads for EmberDB's implemented data types. They are inventory only, as requested.

| Area | Redis workload to retain for later | Relevance and source |
| --- | --- | --- |
| Numeric strings | `INCR` | Implemented by EmberDB and part of Redis's default suite; [source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1924-L1927). |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE_100/300/500/600` | Covers EmberDB list writes, pops, and range reads; [source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1930-L1951) and [LRANGE cases](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1989-L2022). |
| Hashes | `HSET`; custom `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL` | HSET is in Redis's default suite; the other implemented commands use redis-benchmark's arbitrary-command mode. [HSET source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1961-L1965). |
| Sets | `SADD`; custom `SMEMBERS`, `SISMEMBER`, `SREM` | SADD is in Redis's default suite; the remaining implemented commands require arbitrary-command runs. [SADD source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1954-L1958). |
| Sorted sets | `ZADD`; custom `ZRANGE`, `ZREM` | ZADD is in Redis's default suite; the remaining implemented commands require arbitrary-command runs. [ZADD source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L1974-L1980). |
| Pub/sub | Custom `PUBLISH channel payload` plus a paired subscriber throughput harness | `redis-benchmark` can run PUBLISH as an arbitrary command, but SUBSCRIBE is a streaming command and needs a paired publisher/subscriber benchmark. Redis correctness coverage shows the required paired-client shape; [source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/tests/unit/pubsub.tcl#L44-L99). |
| Cluster routing | Repeat applicable command workloads with `--cluster` and hash-tagged keys | Redis distributes clients across primaries and refreshes topology after MOVED/ASK replies; [source](https://github.com/redis/redis/blob/20bb2cfc54aa08c8fdfb8c4c0a8b8258e811711e/src/redis-benchmark.c#L461-L499). |

Pipelined (`-P`) and multi-client (`-c`) matrices are deferred because the requested harness is the basic throughput baseline. Redis documents both as distinct workload dimensions in the [official benchmark guide](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/).

## Redis incompatibilities exposed by mapped tests

- MGET encodes a missing element as the literal string `(nil)` instead of a RESP null. The mapped regression remains skipped in `cmd/ember-server/main_test.go` until production behavior changes.
- DECRBY with the minimum signed integer wraps during negation instead of returning Redis's overflow error. The mapped regression remains skipped in `utils/kvstore/kvstore_test.go`.
