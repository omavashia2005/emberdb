# EmberDB

EmberDB is a Redis-compatible, in-memory key-value server written in Go. It speaks RESP2 and RESP3, runs standalone or as a sharded cluster, and works with `redis-cli` and standard Redis clients.

## Features

| Category | Commands | Notes |
|---|---|---|
| Strings | `GET`, `SET`, `MGET`, `MSET`, `APPEND`, `INCR`, `INCRBY`, `DECR`, `DECRBY` | |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN` | |
| Hashes | `HSET`, `HGET`, `HMSET`, `HMGET`, `HGETALL`, `HDEL` | |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER` | |
| Sorted sets | `ZADD`, `ZRANGE`, `ZREM` | |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE` | |
| Key management | `DEL`, `FLUSHALL` | |
| Connection | `PING`, `ECHO` | |
| Cluster | `CLUSTER MEET`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER ADDSLOTSRANGE`, `CLUSTER MYADDR`, `SETSLOT`, `MIGRATE`, `GETKEYSINSLOT`, `RESTORE-ASKING` | 16,384 hash slots, CRC16 routing, Redis-style `{tag}` hash tags |
| Protocol | RESP2, RESP3 | |

### Not yet implemented

- Key expiration (`EXPIRE`, `TTL`, and related commands)
- Transactions (`MULTI`, `EXEC`, `WATCH`)
- Eviction policies
- Persistence (data lives in memory only and does not survive a restart)
- Replication (every cluster node is a master; there is no replica failover)

## Requirements

- Go 1.24.5 or newer
- Docker Compose, only if you run a cluster or the benchmark suite

## Quickstart

Clone the repository and start a standalone server on port 6379:

```sh
git clone https://github.com/omavashia2005/emberdb.git
cd emberdb
go run . __standalone 6379
```

In a second terminal, connect with `redis-cli` or the bundled client:

```sh
redis-cli -p 6379
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
127.0.0.1:6379> HSET user:1 name Ember
(integer) 1
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Ember"
```

The interactive launcher works too. Run `go run .`, then type `ember start` at its prompt.

## Running a cluster without Docker

EmberDB ships a built-in launcher that starts three local nodes on ports 6379, 6380, and 6381.

```sh
go run .
```

At the prompt, type:

```
ember cluster-start
```

Each node starts as a separate process. Assign hash slots and join them into one cluster with `ember-cli`:

```sh
go run ./cmd/ember-cli --create-cluster 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381
```

To start a single node by hand instead, run:

```sh
go run . __node <port> <cluster-host>
```

## Running a cluster with Docker

`compose.yaml` defines a five-node cluster (`node-1` through `node-5`), mapped to host ports 6379–6383.

```sh
docker compose up -d --build
go run ./cmd/ember-cli --create-cluster 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383
```

Stop the cluster with:

```sh
docker compose down
```

To change the node count, add or remove service blocks in `compose.yaml` and update the host port list in the `--create-cluster` command to match.

## Configuration

EmberDB takes its settings from command-line arguments and one environment variable. There is no config file.

| Setting | How to set it | Default |
|---|---|---|
| Client port | Positional argument to `__standalone` or `__node` | `6379` |
| Cluster host | Positional argument to `__node` | — |
| Cluster bus port | Fixed at client port + 10000 | `16379` for port `6379` |
| pprof listener address | `EMBERDB_PPROF_ADDR` environment variable | `127.0.0.1:6060` |

## Architecture

In standalone mode, EmberDB keeps one map per data type behind a single read-write lock. In cluster mode, it switches to 16,384 fixed slots, each with its own lock, and assigns every key to a slot with a CRC16 hash. Commands with multiple keys (`MGET`, `MSET`) require all keys to share a slot, enforced through Redis-style `{tag}` hash tags.

## Benchmarks

EmberDB and Redis 7 ran the same workload on identical 3-node clusters: the same Go RESP client, the same keyspace, and the same concurrency levels. Full methodology, including the keyspace layout and repeat counts, is in [`docs/redis-benchmarks.md`](docs/redis-benchmarks.md). Raw results for every command, hit/miss variant, and concurrency level are in [`benchmark-results/`](benchmark-results/).

Median ops/sec and p99 latency, 3-node cluster:

| Command | Concurrency | EmberDB ops/sec | Redis ops/sec | EmberDB p99 (ms) | Redis p99 (ms) |
|---|---|---|---|---|---|
| GET (100% hit) | 1 | 15,827 | 17,620 | 0.105 | 0.072 |
| GET (100% hit) | 100 | 326,705 | 443,880 | 1.293 | 0.856 |
| SET (new key) | 1 | 15,925 | 17,015 | 0.105 | 0.074 |
| SET (new key) | 100 | 302,854 | 414,668 | 1.551 | 0.996 |
| MGET (100% hit) | 1 | 14,519 | 16,114 | 0.109 | 0.076 |
| MGET (100% hit) | 100 | 217,558 | 303,865 | 2.653 | 1.797 |
| MSET (new key) | 1 | 14,511 | 15,492 | 0.111 | 0.079 |
| MSET (new key) | 100 | 196,536 | 238,391 | 2.447 | 1.671 |

At one client, EmberDB reaches 85–92% of Redis's throughput. At 100 concurrent clients, it reaches 70–74%.

Run the comparison yourself:

```sh
make bench            # builds 3-node EmberDB and Redis clusters, runs the benchmark, opens a dashboard
make test-cluster     # runs the Redis-mapped correctness tests against both clusters
./scripts/profile-bench.sh   # captures EmberDB runtime traces and Redis CPU profiles
```

The dashboard serves results at `http://127.0.0.1:8080/`. Set `REQUESTS` to change requests per run, or `DASHBOARD_PORT` to change the dashboard port.

## Tests

```sh
go test ./...          # unit tests; the Docker integration test is skipped if no cluster is running
make test-cluster      # three-node EmberDB and Redis integration tests under Docker
```
