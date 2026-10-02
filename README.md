# EmberDB

EmberDB is a work-in-progress Redis-compatible key-value server written in Go. It supports RESP2/RESP3, standalone and cluster modes, pub/sub, and string, list, hash, set, and sorted-set commands. It also supports AOF and RDB persistence. Transactions, TTL commands, and eviction are not implemented yet.

## Requirements

- Go 1.24.5 or newer
- Docker Compose for clusters, integration tests, and benchmarks

Clone the repository and run the commands below from its root:

```sh
git clone https://github.com/omavashia2005/emberdb.git
cd emberdb
```

## Run a standalone server

Start the server on port 6379:

```sh
go run . __standalone 6379
```

In another terminal, connect with the EmberDB CLI or `redis-cli`:

```sh
go run ./cmd/ember-cli
```

The interactive server launcher is also available with `go run .`; enter `ember start` at its prompt.

## Persistence

Persistence is enabled by default. The server stores `appendonly.aof` and `dump.rdb` under `./emberdb-<port>/`. Set `EMBERDB_DATA_DIR` to change the parent directory.

Set `EMBERDB_PERSISTENCE=0` before starting the server to use memory only:

```sh
EMBERDB_PERSISTENCE=0 go run . __standalone 6379
```

This skips loading existing files and creates no AOF or RDB files. Writes made while persistence is off are lost when the server stops. `SAVE`, `BGSAVE`, and `BGREWRITEAOF` return an error in this mode. Leave the variable unset or set it to `1` to enable persistence.

## Run a cluster

Start the five nodes defined in `compose.yaml`, then assign their slots:

```sh
docker compose up -d --build
go run ./cmd/ember-cli --create-cluster 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383
```

Use `EMBERDB_PERSISTENCE=0 docker compose up -d --build` for an in-memory cluster. Stop the cluster with `docker compose down`.

## Tests and benchmarks

```sh
go test ./...                # unit tests; Docker integration test is skipped
make test-cluster            # three-node EmberDB and Redis integration tests
make bench                   # three-node throughput and latency comparison
./scripts/profile-bench.sh   # EmberDB runtime traces and Redis CPU profiles
```

The benchmark opens a results dashboard at `http://127.0.0.1:8080/`; press Ctrl+C to stop it. Set `REQUESTS` to change requests per run, or `DASHBOARD_PORT` to change the dashboard port. Prefix any benchmark, profiling, or cluster test command with `EMBERDB_PERSISTENCE=0` to turn off data persistence for both EmberDB and Redis. Redis still writes its required cluster topology file. See [benchmark details](docs/redis-benchmarks.md).

Profiling traces remain available with the upstream `go-resp` library. Custom RESP write counters are unavailable.
