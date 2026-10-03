# EmberDB

EmberDB is a work-in-progress Redis-compatible key-value server written in Go. It supports RESP2/RESP3, standalone and cluster modes, pub/sub, and string, list, hash, set, and sorted-set commands. All data lives in memory and is lost when the server stops. Transactions, TTL commands, and eviction are not implemented yet.

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

## Run a cluster

Start the five nodes defined in `compose.yaml`, then assign their slots:

```sh
docker compose up -d --build
go run ./cmd/ember-cli --create-cluster 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383
```

Stop the cluster with `docker compose down`.

## Tests and benchmarks

```sh
make test                   # all Go tests in tests/; Docker integration test is skipped
make test-cluster            # three-node EmberDB and Redis integration tests
make bench                   # three-node throughput and latency comparison
./scripts/profile-bench.sh   # EmberDB runtime traces and Redis CPU profiles
```

All Go test files live under `tests/`. The `make test` target uses Go's overlay option to retain access to private server and benchmark functions.

The benchmark opens a results dashboard at `http://127.0.0.1:8080/`; press Ctrl+C to stop it. Set `REQUESTS` to change requests per run, or `DASHBOARD_PORT` to change the dashboard port. Both products use memory only for benchmark data; Redis still writes its required cluster topology file. See [benchmark details](docs/redis-benchmarks.md).

Profiling traces remain available with the upstream `go-resp` library. Custom RESP write counters are unavailable.
