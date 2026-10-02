# [WIP] ember: Redis-compatible distributed Key-Value Store

Currently supports:

* RESP2/RESP3 Compatible
* Pub/Sub (PUBLISH, SUBSCRIBE, UNSUBSCRIBE)
* SET, GET, MSET, MGET, INCR, INCRBY, DECR, DECRBY, FLUSHALL
* Server and Client CLI (kinda like redis-server and redis-cli)
* [WIP] Clusters via docker compose. Currently supports:
  - CLUSTER START
  - CLUSTER CREATE
  - CLUSTER MEET
  - CLUSTER ADDSLOTSRANGE
* Planned: Other data types, persistence, transactions, TTL, Cache Eviction

## Installation

```bash
git clone https://github.com/omavashia2005/emberdb.git && cd emberdb
```
## Run 

Persistence is enabled by default. Set `EMBERDB_PERSISTENCE=0` to run entirely in memory: existing AOF/RDB files are ignored, and no new ones are written. This applies to standalone servers, cluster nodes, and servers started from the CLI. For Docker Compose, prefix the command with the variable (for example, `EMBERDB_PERSISTENCE=0 docker compose up --build`). `SAVE`, `BGSAVE`, and `BGREWRITEAOF` return an error while disabled. Set `EMBERDB_PERSISTENCE=1` (or leave it unset) to enable persistence again.

### Normal Mode

Open connection to server
```bash
go run main.go
ember-server> ember start
```

Start CLI

```bash
./ember-cli
```

Run any Redis commands (SET, GET, MSET, MGET, INCR, INCRBY, DECR, DECRBY, FLUSHALL)

### Cluster Mode

Start the server cluster

```bash
docker compose up --build
```
In a separate session, run the CLI. This example is for the current `compose.yaml` with 5 nodes. Minimum 3 nodes required. 
```bash
./ember-cli --create-cluster 127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381 127.0.0.1:6382 127.0.0.1:6383
```
