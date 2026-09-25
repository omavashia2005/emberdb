# Changes

## Features

- Data types: added Radish-compatible list, hash, set, and sorted-set storage and commands. Standalone mode uses per-type maps; cluster mode uses tagged values per hash slot. (`utils/kvstore/kvstore.go:15-753`, `cmd/ember-server/main.go:120-378`)
- Unsubscribe: added connection-scoped channel removal and cleanup. (`utils/pubsub/pubsub.go:7-95`, `cmd/ember-server/main.go:607-659`)
- Persistence: added checksummed atomic RDB snapshots using Redis's `3600/1`, `300/100`, `60/10000` policy; RESP AOF with one-second fsync, AOF-first recovery, truncated-tail repair, and 100%/64 MiB rewrite policy; added `SAVE`, `BGSAVE`, and `BGREWRITEAOF`. (`utils/kvstore/persistence.go:23-667`, `utils/kvstore/kvstore.go:97-726`, `cmd/ember-server/main.go:102-133,928-952`)

## Tests and benchmarks

- Added Redis-mapped KV and cluster regressions with the exact upstream test, reason, and source link beside each case. (`utils/kvstore/kvstore_test.go:1-148`, `utils/clusters/cluster_test.go:1-101`, `cmd/ember-server/main_test.go:1-219`)
- Added focused datatype, pub/sub, and persistence checks. (`utils/kvstore/data_types_test.go:1-57`, `utils/pubsub/pubsub_test.go:1-16`, `utils/kvstore/persistence_test.go:1-133`, `cmd/ember-server/data_types_test.go:1-46`)
- Added standalone and cluster-enabled GET, SET, MGET, and MSET command-path benchmarks plus the requested Redis benchmark inventory and comparison commands. (`cmd/ember-server/main_test.go:95-219`, `redis-benchmarks.md:1-52`)

## Bugs and improvements

- Fixed the duplicate/wrong server prompt, local node argument count, displayed default port, and custom-port startup. (`main.go:20-169`)
- Added local `--cluster-add-node` support and removes a Docker node when add-node setup fails. (`cmd/ember-cli/main.go:186-258`)
- Centralized non-RESP error categories/output and replaced panics with returned, printed, or RESP-encoded errors. (`utils/errors.go:1-17`, `main.go`, `cmd/ember-cli/main.go`, `cmd/ember-server/main.go`, `utils/clusters/message.go:144-179`)
- Cluster storage now uses `SlotKeys [16384]map[string]Value`; non-cluster storage retains classical per-type maps. (`utils/kvstore/kvstore.go:32-90`)
