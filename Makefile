.PHONY: test bench bench-cluster test-cluster

bench:
	./scripts/docker-clusters.sh bench

bench-cluster:
	./scripts/docker-clusters.sh bench

test-cluster:
	./scripts/docker-clusters.sh test

test:
	go test -overlay tests/overlay.json ./...
