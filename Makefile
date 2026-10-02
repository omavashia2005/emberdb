.PHONY: bench bench-cluster test-cluster

bench:
	./scripts/docker-clusters.sh bench

bench-cluster:
	./scripts/docker-clusters.sh bench

test-cluster:
	./scripts/docker-clusters.sh test
