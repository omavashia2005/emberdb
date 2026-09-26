.PHONY: bench bench-standalone bench-cluster test-cluster

bench:
	./scripts/docker-clusters.sh bench

bench-standalone:
	./scripts/docker-clusters.sh bench standalone

bench-cluster:
	./scripts/docker-clusters.sh bench cluster

test-cluster:
	./scripts/docker-clusters.sh test
