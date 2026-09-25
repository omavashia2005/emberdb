.PHONY: bench test-cluster

bench:
	./scripts/docker-clusters.sh bench

test-cluster:
	./scripts/docker-clusters.sh test
