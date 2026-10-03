Run every Go test from the repository root:

```sh
make test
```

This also compiles all packages and runs the RESP and doublebuffer tests against
`utils/go-resp`, through the existing local module replacement.

`overlay.json` lets the tests in `_server` and `_bench` access their packages'
private functions without adding public APIs or copying production code. Go
ignores these directories during ordinary package discovery; `make test` maps
the files into their original package locations for the test run.

Docker cluster and Redis CPU profile tests retain their existing environment
requirements and skip when their services are absent.
