# Version metadata stamped into internal/version at link time. VERSION
# prefers `git describe` so tagged releases get a real version string
# and untagged commits fall back to a sha. `--dirty` surfaces
# uncommitted working-tree changes so "is this binary reproducible
# from a clean checkout?" is answerable from the binary itself.
# Override any of these from the command line, e.g.
# `make build VERSION=1.2.3`.
VERSION ?= $(shell git describe --tags --dirty --always 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
  -X github.com/philborlin/committed/internal/version.Version=$(VERSION) \
  -X github.com/philborlin/committed/internal/version.Commit=$(COMMIT) \
  -X github.com/philborlin/committed/internal/version.BuildDate=$(DATE)

build:
	go build -ldflags="$(LDFLAGS)"

test:
	go test -short ./... -cover

test/ci:
	go build -ldflags="$(LDFLAGS)"
	go test -race ./... -cover

test-all:
	go build -ldflags="$(LDFLAGS)"
	go test -tags "docker integration" -timeout 300s ./... -cover

# Integration tests gated by the `integration` build tag (the docker
# tag is handled by `test/cdc` because that suite spawns its own
# Postgres containers per test and must run -p=1; mixing the two in
# one `go test` invocation makes parallel container startup choke
# Docker on most CI runners). -race because the integration tests
# exercise the full goroutine fan-out across HTTP / ingest / sync.
test/integration:
	go test -race -tags integration -timeout 300s ./... -cover

# Runs the adversarial raft suite (phase 1: partition, leader flap,
# concurrent config changes; phase 2: asymmetric partition, slow
# follower, disk full). Tagged `adversarial` so it's excluded from
# both `make test` and `make test-all`; those don't need to pay the
# per-test election-timeout cost, and the adversarial scenarios are
# specifically designed to stress-test failure modes rather than happy
# paths.
#
# -race is mandatory here — the whole point of this suite is finding
# concurrency regressions under partition / flap / concurrent-writer
# conditions, and those bugs are race-detector-visible. -count=20 gates
# against timing flakes; a single-count pass under -race isn't enough
# evidence to trust a scenario.
test/adversarial:
	go test -tags adversarial -race -count=20 -timeout 900s ./internal/cluster/db/...

# End-to-end CDC pressure-test harness. Drives a real Postgres
# (testcontainers) and a real committed binary spawned as a child
# process, then asserts the exact proposal stream produced from a
# scripted mutation sequence against an in-script oracle. See
# e2e/cdc/ and .claude/plans/wobbly-watching-lightning.md.
#
# -p=1 because each test boots its own Postgres container — parallel
# container startup chokes Docker on most laptops. -timeout 600s
# because the first test pays a one-off `go build .` of the committed
# binary plus container pull on a fresh machine.
test/cdc:
	go test -tags docker -p=1 -timeout 600s ./e2e/cdc/...

lint:
	golangci-lint run

check: lint test

lint/openapi:
	npx --yes @redocly/cli@latest lint api/openapi.yaml

crosscompile:
	@GOOS=darwin GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o committed-darwin-amd64
	@file committed-darwin-amd64
	@GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o committed-linux-amd64
	@file committed-linux-amd64
	@GOOS=windows GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o committed-windows-amd64.exe
	@file committed-windows-amd64.exe
