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
