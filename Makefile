test:
	go test -short ./... -cover

test/ci:
	go build
	go test ./... -cover

test-all:
	go build
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
	@GOOS=darwin GOARCH=amd64 go build -o committed-darwin-amd64
	@file committed-darwin-amd64
	@GOOS=linux GOARCH=amd64 go build -o committed-linux-amd64
	@file committed-linux-amd64
	@GOOS=windows GOARCH=amd64 go build -o committed-windows-amd64.exe
	@file committed-windows-amd64.exe
