define test
	go test -covermode=atomic -coverprofile=.coverage.tmp -v -p=1 $(1)
	go tool cover -html=.coverage.tmp
	rm .coverage.tmp
endef

test:
	go test -short ./... -cover

test/ci:
	go build
	go test ./... -cover

test-all:
	go build
	go test -tags "docker integration" -timeout 300s ./... -cover

# Runs the adversarial raft suite (phase 1: partition, leader flap,
# concurrent config changes). Tagged `adversarial` so it's excluded from
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

test/topic:
	$(call test,"github.com/philborlin/committed/topic")

test/cluster:
	$(call test,"github.com/philborlin/committed/cluster")

test/sync:
	$(call test,"github.com/philborlin/committed/syncable")

test/bridge:
	$(call test,"github.com/philborlin/committed/bridge")

test/e2e:
	go test -v -p=1 "github.com/philborlin/committed/e2e"

lint/openapi:
	npx --yes @redocly/cli@latest lint api/openapi.yaml

crosscompile:
	@GOOS=darwin GOARCH=amd64 go build -o committed-darwin-amd64
	@file committed-darwin-amd64
	@GOOS=linux GOARCH=amd64 go build -o committed-linux-amd64
	@file committed-linux-amd64
	@GOOS=windows GOARCH=amd64 go build -o committed-windows-amd64.exe
	@file committed-windows-amd64.exe
