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

crosscompile:
	@GOOS=darwin GOARCH=amd64 go build -o committed-darwin-amd64
	@file committed-darwin-amd64
	@GOOS=linux GOARCH=amd64 go build -o committed-linux-amd64
	@file committed-linux-amd64
	@GOOS=windows GOARCH=amd64 go build -o committed-windows-amd64.exe
	@file committed-windows-amd64.exe
