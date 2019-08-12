define test
	go test -covermode=atomic -coverprofile=.coverage.tmp -v -p=1 $(1)
	go tool cover -html=.coverage.tmp
	rm .coverage.tmp
endef

test:
	go test ./... -cover

test/topic:
	$(call test,"github.com/philborlin/committed/topic")

test/cluster:
	$(call test,"github.com/philborlin/committed/db")

test/sync:
	$(call test,"github.com/philborlin/committed/syncable")

test/bridge:
	$(call test,"github.com/philborlin/committed/bridge")
