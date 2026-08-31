package parser_test

import (
	"sync"
	"testing"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestParserRegistration_RaceFreeWithConcurrentParse pins the registries'
// safe-publication contract: sub-parser registration happens after db.New in
// production (several sub-parsers need the live DB — an inherent cycle), and
// the apply path can push a config build that parses concurrently (a
// crash-window replay tail, a peer's commit arriving mid-boot). The reconcile
// contract absorbs the semantic ordering; the registry lock must supply the
// memory safety. Run under -race.
func TestParserRegistration_RaceFreeWithConcurrentParse(t *testing.T) {
	p := parser.New()
	cfg := []byte("[syncable]\nname = \"x\"\ntype = \"stub\"\n\n[stub]\ntopic = \"t\"\n")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				p.AddSyncableParser("stub", &clusterfakes.FakeSyncableParser{})
				p.AddIngestableParser("stub", &clusterfakes.FakeIngestableParser{})
				p.AddDatabaseParser("stub", &clusterfakes.FakeDatabaseParser{})
			}
		}
	}()

	for i := 0; i < 500; i++ {
		// Lookup misses ("cannot parse syncable of type") are fine — the
		// build would degrade loudly and reconcile later; only the map
		// access itself must be race-free.
		_, _ = p.SyncableTopics("text/toml", cfg)
		_, _, _, _ = p.ParseSyncable("text/toml", cfg, nil)
	}

	close(stop)
	wg.Wait()
}
