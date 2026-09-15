package db_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

// raceStubValidator implements both injection seams with no-op behavior; the
// test only cares that injecting it races nothing.
type raceStubValidator struct{}

func (raceStubValidator) ValidateTypeSchema(*cluster.Type) error { return nil }
func (raceStubValidator) ValidateEntityData(*cluster.Type, []byte) (*cluster.SchemaDivergence, error) {
	return nil, nil
}

// TestProductionWiringOrder_RaceFree pins the post-New injection class the
// way cmd/node.go actually exercises it. Production MUST inject after db.New
// — several sub-parsers need the live DB (the loopback Proposer, the ingest
// dialects) while New needs the parser, an inherent cycle — but New's
// machinery is already running: the version announce and scrub scheduler
// propose (the tripwire reads the entity validator), and applied syncable
// configs push builds the listener parses (reading the parser registries).
// Every seam on that path must be SAFELY PUBLISHABLE (atomic validator
// holders; the locked parser registries). This test exists because ordinary
// fixtures wire before Open — the opposite of production — so the race job
// structurally could not see this class until the CI announce/validator race
// fired. Run under -race; each half was red-proved against its pre-fix code.
func TestProductionWiringOrder_RaceFree(t *testing.T) {
	// The announce goroutine (WithVersionAnnounce) is the production racer;
	// the fixture enables it, and registers the http syncable parser the
	// injector loop below keeps re-registering.
	d, s := newWalDBRestatements(t)
	proposeTypeTOML(t, d, "race-topic", "race-topic", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("race-topic"))
	require.NoError(t, err)

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
				// cmd/node.go's post-New wiring, looped: validators and
				// sub-parser registration racing the live engine.
				d.SetEntityValidator(raceStubValidator{})
				d.SetTypeSchemaValidator(raceStubValidator{})
				d.AddSyncableParser("http", &synchttp.SyncableParser{})
				d.AddIngestableParser("stub", &clusterfakes.FakeIngestableParser{})
				d.AddDatabaseParser("stub", &clusterfakes.FakeDatabaseParser{})
			}
		}
	}()

	// Real Proposes drive announceDivergences (the entity-validator reader);
	// ProposeType drives the schema-validator reader; the syncable config
	// drives admission parsing AND the apply-side build (the listener parses
	// against the registries the injector is mutating — the boot-window
	// interleaving a crash-replay tail or a peer's mid-boot commit produces).
	for i := 0; i < 50; i++ {
		p := &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, []byte{byte(i)}, []byte(`{"n":1}`)),
		}}
		require.NoError(t, d.Propose(testCtx(t), p))
	}
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "race-hook", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"race-hook\"\ntype = \"http\"\n\n[http]\ntopic = \"race-topic\"\nurl = \"http://127.0.0.1:1/dead\"\n"),
	}))
	proposeTypeTOML(t, d, "race-topic-2", "race-topic-2", `{"type":"object"}`, "")

	close(stop)
	wg.Wait()
}
