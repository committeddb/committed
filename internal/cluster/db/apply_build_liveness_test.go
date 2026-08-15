package db_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// newWalDBWithSyncListener is newWalDB with the sync/ingest notification
// channels wired through db.New, so the REAL listener goroutines consume
// what the apply path queues — the full production pipeline the apply-
// liveness invariant runs through (apply → pump → listener → build →
// worker), which the nil-channel harness skips.
func newWalDBWithSyncListener(t *testing.T, p *parser.Parser) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	syncCh := make(chan *db.SyncableWithID)
	ingestCh := make(chan *db.IngestableWithID)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)

	id := uint64(1)
	peers := db.Peers{id: ""}
	d := db.New(id, peers, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() {
		_ = d.Close()
		_ = s.Close()
	})
	return d, s
}

// The apply-liveness invariant, layer A: appliedIndex must never wait on a
// syncable's node-local build. The build reaches the destination (Init's
// DDL and prepares) and can hang on destination state — the field wedge
// was an analyst's table lock. Here the sub-parser's SECOND call (the
// node-local build; the first is the admission parse) blocks until
// released, and both the config propose and a data propose must confirm
// while it hangs. Under build-on-apply this fails: the config propose
// itself times out waiting for its own apply.
func TestApplyLiveness_ProposalsConfirmDuringHungSyncableBuild(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	var calls atomic.Int32
	hang := &clusterfakes.FakeSyncableParser{}
	hang.ParseStub = func(*cluster.ParsedConfig, cluster.DatabaseStorage) (cluster.Syncable, error) {
		if calls.Add(1) > 1 {
			<-release
		}
		return &clusterfakes.FakeSyncable{}, nil
	}
	p := parser.New()
	p.AddSyncableParser("hang", hang)
	d, s := newWalDBWithSyncListener(t, p)

	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "hung-build", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "hung-build", "type": "hang"}}`),
	}), "config propose must confirm without waiting for the node-local build")

	// Vacuity guard: the build must actually be in flight and hanging —
	// otherwise the propose below proves nothing.
	require.Eventually(t, func() bool { return calls.Load() > 1 },
		5*time.Second, 5*time.Millisecond, "the node-local build never started")

	// THE invariant: user writes confirm while the build hangs.
	seedUserProposals(t, d, s, "evt", []string{"during-hang"})

	// Release the build and require the pipeline to complete: the config
	// builds clean (no degraded record) — proving the message wasn't lost,
	// just deferred off the apply path.
	release <- struct{}{}
	require.Eventually(t, func() bool {
		return s.ConfigBuildErrorCount() == 0 && calls.Load() == 2
	}, 5*time.Second, 5*time.Millisecond, "released build did not complete cleanly")
}

// The degraded half of the same contract: a build that FAILS at dequeue
// (missing ${VAR}-class, node-local) lands as a loud degraded config —
// recorded asynchronously, after the propose has already confirmed — and
// user writes are never disturbed. The admission parse (first call)
// succeeds so the config clears the 400 gate; only the node-local build
// fails.
func TestApplyLiveness_ListenerBuildFailureLandsDegraded(t *testing.T) {
	var calls atomic.Int32
	failing := &clusterfakes.FakeSyncableParser{}
	failing.ParseStub = func(*cluster.ParsedConfig, cluster.DatabaseStorage) (cluster.Syncable, error) {
		if calls.Add(1) > 1 {
			return nil, errors.New("interpolate: variable SINK_DSN not set on this node")
		}
		return &clusterfakes.FakeSyncable{}, nil
	}
	p := parser.New()
	p.AddSyncableParser("hang", failing)
	d, s := newWalDBWithSyncListener(t, p)

	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "degraded-build", MimeType: "application/json",
		Data: []byte(`{"syncable": {"name": "degraded-build", "type": "hang"}}`),
	}))

	require.Eventually(t, func() bool {
		for _, e := range s.ConfigBuildErrors() {
			if e.Kind == "syncable" && e.ID == "degraded-build" {
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond, "failed build never recorded degraded")

	// The config bytes persisted (deterministic state machine) and user
	// writes flow — degraded is a node-local worker condition, not a log
	// condition.
	exists, err := s.SyncableExists("degraded-build")
	require.NoError(t, err)
	require.True(t, exists)
	seedUserProposals(t, d, s, "evt", []string{"after-degrade"})
}

// The abstraction line layer A draws is "apply never depends on
// EXTERNAL-SYSTEM AVAILABILITY" — not "apply does nothing but bbolt".
// Database configs deliberately still build their pool at apply (giving
// every downstream build read-after-write on storage.Database), which is
// sound only because pool construction is connection-LAZY: sql.Open /
// sql.OpenDB assemble the pool without dialing. This pins that property:
// the connection string points at TEST-NET-1 (RFC 5737, never routable —
// a dial hangs), so if the database build path ever grows a Ping/dial,
// this apply wedges and the propose times out. The entity is proposed
// RAW, bypassing ProposeDatabase, so admission stays free to add its own
// bounded connect-validation without tripping this pin — the fix for a
// red here is moving the I/O to admission or the listener, never back to
// the apply path.
func TestApplyLiveness_DatabaseBuildAtApplyIsConnectionLazy(t *testing.T) {
	t.Setenv("TEST_STALL_PGPW", "p")
	d, _ := newWalDBWithSQLParsers(t)

	e, err := cluster.NewUpsertDatabaseEntity(&cluster.Configuration{
		ID: "blackhole", Name: "blackhole", MimeType: "text/toml",
		Data: []byte("[database]\ntype = \"sql\"\nname = \"blackhole\"\n[sql]\ndialect = \"postgres\"\nconnectionString = \"postgres://u:${TEST_STALL_PGPW}@192.0.2.1:5432/db\"\n"),
	})
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}),
		"a database apply must stay connection-lazy — a build that dials would wedge the apply path on an unreachable sink")
}
