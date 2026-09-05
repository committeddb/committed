package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// migrationEditSink is a keyed, remat-capable recorder: it keeps every synced
// payload per key in arrival order, so the test can see rows BOTH as first
// synced (through the buggy migration) and as re-materialized (through the
// fixed one).
type migrationEditSink struct {
	mu       sync.Mutex
	payloads map[string][]string
}

func (f *migrationEditSink) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type != nil && !cluster.IsInternal(e.Type.ID) && e.Variant() == cluster.EntityVariantRow {
			f.payloads[string(e.Key)] = append(f.payloads[string(e.Key)], string(e.Data))
		}
	}
	return true, nil
}
func (f *migrationEditSink) Close() error                                         { return nil }
func (f *migrationEditSink) CanRematerialize() bool                               { return true }
func (f *migrationEditSink) BeginRematerialization(context.Context, uint64) error { return nil }
func (f *migrationEditSink) CompleteRematerialization(context.Context) error      { return nil }

func (f *migrationEditSink) latest(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ps := f.payloads[key]
	if len(ps) == 0 {
		return ""
	}
	return ps[len(ps)-1]
}

func (f *migrationEditSink) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, ps := range f.payloads {
		n += len(ps)
	}
	return n
}

// TestMigrationEditDependents pins the advisory's enumeration: only
// ALWAYS-CURRENT consumers of the edited type's topic are named — as-stored
// consumers deliver written bytes and are genuinely unaffected by a
// migration edit, and other topics' consumers are not dependents at all.
func TestMigrationEditDependents(t *testing.T) {
	sink := &migrationEditSink{payloads: map[string][]string{}}
	d, _ := newWalDBMigrationEdit(t, sink)

	post := func(id, topic, mode string) {
		t.Helper()
		toml := fmt.Sprintf("[syncable]\nname = %q\ntype = \"recorder\"\n", id)
		if mode != "" {
			toml = fmt.Sprintf("[syncable]\nname = %q\ntype = \"recorder\"\nmode = %q\n", id, mode)
		}
		toml += fmt.Sprintf("[recorder]\ntopic = %q\n", topic)
		require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
			ID: id, MimeType: "text/toml", Data: []byte(toml),
		}))
	}
	// The recorder parser ignores its config, but the ENVELOPE (mode) and
	// the sql.topic key drive the enumeration's parser reads.
	post("ac-on-topic", "photos", "always-current")
	post("stored-on-topic", "photos", "")
	post("ac-elsewhere", "other", "always-current")

	deps := d.MigrationEditDependents("photos")
	require.Len(t, deps, 1)
	require.Equal(t, "ac-on-topic", deps[0].ID)
}

// recorderTopicParser is the test's syncable kind: it returns the fixed sink
// and — unlike a bare counterfeiter fake — implements the topic extractor, so
// the dependents enumeration (which reads topics from config) sees real
// topology.
type recorderTopicParser struct{ sink cluster.Syncable }

func (p *recorderTopicParser) Parse(_ *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	return p.sink, nil
}

func (p *recorderTopicParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	if topic := v.GetString("recorder.topic"); topic != "" {
		return []string{topic}
	}
	return nil
}

// newWalDBMigrationEdit wires the standard single-node fixture with the
// recorder kind above.
func newWalDBMigrationEdit(t *testing.T, sink *migrationEditSink) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	p.AddSyncableParser("recorder", &recorderTopicParser{sink: sink})
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

// TestMigrationEditThenRematerializeFixesHistory is the ticket's success
// criterion end to end: rows synced through a BUGGY migration stay wrong
// after the in-place fix (the documented boundary), and the prescribed
// action — POST /rematerialize — replays them through the FIXED chain so the
// sink converges on the corrected output.
func TestMigrationEditThenRematerializeFixesHistory(t *testing.T) {
	sink := &migrationEditSink{payloads: map[string][]string{}}
	d, s := newWalDBMigrationEdit(t, sink)

	// v1 of the type, and rows STAMPED v1.
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object"}`, "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.Equal(t, 1, tp.Version)

	// An always-current consumer: every v1 row reaches it through the
	// current version's migration chain.
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "mirror", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"mirror\"\ntype = \"recorder\"\nmode = \"always-current\"\n[recorder]\ntopic = \"photos\"\n"),
	}))

	for i := 1; i <= 3; i++ {
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, fmt.Appendf(nil, "k%d", i), fmt.Appendf(nil, `{"v":%d}`, i)),
		}}))
	}

	// v2 with a BUGGY migration: v should scale by 100, the operator wrote 10.
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object","x":1}`,
		"\n[migration]\ntransform = \".v = .v * 10\"")
	require.Eventually(t, func() bool {
		tp2, rerr := s.ResolveType(cluster.LatestTypeRef("photos"))
		return rerr == nil && tp2.Version == 2
	}, 10*time.Second, 10*time.Millisecond)

	// The already-synced v1 rows went through NO migration (they were synced
	// while v1 was current); wait for all three, then observe the buggy
	// transform on a NEW row.
	require.Eventually(t, func() bool { return sink.total() >= 3 },
		10*time.Second, 10*time.Millisecond, "initial rows never synced")
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k4"), []byte(`{"v":4}`)),
	}}))
	require.Eventually(t, func() bool { return sink.latest("k4") == `{"v":40}` },
		10*time.Second, 10*time.Millisecond, "the buggy migration never applied: %q", sink.latest("k4"))

	// Fix the migration IN PLACE (same schema, new jq) — the "fix a buggy
	// migration" path: version stays 2, and the dependents enumeration names
	// the always-current mirror.
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object","x":1}`,
		"\n[migration]\ntransform = \".v = .v * 100\"")
	require.Eventually(t, func() bool {
		tp2, rerr := s.ResolveType(cluster.LatestTypeRef("photos"))
		return rerr == nil && tp2.Version == 2 && string(tp2.Migration) == ".v = .v * 100"
	}, 10*time.Second, 10*time.Millisecond, "the in-place edit never applied")
	deps := d.MigrationEditDependents("photos")
	require.Len(t, deps, 1)
	require.Equal(t, "mirror", deps[0].ID)

	// The DURABLE signal (the root-cause layer): the in-place edit moved the
	// interpretation coordinate past the mirror's pin — interpretationStale
	// flips on the status surface, from replicated state, with no dependence
	// on anyone having read the POST response.
	require.Eventually(t, func() bool {
		_, stale, serr := d.SyncableInterpretation("mirror")
		return serr == nil && stale
	}, 10*time.Second, 10*time.Millisecond, "the migration edit never flagged the always-current consumer stale")

	// The boundary the docs promise: k4's synced row still carries the buggy
	// output. The prescribed action re-materializes: every v1-stamped row
	// replays through the FIXED chain.
	require.Equal(t, `{"v":40}`, sink.latest("k4"), "already-synced rows must be untouched by the edit alone")
	require.NoError(t, d.RematerializeSyncable(testCtx(t), "mirror"))
	require.Eventually(t, func() bool {
		return sink.latest("k1") == `{"v":100}` &&
			sink.latest("k2") == `{"v":200}` &&
			sink.latest("k3") == `{"v":300}` &&
			sink.latest("k4") == `{"v":400}`
	}, 15*time.Second, 10*time.Millisecond,
		"re-materialization did not re-derive history through the fixed migration: k1=%q k4=%q",
		sink.latest("k1"), sink.latest("k4"))

	// Healing: the remat refreshed the pin to the current coordinate — the
	// staleness light turns off through the SAME verb restatements heal with.
	require.Eventually(t, func() bool {
		_, stale, serr := d.SyncableInterpretation("mirror")
		return serr == nil && !stale
	}, 10*time.Second, 10*time.Millisecond, "re-materialization must clear interpretationStale")
}

// TestMigrationEditStaleness_ScopeAndFreshPins pins the coordinate's edges:
// an as-stored consumer of the edited topic never goes stale (it applies no
// migrations), and a syncable created AFTER the edit pins the current
// coordinate — its from-0 replay reads through the fixed chain, so flagging
// it stale would be false.
func TestMigrationEditStaleness_ScopeAndFreshPins(t *testing.T) {
	sink := &migrationEditSink{payloads: map[string][]string{}}
	d, s := newWalDBMigrationEdit(t, sink)

	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object"}`, "")
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object","x":1}`,
		"\n[migration]\ntransform = \".v = .v * 10\"")

	// An as-stored consumer synced under the buggy transform era…
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "stored", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"stored\"\ntype = \"recorder\"\n[recorder]\ntopic = \"photos\"\n"),
	}))
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"v":1}`)),
	}}))
	require.Eventually(t, func() bool { return sink.total() >= 1 }, 10*time.Second, 10*time.Millisecond)

	// …the in-place fix lands…
	proposeTypeTOML(t, d, "photos", "photos", `{"type":"object","x":1}`,
		"\n[migration]\ntransform = \".v = .v * 100\"")
	require.Eventually(t, func() bool {
		return s.TypeMigrationEditedAt("photos") > 0
	}, 10*time.Second, 10*time.Millisecond, "the edit coordinate never recorded")

	// …and the as-stored consumer stays fresh: written bytes are its truth.
	_, stale, serr := d.SyncableInterpretation("stored")
	require.NoError(t, serr)
	require.False(t, stale, "an as-stored consumer must not go stale on a migration edit")

	// A NEW always-current consumer created after the edit pins the current
	// coordinate: from-0 replay reads the fixed chain — never stale.
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "fresh-ac", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"fresh-ac\"\ntype = \"recorder\"\nmode = \"always-current\"\n[recorder]\ntopic = \"photos\"\n"),
	}))
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k2"), []byte(`{"v":2}`)),
	}}))
	require.Eventually(t, func() bool {
		cp, _, perr := d.SyncableProgress("fresh-ac")
		return perr == nil && cp > 0
	}, 10*time.Second, 10*time.Millisecond, "the fresh consumer never checkpointed")
	_, stale, serr = d.SyncableInterpretation("fresh-ac")
	require.NoError(t, serr)
	require.False(t, stale, "a consumer created after the edit pins the current coordinate — flagging it stale would be false")
}
