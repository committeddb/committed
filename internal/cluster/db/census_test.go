package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/http"
)

// censusIngestable emits snapshot rows (SourceSeq 0, Generation = epoch),
// closes the pass with a refresh-boundary marker (which forces the census to
// publish), and then blocks until cancelled — the in-process stand-in for a
// dialect's snapshot pass.
type censusIngestable struct {
	typ   *cluster.Type
	epoch uint64
	rows  [][]byte
}

func (ci *censusIngestable) Ingest(ctx context.Context, _ cluster.Position, pr chan<- *cluster.Proposal, _ chan<- cluster.Position) error {
	for i, data := range ci.rows {
		e := cluster.NewUpsertEntity(ci.typ, fmt.Appendf(nil, "k%d", i), data)
		e.Generation = ci.epoch
		select {
		case pr <- &cluster.Proposal{Entities: []*cluster.Entity{e}}:
		case <-ctx.Done():
			return nil
		}
	}
	marker := cluster.NewRefreshBoundaryEntity(ci.typ, ci.epoch)
	select {
	case pr <- &cluster.Proposal{Entities: []*cluster.Entity{marker}}:
	case <-ctx.Done():
		return nil
	}
	<-ctx.Done()
	return nil
}

func (ci *censusIngestable) Close() error { return nil }

func (ci *censusIngestable) Status(context.Context, cluster.Position) (cluster.IngestableStatus, error) {
	return cluster.IngestableStatus{}, nil
}

// newWalDBAnnounced is newWalDB plus the feature-level self-announce — the
// census tests close snapshot passes with refresh-boundary markers, whose
// emission is gated on every member having announced (awaitRefreshBoundaryEnabled).
func newWalDBAnnounced(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithVersionAnnounce())
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

func waitForCensus(t *testing.T, s *wal.Storage, id string, cond func(*cluster.IngestableCensus) bool) *cluster.IngestableCensus {
	t.Helper()
	var got *cluster.IngestableCensus
	require.Eventually(t, func() bool {
		c, ok := s.IngestableCensus(id)
		if !ok {
			return false
		}
		got = c
		return cond(c)
	}, 15*time.Second, 10*time.Millisecond, "census never published")
	return got
}

// TestCensus_SnapshotPassPublishesReplicatedCensus drives the worker fold end
// to end: snapshot rows (two interleaved shapes) → the refresh boundary
// forces a publish → the replicated record carries shapes with counts and row
// ranges. Default-on: the seeded config declares nothing census-related.
func TestCensus_SnapshotPassPublishesReplicatedCensus(t *testing.T) {
	d, s := newWalDBAnnounced(t)
	id := "census-ingest"
	seedIngestableConfig(t, d, id)
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)

	worker := &censusIngestable{typ: typ, epoch: 1, rows: [][]byte{
		[]byte(`{"caption":"a","size":1}`),
		[]byte(`{"caption":"b","ai":{"m":"v9"}}`),
		[]byte(`{"caption":"c","size":2}`),
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker))

	c := waitForCensus(t, s, id, func(c *cluster.IngestableCensus) bool {
		tc := c.Topics["photos"]
		return tc != nil && tc.Rows == 3
	})
	require.Equal(t, uint64(1), c.RefreshEpoch)
	tc := c.Topics["photos"]
	require.Len(t, tc.Shapes, 2, "two distinct shapes")
	paths := map[string]*cluster.PathCensus{}
	for _, pc := range tc.PathsView() {
		paths[pc.Path] = pc
	}
	require.Equal(t, uint64(3), paths["$.caption"].Count)
	require.Equal(t, uint64(2), paths["$.size"].Count)
	require.Equal(t, uint64(1), paths["$.ai.m"].Count)
	require.Nil(t, tc.Values, "no value tracking without the opt-in — the PII posture")

	// A fresh full snapshot at a HIGHER epoch resets the census instead of
	// double-counting the re-observed rows.
	worker2 := &censusIngestable{typ: typ, epoch: 2, rows: [][]byte{
		[]byte(`{"caption":"a","size":1}`),
		[]byte(`{"caption":"c","size":2}`),
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker2))
	c = waitForCensus(t, s, id, func(c *cluster.IngestableCensus) bool {
		return c.RefreshEpoch == 2
	})
	require.Equal(t, uint64(2), c.Topics["photos"].Rows, "the higher epoch reset the census")
	require.Len(t, c.Topics["photos"].Shapes, 1)
}

// TestCensus_OptOutAndValueTracking pins the two config knobs on the
// [ingestable] envelope: census = false disables the census entirely, and
// censusValues = true opts into bounded distinct-value tracking.
func TestCensus_OptOutAndValueTracking(t *testing.T) {
	d, s := newWalDBAnnounced(t)
	proposeTypeTOML(t, d, "evt", "evt", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("evt"))
	require.NoError(t, err)

	// Opt-out: no census record ever appears.
	optOut := "census-off"
	seedIngestableConfigTOML(t, d, optOut, "[ingestable]\nname = \"census-off\"\ncensus = false\n")
	require.NoError(t, d.Ingest(context.Background(), optOut,
		&censusIngestable{typ: typ, epoch: 1, rows: [][]byte{[]byte(`{"a":"x"}`)}}))

	// Values opt-in on a second ingestable.
	withValues := "census-values"
	seedIngestableConfigTOML(t, d, withValues, "[ingestable]\nname = \"census-values\"\ncensusValues = true\n")
	require.NoError(t, d.Ingest(context.Background(), withValues,
		&censusIngestable{typ: typ, epoch: 1, rows: [][]byte{
			[]byte(`{"license":"cc"}`), []byte(`{"license":"arr"}`),
		}}))

	c := waitForCensus(t, s, withValues, func(c *cluster.IngestableCensus) bool {
		tc := c.Topics["evt"]
		return tc != nil && tc.Rows == 2
	})
	require.Equal(t, []string{"arr", "cc"}, c.Topics["evt"].Values["$.license"].Values)

	_, ok := s.IngestableCensus(optOut)
	require.False(t, ok, "census = false publishes nothing")
}

// seedIngestableConfigTOML seeds an ingestable config with the given TOML
// envelope (the census knobs live there — worker-level, dialect-agnostic).
func seedIngestableConfigTOML(t *testing.T, d *db.DB, id, toml string) {
	t.Helper()
	e, err := cluster.NewUpsertIngestableEntity(&cluster.Configuration{
		ID: id, MimeType: "text/toml", Data: []byte(toml),
	})
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
}

// TestCensus_DraftBlessTripwire is the joint criterion with the validation
// tripwire — the Erik enum case as one flow, with NO separate value-watch
// feature: the census (values opted in) drafts a schema whose low-cardinality
// license field is an enum; the operator blesses the draft as an
// announce-typed contract; a row carrying a NEW enum value then fires the
// tripwire as an ordinary schema violation.
func TestCensus_DraftBlessTripwire(t *testing.T) {
	d, s := newWalDBAnnounced(t)
	sv := &http.SchemaValidator{}
	d.SetTypeSchemaValidator(sv)
	d.SetEntityValidator(sv)

	id := "census-bless"
	seedIngestableConfigTOML(t, d, id, "[ingestable]\nname = \"census-bless\"\ncensusValues = true\n")
	proposeTypeTOML(t, d, "photo-meta", "photo-meta", "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.NoError(t, err)

	// The snapshot pass observes two license values, one of them repeating
	// (the enum heuristic requires repeat evidence); captions are free text.
	require.NoError(t, d.Ingest(context.Background(), id, &censusIngestable{typ: typ, epoch: 1, rows: [][]byte{
		[]byte(`{"caption":"a","license":"cc"}`),
		[]byte(`{"caption":"b","license":"arr"}`),
		[]byte(`{"caption":"c","license":"cc"}`),
	}}))
	c := waitForCensus(t, s, id, func(c *cluster.IngestableCensus) bool {
		tc := c.Topics["photo-meta"]
		return tc != nil && tc.Rows == 3
	})

	draft, err := c.Topics["photo-meta"].DraftTypeSchema()
	require.NoError(t, err)
	require.Contains(t, string(draft), `"enum"`, "the low-cardinality license drafts as an enum")

	// Bless the draft: the events topic first, then the announce-typed
	// contract carrying the drafted schema verbatim.
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "schema-changes", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"SchemaChanges\"\nentityKind = \"standalone\""),
	}))
	blessed := fmt.Sprintf(`[type]
name = "photo-meta"
schemaType = "JSONSchema"
schema = '''%s'''
validate = 2
schemaChangeTopic = "schema-changes"

[migration]
none = true
`, string(draft))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "photo-meta", MimeType: "text/toml", Data: []byte(blessed),
	}))
	tp2, err := s.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.NoError(t, err)
	require.Equal(t, cluster.ValidateAnnounce, tp2.Validate)

	// A known enum value conforms — no event. A NEW enum value is a schema
	// violation: the tripwire announces it, keyword "enum".
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp2, []byte("k1"), []byte(`{"caption":"x","license":"cc"}`))}}))
	require.Empty(t, readContractExtensions(t, s, "census-verify-0"))

	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp2, []byte("k2"), []byte(`{"caption":"y","license":"gpl"}`))}}))
	events := readContractExtensions(t, s, "census-verify-1")
	require.Len(t, events, 1, "a new enum value fires the tripwire — no separate value-watch feature")
	var sawEnum bool
	for _, v := range events[0].Violations {
		if strings.Contains(v.Keyword, "enum") {
			sawEnum = true
		}
	}
	require.True(t, sawEnum, "the violation names the enum keyword: %+v", events[0].Violations)

	// And the draft round-trips as valid JSON (an operator pastes it as-is).
	var compiled map[string]any
	require.NoError(t, json.Unmarshal(draft, &compiled))
}
