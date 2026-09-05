package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/syncable/loopback"
)

// recordedEntity is one entity a recorderSink observed, flattened for asserts.
type recordedEntity struct {
	topic      string
	key        string
	data       string
	generation uint64
	variant    cluster.EntityVariant
}

// recorderSink records every non-internal entity it is handed, across all
// topics — the observer for what actually lands on a derived topic.
type recorderSink struct {
	mu       sync.Mutex
	entities []recordedEntity
}

func (r *recorderSink) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type == nil || cluster.IsInternal(e.Type.ID) {
			continue
		}
		r.entities = append(r.entities, recordedEntity{
			topic:      e.Type.ID,
			key:        string(e.Key),
			data:       string(e.Data),
			generation: e.Generation,
			variant:    e.Variant(),
		})
	}
	return true, nil
}
func (r *recorderSink) Close() error { return nil }

func (r *recorderSink) onTopic(topic string) []recordedEntity {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recordedEntity
	for _, e := range r.entities {
		if e.topic == topic {
			out = append(out, e)
		}
	}
	return out
}

// newWalDBLoopback wires a fixture with the REAL loopback parser (proposing
// back into this db) plus a "recorder" syncable kind, with real pump channels
// so workers run — the same shape cmd/node.go wires.
func newWalDBLoopback(t *testing.T, sink *recorderSink) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	recParser := &clusterfakes.FakeSyncableParser{}
	recParser.ParseReturns(sink, nil)
	p.AddSyncableParser("recorder", recParser)
	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(dir, p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, db.WithTickInterval(testTickInterval))
	// Registered on d AFTER db.New, like cmd/node.go: the loopback needs d as
	// its Proposer. Same parser map underneath, so wal builds see it too.
	d.AddSyncableParser("loopback", &loopback.SyncableParser{Proposer: d})
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

func proposeKindedType(t *testing.T, d *db.DB, id, kind string) {
	t.Helper()
	data := fmt.Sprintf("[type]\nname = %q\nentityKind = %q\n", id, kind)
	require.NoError(t, d.ProposeType(testCtx(t),
		&cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(data)}))
}

func proposeLoopback(t *testing.T, d *db.DB, id, source, target, extra string) error {
	t.Helper()
	data := fmt.Sprintf("[syncable]\nname = %q\ntype = \"loopback\"\n[loopback]\ntopic = %q\ntarget = %q\n%s",
		id, source, target, extra)
	return d.ProposeSyncable(testCtx(t),
		&cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte(data)})
}

// TestLoopback_DerivesAndForwards drives the whole derivation chain through
// real workers: rows proposed to the raw topic are transformed ONCE by the
// loopback and land on the derived topic — where a plain downstream sink
// consumes them dumb — with keys, generations, delete tombstones, and
// refresh-boundary markers all forwarded faithfully.
func TestLoopback_DerivesAndForwards(t *testing.T) {
	sink := &recorderSink{}
	d, s := newWalDBLoopback(t, sink)

	proposeKindedType(t, d, "raw", "snapshot")
	proposeKindedType(t, d, "canon", "snapshot")
	rawType, err := s.ResolveType(cluster.LatestTypeRef("raw"))
	require.NoError(t, err)

	require.NoError(t, proposeLoopback(t, d, "canonizer", "raw", "canon",
		"[[loopback.mappings]]\njsonPath = \"$.id\"\nfield = \"id\"\n[[loopback.mappings]]\njsonPath = \"$.deep.title\"\nfield = \"title\"\n"))
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "canon-sink", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"canon-sink\"\ntype = \"recorder\"\n"),
	}))

	// Rows with a generation, in order.
	for i, k := range []string{"k1", "k2"} {
		e := cluster.NewUpsertEntity(rawType, []byte(k),
			[]byte(fmt.Sprintf(`{"id":%q,"deep":{"title":"t%d"},"noise":true}`, k, i)))
		e.Generation = 7
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))
	}
	require.Eventually(t, func() bool {
		return len(sink.onTopic("canon")) >= 2
	}, 15*time.Second, 10*time.Millisecond, "derived rows never reached the downstream sink")

	rows := sink.onTopic("canon")
	require.Equal(t, "k1", rows[0].key, "key preserved through the derivation")
	var payload map[string]any
	require.NoError(t, json.Unmarshal([]byte(rows[0].data), &payload))
	require.Equal(t, map[string]any{"id": "k1", "title": "t0"}, payload, "transformed shape, noise dropped")
	require.Equal(t, uint64(7), rows[0].generation, "source generation forwarded verbatim")
	require.Equal(t, "k2", rows[1].key, "log order preserved")

	// A delete tombstone chases the chain (the RTBF guarantee)…
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewDeleteEntity(rawType, []byte("k1"))}}))
	// …and a refresh-boundary marker forwards at the same epoch, so an ingest
	// full-refresh of the source reconciles all the way downstream.
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewRefreshBoundaryEntity(rawType, 8)}}))

	require.Eventually(t, func() bool {
		return len(sink.onTopic("canon")) >= 4
	}, 15*time.Second, 10*time.Millisecond, "delete/boundary never forwarded")
	rows = sink.onTopic("canon")
	require.Equal(t, cluster.EntityVariantDelete, rows[2].variant)
	require.Equal(t, "k1", rows[2].key)
	require.Equal(t, cluster.EntityVariantRefresh, rows[3].variant)
	require.Equal(t, uint64(8), rows[3].generation)

	// Derivation provenance is queryable.
	source, target, ok := d.SyncableDerivation("canonizer")
	require.True(t, ok)
	require.Equal(t, "raw", source)
	require.Equal(t, "canon", target)
	_, _, ok = d.SyncableDerivation("canon-sink")
	require.False(t, ok, "non-deriving kinds report no derivation")
}

// TestLoopback_GraphGuardsAtAdmission pins the two derivation-graph
// invariants at POST: no cycles (direct or via a chain), and one producer
// per derived topic.
func TestLoopback_GraphGuardsAtAdmission(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)

	for _, id := range []string{"a", "b", "c"} {
		proposeKindedType(t, d, id, "snapshot")
	}
	require.NoError(t, proposeLoopback(t, d, "ab", "a", "b", ""))

	err := proposeLoopback(t, d, "ba", "b", "a", "")
	require.ErrorContains(t, err, "derivation cycle", "a direct cycle is refused")

	require.NoError(t, proposeLoopback(t, d, "bc", "b", "c", ""), "chains are fine (a DAG, not a tree)")
	err = proposeLoopback(t, d, "ca", "c", "a", "")
	require.ErrorContains(t, err, "derivation cycle", "a cycle through a chain is refused")
	require.ErrorContains(t, err, "c → a → b → c", "the refusal names the cycle path")

	err = proposeLoopback(t, d, "second-producer", "c", "b", "")
	require.ErrorContains(t, err, "already has a producer", "fan-in is refused")

	// A re-POST of an existing config does not collide with its own edges.
	require.NoError(t, proposeLoopback(t, d, "ab", "a", "b",
		"[[loopback.mappings]]\njsonPath = \"$.id\"\nfield = \"id\"\n"))
}

// TestLoopback_RaceCommittedCycleDegradesDeterministically: a config that
// slips PAST the leader's admission check (raced proposes; simulated here by
// proposing the raw config entity directly) is refused at APPLY by the
// deterministic log-index-order replay: persisted, loudly degraded, never
// run — so the infinite consensus loop is impossible, not just discouraged.
func TestLoopback_RaceCommittedCycleDegradesDeterministically(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)

	proposeKindedType(t, d, "a", "snapshot")
	proposeKindedType(t, d, "b", "snapshot")
	require.NoError(t, proposeLoopback(t, d, "ab", "a", "b", ""))

	// Bypass admission: commit the cycle-closing config as a raw entity.
	cfg := &cluster.Configuration{
		ID: "ba", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"ba\"\ntype = \"loopback\"\n[loopback]\ntopic = \"b\"\ntarget = \"a\"\n"),
	}
	e, err := cluster.NewUpsertSyncableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	require.Eventually(t, func() bool {
		for _, ce := range d.ConfigBuildErrors() {
			if ce.Kind == "syncable" && ce.ID == "ba" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "the raced-in cycle config never degraded")

	// The earlier (innocent) config is untouched.
	for _, ce := range d.ConfigBuildErrors() {
		require.NotEqual(t, "ab", ce.ID, "the earlier config must stay accepted")
	}
}

// TestLoopback_Rematerialize: the loopback implements the re-materialization
// verb natively — a plain replay converges a snapshot-kind derived topic
// (transforms are total and key-preserving), and the downstream sink simply
// sees the re-derived rows again.
func TestLoopback_Rematerialize(t *testing.T) {
	sink := &recorderSink{}
	d, s := newWalDBLoopback(t, sink)

	proposeKindedType(t, d, "raw", "snapshot")
	proposeKindedType(t, d, "canon", "snapshot")
	rawType, err := s.ResolveType(cluster.LatestTypeRef("raw"))
	require.NoError(t, err)

	require.NoError(t, proposeLoopback(t, d, "canonizer", "raw", "canon", ""))
	require.NoError(t, d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: "canon-sink", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"canon-sink\"\ntype = \"recorder\"\n"),
	}))

	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(rawType, []byte(k), []byte(`{"a":1}`)),
		}}))
	}
	require.Eventually(t, func() bool {
		return len(sink.onTopic("canon")) >= 3
	}, 15*time.Second, 10*time.Millisecond, "initial derivation never completed")

	require.NoError(t, d.RematerializeSyncable(testCtx(t), "canonizer"))

	// The replay re-derives every row; the record clears on completion.
	require.Eventually(t, func() bool {
		return len(sink.onTopic("canon")) >= 6
	}, 15*time.Second, 10*time.Millisecond, "the replay never re-derived the topic")
	require.Eventually(t, func() bool {
		_, ok := s.SyncableRematerialization("canonizer")
		return !ok
	}, 10*time.Second, 10*time.Millisecond, "the in-progress record never cleared")

	rows := sink.onTopic("canon")
	require.Equal(t, []string{"k1", "k2", "k3"}, []string{
		rows[len(rows)-3].key, rows[len(rows)-2].key, rows[len(rows)-1].key,
	}, "the replay re-derived in log order")
}

// TestLoopback_RematerializeRefusedForAppendTargets: an acknowledged
// event-kind derived topic accepts normal-operation duplicate semantics, but
// a wholesale re-append is refused — the verb requires a target that
// converges.
func TestLoopback_RematerializeRefusedForAppendTargets(t *testing.T) {
	sink := &recorderSink{}
	d, _ := newWalDBLoopback(t, sink)

	proposeKindedType(t, d, "raw", "snapshot")
	proposeKindedType(t, d, "events", "event")
	require.NoError(t, proposeLoopback(t, d, "eventizer", "raw", "events",
		"acknowledgeAppendSemantics = true\n"))

	err := d.RematerializeSyncable(testCtx(t), "eventizer")
	require.ErrorIs(t, err, cluster.ErrNotRematerializable)
}
