package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/http"
)

const tripwireSchema = `{"type":"object","properties":{"caption":{"type":"string"},"size":{"type":"number"}},"additionalProperties":false}`

// proposeAnnounceFixtures declares the events topic and an announce-typed
// topic pointed at it, returning the resolved announce type.
func proposeAnnounceFixtures(t *testing.T, d *db.DB, s *wal.Storage) *cluster.Type {
	t.Helper()
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "schema-changes", MimeType: "text/toml",
		Data: []byte("[type]\nname = \"SchemaChanges\"\nentityKind = \"standalone\""),
	}))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "photo-meta", MimeType: "text/toml",
		Data: []byte(fmt.Sprintf("[type]\nname = \"PhotoMeta\"\nschemaType = \"JSONSchema\"\nschema = '%s'\nvalidate = 2\nschemaChangeTopic = \"schema-changes\"", tripwireSchema)),
	}))
	tp, err := s.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.NoError(t, err)
	require.Equal(t, cluster.ValidateAnnounce, tp.Validate)
	require.Equal(t, "schema-changes", tp.SchemaChangeTopic)
	return tp
}

// readContractExtensions drains a fresh reader and returns every event on the
// schema-changes topic, keyed order preserved.
func readContractExtensions(t *testing.T, s *wal.Storage, cursor string) []*cluster.ContractExtension {
	t.Helper()
	var events []*cluster.ContractExtension
	r := s.Reader(cursor)
	for {
		a, err := r.Read()
		if err != nil {
			break
		}
		for _, e := range a.Entities {
			if e.Type != nil && e.Type.ID == "schema-changes" {
				ce := &cluster.ContractExtension{}
				require.NoError(t, json.Unmarshal(e.Data, ce))
				events = append(events, ce)
			}
		}
	}
	return events
}

// TestTripwire_AnnouncesEachDivergentShapeOnce drives the whole engine
// through Propose with real storage and the real schema compilers: a
// divergent payload commits AND announces (event + dedupe mark, atomically);
// repeats of the same shape — same proposal, later proposals — stay silent; a
// new shape announces again; conformant payloads and delete tombstones never
// announce.
func TestTripwire_AnnouncesEachDivergentShapeOnce(t *testing.T) {
	d, s := newWalDB(t)
	sv := &http.SchemaValidator{}
	d.SetTypeSchemaValidator(sv)
	d.SetEntityValidator(sv)
	tp := proposeAnnounceFixtures(t, d, s)

	// A conformant payload: commits, no event.
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k0"), []byte(`{"caption":"ok","size":1}`))}}))
	require.Empty(t, readContractExtensions(t, s, "verify-0"))

	// A divergent payload (added path + type mismatch): commits AND announces.
	divergent := []byte(`{"caption":7,"ai_labels":{"model":"v9"}}`)
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k1"), divergent)}}))

	events := readContractExtensions(t, s, "verify-1")
	require.Len(t, events, 1, "one distinct shape, one event")
	ev := events[0]
	require.Equal(t, "photo-meta", ev.TypeID)
	require.Equal(t, tp.Version, ev.Version)
	require.NotEmpty(t, ev.Fingerprint)
	require.Contains(t, ev.ObservedShape, "$.ai_labels.model:string")
	require.NotEmpty(t, ev.Violations)
	require.True(t, s.HasContractFingerprint("photo-meta", tp.Version, ev.Fingerprint),
		"the dedupe mark commits with the event")

	// Same shape again (different values, two rows in one proposal): silent.
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k2"), []byte(`{"caption":8,"ai_labels":{"model":"v10"}}`)),
		cluster.NewUpsertEntity(tp, []byte("k3"), []byte(`{"caption":9,"ai_labels":{"model":"v11"}}`)),
	}}))
	require.Len(t, readContractExtensions(t, s, "verify-2"), 1, "an announced shape never re-announces")

	// A DIFFERENT divergent shape announces its own event.
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k4"), []byte(`{"caption":"x","license":"cc"}`))}}))
	events = readContractExtensions(t, s, "verify-3")
	require.Len(t, events, 2)
	require.NotEqual(t, events[0].Fingerprint, events[1].Fingerprint)

	// Delete tombstones carry no payload — never validated, never announced.
	require.NoError(t, d.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewDeleteEntity(tp, []byte("k1"))}}))
	require.Len(t, readContractExtensions(t, s, "verify-4"), 2)

	// And every data payload above actually committed (the tripwire never
	// gates): 5 rows + 1 tombstone on the photo-meta topic.
	var dataEntities int
	r := s.Reader("verify-data")
	for {
		a, err := r.Read()
		if err != nil {
			break
		}
		for _, e := range a.Entities {
			if e.Type != nil && e.Type.ID == "photo-meta" {
				dataEntities++
			}
		}
	}
	require.Equal(t, 6, dataEntities)
}

// TestTripwire_DedupeSurvivesRestart pins the mark's durability: the same
// divergent shape proposed after a full storage close/reopen does not
// re-announce — the dedupe is replicated consensus state, not a worker note.
func TestTripwire_DedupeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	s1, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d1 := db.New(uint64(1), db.Peers{1: ""}, s1, p, nil, nil, db.WithTickInterval(testTickInterval))
	sv := &http.SchemaValidator{}
	d1.SetTypeSchemaValidator(sv)
	d1.SetEntityValidator(sv)

	tp := proposeAnnounceFixtures(t, d1, s1)
	divergent := []byte(`{"caption":7,"ai_labels":{"model":"v9"}}`)
	require.NoError(t, d1.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp, []byte("k1"), divergent)}}))
	require.Len(t, readContractExtensions(t, s1, "before-restart"), 1)

	require.NoError(t, d1.Close())
	require.NoError(t, s1.Close())

	s2, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d2 := db.New(uint64(1), db.Peers{1: ""}, s2, p, nil, nil, db.WithTickInterval(testTickInterval))
	d2.SetTypeSchemaValidator(sv)
	d2.SetEntityValidator(sv)
	t.Cleanup(func() { _ = d2.Close(); _ = s2.Close() })

	tp2, err := s2.ResolveType(cluster.LatestTypeRef("photo-meta"))
	require.NoError(t, err)
	require.NoError(t, d2.Propose(testCtx(t),
		&cluster.Proposal{Entities: []*cluster.Entity{cluster.NewUpsertEntity(tp2, []byte("k9"), []byte(`{"caption":1,"ai_labels":{"model":"v12"}}`))}}))
	require.Len(t, readContractExtensions(t, s2, "after-restart"), 1,
		"an announced shape stays announced across a restart")
}

// TestTripwire_IngestWorkerLanesAnnounce drives divergent payloads through a
// REAL ingest worker — both of its propose lanes bypass or re-enter
// db.Propose differently (the pipelined snapshot lane submits via
// proposeAsync; the ordered CDC lane goes through proposeIngestData →
// Propose) — and pins that each lane announces, that CDC events carry the
// source position, and that ingest never pauses (the data commits and the
// highwater advances normally).
func TestTripwire_IngestWorkerLanesAnnounce(t *testing.T) {
	d, s := newWalDB(t)
	sv := &http.SchemaValidator{}
	d.SetTypeSchemaValidator(sv)
	d.SetEntityValidator(sv)
	tp := proposeAnnounceFixtures(t, d, s)

	id := "tripwire-ingest"
	seedIngestableConfig(t, d, id)

	// One snapshot-lane event (SourceSeq 0, pipelined) with divergent shape A,
	// one CDC-lane event (SourceSeq 7, ordered) with divergent shape B.
	worker := &seqIngestable{typ: tp, events: []seqEvent{
		{0, `{"caption":1}`},
		{7, `{"caption":"x","license":"cc"}`},
	}}
	require.NoError(t, d.Ingest(context.Background(), id, worker))

	require.Eventually(t, func() bool {
		return s.IngestSourceSeqHighwater(id) == 7
	}, 10*time.Second, 5*time.Millisecond, "the divergent CDC row must commit — the tripwire never pauses ingest")

	var events []*cluster.ContractExtension
	require.Eventually(t, func() bool {
		events = readContractExtensions(t, s, "verify-ingest")
		return len(events) == 2
	}, 10*time.Second, 5*time.Millisecond, "each lane's divergent shape announces once")

	// Both lanes' events carry the ingestable id (stamped before the lane
	// split); the CDC-lane event additionally locates its source position,
	// the snapshot-lane event carries 0 (a snapshot row has no position).
	seqs := map[uint64]bool{}
	for _, ev := range events {
		require.Equal(t, "photo-meta", ev.TypeID)
		require.Equal(t, id, ev.IngestableID)
		seqs[ev.SourceSeq] = true
	}
	require.True(t, seqs[0] && seqs[7], "one snapshot-lane event (seq 0) and one CDC-lane event (seq 7): %v", seqs)
}

// TestProposeType_AnnounceAdmission pins the loud-at-POST rules: announce
// requires a destination; the destination must already exist and must not
// itself be announce-typed; a destination without announce is refused; and
// self-reference is refused.
func TestProposeType_AnnounceAdmission(t *testing.T) {
	d, _ := newWalDB(t)

	base := "[type]\nname = \"T\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'"

	// announce without a destination → refused.
	err := d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-a", MimeType: "text/toml", Data: []byte(base + "\nvalidate = 2"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schemaChangeTopic")

	// a destination without announce → refused.
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-b", MimeType: "text/toml", Data: []byte(base + "\nschemaChangeTopic = \"somewhere\""),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "only valid with validate = 2")

	// self-reference → refused.
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-c", MimeType: "text/toml", Data: []byte(base + "\nvalidate = 2\nschemaChangeTopic = \"t-c\""),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be the type itself")

	// a destination that does not exist yet → refused (declare it first).
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-d", MimeType: "text/toml", Data: []byte(base + "\nvalidate = 2\nschemaChangeTopic = \"missing\""),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not name an existing type")

	// an announce-typed destination → refused (no chained events topics).
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "events-plain", MimeType: "text/toml", Data: []byte("[type]\nname = \"Events\""),
	}))
	require.NoError(t, d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "events-announcing", MimeType: "text/toml",
		Data: []byte(base + "\nvalidate = 2\nschemaChangeTopic = \"events-plain\""),
	}))
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-e", MimeType: "text/toml",
		Data: []byte(base + "\nvalidate = 2\nschemaChangeTopic = \"events-announcing\""),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "itself announce-typed")

	// an unknown strategy integer → refused.
	err = d.ProposeType(testCtx(t), &cluster.Configuration{
		ID: "t-f", MimeType: "text/toml", Data: []byte(base + "\nvalidate = 9"),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a known validation strategy")
}
