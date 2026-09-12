package db_test

import (
	"context"
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
	"github.com/committeddb/committed/internal/version"
)

// zoneRecorderSink records synced keys — the observer for whether a pinned
// worker serves.
type zoneRecorderSink struct {
	mu   sync.Mutex
	keys []string
}

func (r *zoneRecorderSink) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range a.Entities {
		if e.Type != nil && !cluster.IsInternal(e.Type.ID) && e.Variant() == cluster.EntityVariantRow {
			r.keys = append(r.keys, string(e.Key))
		}
	}
	return true, nil
}
func (r *zoneRecorderSink) Close() error { return nil }

func (r *zoneRecorderSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

// newWalDBZone wires an announce-enabled single-node fixture whose node has
// the given zone, with a "recorder" syncable kind and real pump channels.
// With announce, it returns only after BOTH startup announcements (zone and
// feature level) have APPLIED: the fixture's callers assert against the
// feature-gated admission and serving paths, and a fixture handed out before
// the async announcements land gives every point-in-time assertion a startup
// race — the rolling-upgrade refusal where a zone error is pinned, or a
// leader-served write from a pin the gate would stall.
func newWalDBZone(t *testing.T, nodeZone string, sink *zoneRecorderSink, announce bool) (*db.DB, *wal.Storage) {
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
	opts := []db.Option{db.WithTickInterval(testTickInterval)}
	if announce {
		opts = append(opts, db.WithVersionAnnounce(), db.WithZone(nodeZone))
	}
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, syncCh, ingestCh, opts...)
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	if announce {
		require.Eventually(t, func() bool {
			zone, ok := s.MemberZone(1)
			return ok && zone == nodeZone
		}, 10*time.Second, 10*time.Millisecond, "zone never announced")
		waitForMemberVersion(t, d, 1, version.FeatureLevel)
	}
	return d, s
}

func proposePinnedRecorder(t *testing.T, d *db.DB, id, zone string) error {
	t.Helper()
	toml := fmt.Sprintf("[syncable]\nname = %q\ntype = \"recorder\"\nzone = %q\n", id, zone)
	return d.ProposeSyncable(testCtx(t), &cluster.Configuration{
		ID: id, MimeType: "text/toml", Data: []byte(toml),
	})
}

// TestZonePin_AdmissionAndServing drives the happy path on a single-node
// cluster: the node announces its zone, a config pinned to that zone is
// admitted and SERVED (ownership resolves via the pin, not the leader
// fallback), and status reports the pin. A pin to an unserved zone is
// refused loudly at POST.
func TestZonePin_AdmissionAndServing(t *testing.T) {
	sink := &zoneRecorderSink{}
	d, s := newWalDBZone(t, "z-east", sink, true)

	// Admission: unserved zone refused loudly; served zone admitted. (The
	// fixture has already waited for both startup announcements to apply.)
	err := proposePinnedRecorder(t, d, "pinned", "z-ghost")
	require.Error(t, err, "a pin to an unserved zone must be refused at POST")
	require.Contains(t, err.Error(), "z-ghost")

	require.NoError(t, proposePinnedRecorder(t, d, "pinned", "z-east"))

	// The pinned syncable is served (by this node, resolved via the pin).
	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(tp, []byte("k1"), []byte(`{"a":1}`)),
	}}))
	require.Eventually(t, func() bool { return sink.count() >= 1 },
		10*time.Second, 10*time.Millisecond, "the pinned worker never served")

	// Status surface.
	zone, unsatisfiable, ok := d.SyncableZonePin("pinned")
	require.True(t, ok)
	require.Equal(t, "z-east", zone)
	require.False(t, unsatisfiable)
	require.Equal(t, uint64(1), d.SyncableOwner("pinned"))

	// Owner-executed verbs pass the pin guard (this node IS the pinned
	// owner): the remat probe reaches the sink-capability check — the
	// recorder sink can't converge a replay, which is the NEXT error after
	// the routing guard. Never the routing refusal.
	require.ErrorIs(t, d.RematerializeSyncable(testCtx(t), "pinned"), cluster.ErrNotRematerializable)

	// An unpinned syncable reports no pin.
	_, _, ok = d.SyncableZonePin("nope")
	require.False(t, ok)
}

// TestZonePin_FeatureGateRefusesOnColdCluster: with no feature-level
// announcements (a mixed-version cluster's conservative floor), a pinned
// config is refused 503-shaped — never silently accepted as leader-served.
func TestZonePin_FeatureGateRefusesOnColdCluster(t *testing.T) {
	sink := &zoneRecorderSink{}
	d, _ := newWalDBZone(t, "", sink, false) // no announcements: cluster min stays 0

	err := proposePinnedRecorder(t, d, "pinned", "z-east")
	var lvl *cluster.ClusterBelowFeatureLevelError
	require.ErrorAs(t, err, &lvl, "a pin on a below-level cluster must be refused, got: %v", err)
	require.Equal(t, uint64(3), lvl.Required)
}

// TestZonePin_UnsatisfiableStrictStallAndCatchUp pins the strict-pin
// contract end to end: a pinned syncable whose zone loses its node STALLS —
// zero writes from anyone, loud status — and catches up COMPLETELY when a
// node announces the zone again. (The unserved pin is planted by proposing
// the raw config entity, bypassing admission — the post-admission
// membership-change path.)
func TestZonePin_UnsatisfiableStrictStallAndCatchUp(t *testing.T) {
	sink := &zoneRecorderSink{}
	d, s := newWalDBZone(t, "z-east", sink, true)
	// Plant a pin to a zone nobody serves (admission bypassed — this models
	// the only node in a zone being removed after admission).
	cfg := &cluster.Configuration{
		ID: "stalled", MimeType: "text/toml",
		Data: []byte("[syncable]\nname = \"stalled\"\ntype = \"recorder\"\nzone = \"z-west\"\n"),
	}
	e, err := cluster.NewUpsertSyncableEntity(cfg)
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{e}}))

	proposeTypeTOML(t, d, "photos", "photos", "", "")
	tp, err := s.ResolveType(cluster.LatestTypeRef("photos"))
	require.NoError(t, err)
	for _, k := range []string{"k1", "k2", "k3"} {
		require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{
			cluster.NewUpsertEntity(tp, []byte(k), []byte(`{"a":1}`)),
		}}))
	}

	// Strict stall: the pin resolves nobody — the leader must NOT serve.
	require.Eventually(t, func() bool {
		zone, unsatisfiable, ok := d.SyncableZonePin("stalled")
		return ok && zone == "z-west" && unsatisfiable
	}, 10*time.Second, 10*time.Millisecond, "the unsatisfiable pin never surfaced")
	require.Equal(t, uint64(0), d.SyncableOwner("stalled"), "nobody owns an unsatisfiable pin")
	require.Never(t, func() bool { return sink.count() > 0 },
		2*time.Second, 50*time.Millisecond, "STRICT pin: zero writes while unsatisfiable — no silent leader fallback")

	// Heal: this node announces z-west (models a node coming up in the
	// zone). The worker notices on its ownership poll and catches up fully.
	ze, err := cluster.NewNodeZoneEntity(1, "z-west")
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{ze}}))

	require.Eventually(t, func() bool { return sink.count() >= 3 },
		15*time.Second, 10*time.Millisecond, "the healed pin never caught up")
	zone, unsatisfiable, ok := d.SyncableZonePin("stalled")
	require.True(t, ok)
	require.Equal(t, "z-west", zone)
	require.False(t, unsatisfiable)
	require.Equal(t, uint64(1), d.SyncableOwner("stalled"))
}
