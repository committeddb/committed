package db_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// seedUserProposals registers a schema-less type and proposes one user
// entity per payload against a real wal-backed DB. A real wal.Storage
// (unlike the in-memory test double, which stubs ResolveType) refuses to
// apply an entity whose type isn't registered, so the type must exist
// before the data is proposed. The type registration is itself a system
// proposal, which the test syncables skip (see allSystem).
func seedUserProposals(t *testing.T, d *db.DB, s *wal.Storage, typeID string, payloads []string) {
	t.Helper()
	proposeTypeTOML(t, d, typeID, typeID, "", "")
	typ, err := s.ResolveType(cluster.LatestTypeRef(typeID))
	require.NoError(t, err)
	for i, payload := range payloads {
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: typ,
			Key:  fmt.Appendf(nil, "k%d", i),
			Data: []byte(payload),
		}}}
		require.NoError(t, d.Propose(testCtx(t), p))
	}
}

// allSystem reports whether every entity in p is a system entity (type,
// syncable, database, syncable-index). The sync worker delivers system
// proposals to syncables alongside user data; tests that care only about
// user data skip them — and crucially do NOT bump the index for them, so
// the persisted index tracks user proposals exactly.
func allSystem(a *cluster.Actual) bool {
	for _, e := range a.Entities {
		if !cluster.IsSystem(e.ID) {
			return false
		}
	}
	return true
}

// probeSyncable records the persisted SyncableIndex observed at the
// START of each user Sync call. With the bump made blocking, the bump
// for user proposal k-1 must be durable before Sync(user proposal k)
// runs — so the recorded values prove the lock-step durability
// guarantee. System proposals are skipped (not recorded, not bumped).
type probeSyncable struct {
	s  *wal.Storage
	id string

	mu              sync.Mutex
	persistedAtSync []uint64
	count           int

	doneAt int
	cancel func()
}

func (p *probeSyncable) Sync(ctx context.Context, prop *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(prop) {
		return cluster.ShouldSnapshot(false), nil
	}

	idx, err := p.s.GetSyncableIndex(p.id)
	if err != nil {
		idx = 0
	}

	p.mu.Lock()
	p.persistedAtSync = append(p.persistedAtSync, idx)
	p.count++
	done := p.count == p.doneAt
	p.mu.Unlock()

	if done && p.cancel != nil {
		p.cancel()
	}
	return cluster.ShouldSnapshot(true), nil
}

func (p *probeSyncable) Close() error { return nil }

func (p *probeSyncable) observed() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]uint64, len(p.persistedAtSync))
	copy(out, p.persistedAtSync)
	return out
}

// TestSyncBump_DurableBeforeNextSync proves the core invariant the
// durable-bump change buys: after a successful Sync, the index bump is
// durably applied before the worker's next iteration begins. The probe
// reads the persisted SyncableIndex at the start of every user Sync;
// with the blocking bump, the value seen at user-sync k is exactly the
// index bumped after user-sync k-1 — zero before the first one, then
// strictly increasing and non-zero. With the old fire-and-forget bump
// the value could still be 0 (the bump hadn't applied yet), so this
// assertion is what the fix makes hold.
func TestSyncBump_DurableBeforeNextSync(t *testing.T) {
	d, s := newWalDB(t)
	id := "durable-sync"

	const n = 5
	payloads := make([]string, n)
	for i := range payloads {
		payloads[i] = fmt.Sprintf("v%d", i)
	}
	seedUserProposals(t, d, s, "evt", payloads)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := &probeSyncable{s: s, id: id, doneAt: n, cancel: cancel}
	require.NoError(t, d.Sync(context.Background(), id, probe))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never consumed all proposals")
	}

	got := probe.observed()
	require.GreaterOrEqual(t, len(got), n, "probe should have observed one persisted index per user sync")

	require.Equal(t, uint64(0), got[0], "no user bump has happened before the first user sync")
	for k := 1; k < n; k++ {
		require.Greater(t, got[k], uint64(0),
			"the bump for user proposal %d must be durable before user sync %d runs", k-1, k)
		require.Greater(t, got[k], got[k-1],
			"the persisted index must advance between user syncs (sync %d saw %d, sync %d saw %d)",
			k-1, got[k-1], k, got[k])
	}
}

// recordingSyncable records the payload of every USER proposal it syncs
// and asks for a snapshot (index bump) per a caller-supplied predicate
// over the user-proposal count. Returning snapshot=false for a given
// call models "this proposal was delivered downstream but its bump never
// landed" — exactly the state a crash between Sync and the bump leaves
// behind. System proposals are skipped (not recorded, not counted, not
// bumped).
type recordingSyncable struct {
	mu     sync.Mutex
	synced []string

	snapshotFor func(count int) bool
	doneAt      int
	cancel      func()
}

func (s *recordingSyncable) Sync(ctx context.Context, p *cluster.Actual) (cluster.ShouldSnapshot, error) {
	if allSystem(p) {
		return cluster.ShouldSnapshot(false), nil
	}

	s.mu.Lock()
	s.synced = append(s.synced, string(p.Entities[0].Data))
	count := len(s.synced)
	snap := s.snapshotFor(count)
	done := count == s.doneAt
	s.mu.Unlock()

	if done && s.cancel != nil {
		s.cancel()
	}
	return cluster.ShouldSnapshot(snap), nil
}

func (s *recordingSyncable) Close() error { return nil }

func (s *recordingSyncable) syncedPayloads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.synced))
	copy(out, s.synced)
	return out
}

// TestSyncBump_RecoveryReSyncsOnlyUnbumpedTail proves the headline
// success criterion: a crash between a Sync completing and its bump
// landing produces at most one duplicate on recovery.
//
// The first worker syncs three proposals; the bumps for the first two
// are durable (blocking), but the third returns snapshot=false — modeling
// a crash right after Sync(p2) but before its bump persisted. A fresh
// worker (the in-process stand-in for a restarted node: db.Sync builds a
// new Reader from the persisted SyncableIndex) then resumes. It must
// re-sync ONLY p2, not the whole run.
func TestSyncBump_RecoveryReSyncsOnlyUnbumpedTail(t *testing.T) {
	d, s := newWalDB(t)
	id := "recovery-sync"

	seedUserProposals(t, d, s, "evt", []string{"v0", "v1", "v2"})

	// Worker 1: bump for the first two user syncs (durable), but NOT for
	// the third — leaving p2 delivered-but-not-recorded, the crash state.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	worker1 := &recordingSyncable{
		snapshotFor: func(count int) bool { return count < 3 },
		doneAt:      3,
		cancel:      cancel1,
	}
	require.NoError(t, d.Sync(context.Background(), id, worker1))

	select {
	case <-ctx1.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("first worker never consumed all proposals")
	}
	require.Equal(t, []string{"v0", "v1", "v2"}, worker1.syncedPayloads())

	// Worker 2: the recovery worker. db.Sync replaces worker 1 and builds
	// a fresh Reader starting from the persisted SyncableIndex, which only
	// advanced through p1. It must re-deliver exactly p2.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	worker2 := &recordingSyncable{
		snapshotFor: func(count int) bool { return true },
		doneAt:      1,
		cancel:      cancel2,
	}
	require.NoError(t, d.Sync(context.Background(), id, worker2))

	select {
	case <-ctx2.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("recovery worker never re-synced the un-bumped proposal")
	}

	require.Equal(t, []string{"v2"}, worker2.syncedPayloads(),
		"recovery should re-sync only the single un-bumped proposal, not the whole run")
}

// TestSyncBump_EmitsDurationMetric verifies the new
// committed.sync.bump.duration histogram records one observation per
// durable bump. MemoryStorage is fine here — the metric is recorded
// regardless of storage backend, and its ResolveType stub lets us reuse
// the createProposals helper without registering a type.
func TestSyncBump_EmitsDurationMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { provider.Shutdown(context.Background()) })

	m := metrics.New(provider.Meter("test"))
	s := NewMemoryStorage()
	id := uint64(1)
	peers := db.Peers{id: ""}

	d := db.New(id, peers, s, parser.New(), nil, nil,
		db.WithTickInterval(testTickInterval),
		db.WithMetrics(m),
	)
	defer d.Close()

	const n = 3
	inputs := make([][]string, n)
	for i := range inputs {
		inputs[i] = []string{fmt.Sprintf("v%d", i)}
	}
	ps := createProposals(inputs)
	for _, p := range ps {
		require.NoError(t, d.Propose(testCtx(t), p))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncable := NewSyncable(n, cancel)
	require.NoError(t, d.Sync(context.Background(), "metric-sync", syncable))

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("syncable never consumed all proposals")
	}

	// The last bump may still be in flight when the syncable's cancel
	// fires, so poll until every bump has recorded.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		bump := findMetric(rm, "committed.sync.bump.duration")
		if bump == nil {
			return false
		}
		hist, ok := bump.Data.(metricdata.Histogram[float64])
		if !ok || len(hist.DataPoints) != 1 {
			return false
		}
		return hist.DataPoints[0].Count == uint64(n)
	}, 5*time.Second, 5*time.Millisecond, "expected %d sync bump observations", n)
}
