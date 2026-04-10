package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/stretchr/testify/require"
)

// TestApplyDeterminism is the load-bearing regression test for the
// determinism contract documented in
// docs/event-log-architecture.md § "Determinism requirement". Three
// fresh wal.Storage instances are constructed on disjoint temp dirs,
// the SAME raft entries are applied to each in the same order, and
// every bbolt bucket's contents are required to match exactly across
// all three nodes.
//
// This is the test the determinism-audit ticket calls for. The "build a
// 3-node raft cluster" framing in the ticket is satisfied here without
// real raft: raft's only contract is "every node sees the same committed
// entries in the same order", and that's exactly what this test simulates.
// Bypassing raft makes the test fast (a unit test, not an integration
// test) and deterministic (no election timing or HTTP transport flakiness),
// while still asserting the actual invariant: given the same input,
// every node's apply produces byte-identical bucket state.
//
// The propose burst is varied on purpose:
//   - Multiple types (covers handleType)
//   - A database with a parsed configuration (covers handleDatabase)
//   - A syncable (covers handleSyncable + the channel notify path)
//   - An ingestable (covers handleIngestable + the channel notify path)
//   - User-defined entities, both stamped and unstamped (covers
//     handleUserDefined and the no-fallback contract)
//   - Mixed proposals carrying multiple entities at once
//   - A delete (covers the delete branch in handleType)
//
// Bucket comparison uses BucketSnapshot (defined in wal/export_test.go),
// which walks every bucket in sorted name order and every key in sorted
// order, formatting each entry as a "bucket/hex=hex" line. testify's
// require.Equal on the resulting slices produces a per-line diff on
// failure, which is much more useful than a hash mismatch.
//
// Time-series storage is NOT in BucketSnapshot because tstorage's
// on-disk format is not byte-stable today (out-of-scope per the
// ticket). User-defined entities still flow through handleUserDefined
// and write to the time-series store; we cover that path with a
// separate cross-node assertion against the raw tstorage points,
// because the original known non-determinism (the time.Now() fallback)
// lived in handleUserDefined and would otherwise go undetected.
//
// If anyone reintroduces a source of non-determinism in the apply path —
// time.Now() at apply, map iteration order, random IDs, etc. — this test
// will fail loudly with a per-line diff across nodes.
func TestApplyDeterminism(t *testing.T) {
	const nodes = 3

	// Each node needs its own parser instance with its own fakes — the
	// parser registry is mutated by AddDatabaseParser/etc, so sharing
	// would couple the nodes. Each fake-parser returns a fresh fake
	// object per call, but the bucket-stored bytes (Configuration
	// proto) are independent of those fakes, so the hash still matches.
	storages := make([]*StorageWrapper, nodes)
	for i := 0; i < nodes; i++ {
		p := parser.New()

		fakeDBParser := &clusterfakes.FakeDatabaseParser{}
		fakeDBParser.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
		p.AddDatabaseParser("sql", fakeDBParser)

		fakeSyncParser := &clusterfakes.FakeSyncableParser{}
		fakeSyncParser.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
		p.AddSyncableParser("sql", fakeSyncParser)

		fakeIngestParser := &clusterfakes.FakeIngestableParser{}
		fakeIngestParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
		p.AddIngestableParser("kafka", fakeIngestParser)

		// Buffered channels so saveSyncable / saveIngestable's notify
		// step doesn't deadlock during apply. Capacity matches the
		// number of syncable + ingestable entities in the burst below.
		syncCh := make(chan *db.SyncableWithID, 4)
		ingestCh := make(chan *db.IngestableWithID, 4)

		s := OpenStorage(t, t.TempDir(), p, syncCh, ingestCh)
		storages[i] = s
		t.Cleanup(s.Cleanup)
	}

	// Build the propose burst once. Every node applies exactly the
	// same []pb.Entry slice, mirroring what raft would deliver.
	entries := buildVariedBurst(t)

	// Apply on every node in the same order.
	for _, s := range storages {
		saveAndApply(t, s, entries)
	}

	// Sanity-check: applied indexes should all match (they trace the
	// last entry index that ApplyCommitted bumped past).
	wantApplied := entries[len(entries)-1].Index
	for i, s := range storages {
		require.Equalf(t, wantApplied, s.AppliedIndex(),
			"node %d appliedIndex mismatch", i)
	}

	// Snapshot every node's raft entry log and compare side-by-side.
	// The ticket calls for hashing each node's "permanent event log
	// directory contents (or raft entry log today, until
	// permanent-event-log.md lands)". The entry log is what raft
	// replicates and what the future permanent log will be derived
	// from, so checking it locks in determinism on both sides of the
	// permanent-log split.
	//
	// Today this is mostly a check on raft itself (raft's contract is
	// "every node sees the same committed entries in the same order"),
	// but it's still load-bearing for the test as a whole: a Save bug
	// that drops or reorders entries on one node would diverge here
	// even though the bbolt walk and the tstorage walk look fine.
	entryLogs := make([][]pb.Entry, nodes)
	for i, s := range storages {
		entryLogs[i] = s.ents(t)
	}
	for i := 1; i < nodes; i++ {
		require.Equalf(t, entryLogs[0], entryLogs[i],
			"raft entry log differs across nodes: node %d vs node 0", i)
	}

	// Snapshot every node's bucket contents and compare side-by-side.
	// require.Equal prints a per-line diff on failure, so a divergence
	// shows up as the actual differing key/value rather than two opaque
	// hashes. The snapshot is built by BucketSnapshot
	// (wal/export_test.go), which walks bbolt in sorted bucket-name
	// order with each bucket's keys walked lexicographically.
	snapshots := make([][]string, nodes)
	for i, s := range storages {
		snap, err := s.BucketSnapshot()
		require.NoErrorf(t, err, "node %d BucketSnapshot", i)
		snapshots[i] = snap
	}
	for i := 1; i < nodes; i++ {
		require.Equalf(t, snapshots[0], snapshots[i],
			"apply path is non-deterministic: node %d bbolt buckets differ from node 0", i)
	}

	// BucketSnapshot deliberately excludes the time-series store (its
	// on-disk format is not byte-stable today). The time-series store IS
	// where the original known non-determinism lived (handleUserDefined's
	// old time.Now() fallback), so we make a separate cross-node
	// assertion by pulling the raw points out of tstorage and comparing
	// every timestamp. If anyone reintroduces a wall-clock read in
	// handleUserDefined, this assertion will fail because the three
	// node-applies' timestamps will differ — back-to-back applies still
	// tick the millisecond.
	const userMetric = "user-events"
	// Cover everything from unix epoch through year 9999 (no point in
	// being clever — this only matters as a query, not as storage).
	const tsLo = int64(0)
	const tsHi = int64(253402300799000) // 9999-12-31 in ms
	var firstPoints []*timeseriesPoint
	for i, s := range storages {
		points := selectAllPoints(t, s, userMetric, tsLo, tsHi)
		if i == 0 {
			firstPoints = points
			continue
		}
		require.Equalf(t, len(firstPoints), len(points),
			"node %d time-series point count mismatch (got %d, want %d)",
			i, len(points), len(firstPoints))
		for j, p := range points {
			require.Equalf(t, firstPoints[j].timestamp, p.timestamp,
				"node %d time-series point %d timestamp mismatch — apply non-deterministic in handleUserDefined?",
				i, j)
			require.Equalf(t, firstPoints[j].value, p.value,
				"node %d time-series point %d value mismatch", i, j)
		}
	}
}

type timeseriesPoint struct {
	timestamp int64
	value     float64
}

// selectAllPoints reads every point in [lo, hi] for the given metric and
// returns them in tstorage's natural order. tstorage.Select returns
// `ErrNoDataPoints` for an empty range, which we normalize to an empty
// slice so callers can compare lengths uniformly.
func selectAllPoints(t *testing.T, s *StorageWrapper, metric string, lo, hi int64) []*timeseriesPoint {
	t.Helper()
	raw, err := s.TimeSeriesStorage.Select(metric, nil, lo, hi)
	if err != nil && err.Error() != "no data points found" {
		t.Fatalf("tstorage.Select: %v", err)
	}
	out := make([]*timeseriesPoint, 0, len(raw))
	for _, p := range raw {
		out = append(out, &timeseriesPoint{timestamp: p.Timestamp, value: p.Value})
	}
	return out
}

// buildVariedBurst constructs a representative slice of raft entries
// covering every entity handler in applyEntity. Index/term values are
// monotonic so the entries form a valid raft log prefix.
func buildVariedBurst(t *testing.T) []pb.Entry {
	t.Helper()

	// Two types so handleType runs more than once. Both go in the
	// "types" bucket.
	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "user-events", Name: "UserEvents", Version: 1})
	require.NoError(t, err)
	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "audit-events", Name: "AuditEvents", Version: 1})
	require.NoError(t, err)
	// A third type that we'll later delete, exercising the delete branch
	// of handleType.
	t3, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "tmp-type", Name: "Tmp", Version: 1})
	require.NoError(t, err)

	// A database (parsed via the registered "sql" fake parser).
	dbCfg := &cluster.Configuration{
		ID:       "primary-db",
		MimeType: "application/json",
		Data:     []byte(`{"database":{"name":"primary","type":"sql"}}`),
	}
	dbEnt, err := cluster.NewUpsertDatabaseEntity(dbCfg)
	require.NoError(t, err)

	// A syncable and an ingestable (parsed via fakes).
	syncCfg := &cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable":{"name":"s1","type":"sql"}}`),
	}
	syncEnt, err := cluster.NewUpsertSyncableEntity(syncCfg)
	require.NoError(t, err)

	ingestCfg := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"i1","type":"kafka"}}`),
	}
	ingestEnt, err := cluster.NewUpsertIngestableEntity(ingestCfg)
	require.NoError(t, err)

	// A SyncableIndex bump (covers handleSyncableIndex).
	siEnt, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: "sync-1", Index: 5})
	require.NoError(t, err)

	// User-defined entities. One stamped (the common case), one
	// unstamped (Timestamp == 0) to lock in the no-fallback contract.
	userType := &cluster.Type{ID: "user-events"}
	userStamped := &cluster.Entity{
		Type:      userType,
		Key:       []byte("alice"),
		Data:      []byte(`{"action":"login"}`),
		Timestamp: 1700000000000, // fixed wall-clock; deterministic
	}
	userUnstamped := &cluster.Entity{
		Type: userType,
		Key:  []byte("bob"),
		Data: []byte(`{"action":"logout"}`),
		// Timestamp deliberately 0 — verifies the no-fallback path.
	}

	// A delete-type entity, exercising the delete branch of handleType.
	delTmp := cluster.NewDeleteTypeEntity("tmp-type")

	// Compose proposals. Some single-entity, one multi-entity, in a
	// shape that exercises the per-entity loop in ApplyCommitted.
	idx := uint64(1)
	term := uint64(1)
	entries := []pb.Entry{
		makeEntry(t, idx, t1),
		makeEntry(t, idx+1, t2),
		makeEntry(t, idx+2, t3),
		makeEntry(t, idx+3, dbEnt),
		// Multi-entity proposal: type+user mixed
		mustMakeProposal(t, idx+4, dbEnt, userStamped),
		makeEntry(t, idx+5, syncEnt),
		makeEntry(t, idx+6, ingestEnt),
		makeEntry(t, idx+7, userStamped),
		makeEntry(t, idx+8, userUnstamped),
		makeEntry(t, idx+9, siEnt),
		makeEntry(t, idx+10, delTmp),
	}
	for i := range entries {
		entries[i].Term = term
	}
	return entries
}

// mustMakeProposal builds a single pb.Entry containing a proposal of
// multiple entities. Mirrors makeEntry but accepts more than one entity
// in the same proposal.
func mustMakeProposal(t *testing.T, idx uint64, entities ...*cluster.Entity) pb.Entry {
	t.Helper()
	p := &cluster.Proposal{Entities: entities}
	bs, err := p.Marshal()
	require.NoError(t, err)
	return pb.Entry{Term: 1, Index: idx, Type: pb.EntryNormal, Data: bs}
}

