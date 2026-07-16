package db_test

import (
	"context"
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
	"github.com/committeddb/committed/internal/cluster/migration"
)

// okSyncable accepts everything. The always-current tests that drive a
// migration failure never need the inner syncable to act — the wrapper
// fails before it is reached.
type okSyncable struct{}

func (okSyncable) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	return false, nil
}
func (okSyncable) Close() error { return nil }

// seedPersonAndBreakMigration registers "person" v1, proposes one entity
// under it, and evolves the type to v2 with a migration program that fails
// at runtime on that entity. Returns the v2 schema (for in-place migration
// fixes, which require the schema unchanged).
func seedPersonAndBreakMigration(t *testing.T, d *db.DB, s *wal.Storage) string {
	t.Helper()
	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)

	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: v1,
		Key:  []byte("alice"),
		Data: []byte(`{"name":"alice"}`),
	}}}))

	v2Schema := `{"type":"object","required":["email"]}`
	proposeTypeWithMigration(t, d, "person", "Person", v2Schema,
		`error("cannot derive email for " + .name)`)
	return v2Schema
}

// deadLetterMigration runs an always-current syncable over the broken
// state seedPersonAndBreakMigration set up, waits for the worker to
// dead-letter the failing proposal against the type, and returns the
// failed proposal's raft index plus the v2 schema.
func deadLetterMigration(t *testing.T, d *db.DB, s *wal.Storage, syncID string, m *metrics.Metrics) (uint64, string) {
	t.Helper()
	v2Schema := seedPersonAndBreakMigration(t, d, s)

	wrapped := migration.Wrap(okSyncable{}, s, m)
	require.NoError(t, d.Sync(context.Background(), syncID, wrapped))

	var dls []cluster.TypeMigrationDeadLetter
	require.Eventually(t, func() bool {
		var err error
		dls, err = d.TypeMigrationDeadLetters("person", 0, 10)
		return err == nil && len(dls) == 1
	}, 10*time.Second, 10*time.Millisecond, "a runtime migration failure must produce exactly one type-keyed dead letter")
	return dls[0].Index, v2Schema
}

// TestSync_MigrationRuntimeError_DeadLettersTypeAndSyncable is the headline
// observability criterion: a jq program failing at runtime during an
// always-current sync (a) records a type-keyed dead letter naming the
// failing chain step, (b) records the usual syncable dead letter pointing
// at the same proposal, and (c) increments
// committed.type.migration.errors{type_id, from_version, to_version}.
func TestSync_MigrationRuntimeError_DeadLettersTypeAndSyncable(t *testing.T) {
	d, s, reader := newWalDBWithMetrics(t)
	const syncID = "ac-sync"
	// The syncable dead-letter persists only while the syncable config exists (the
	// type-migration dead-letter, keyed by type id, is unaffected — see the sibling
	// follow-up ticket).
	seedSyncableConfig(t, d, syncID)

	index, _ := deadLetterMigration(t, d, s, syncID, nil)

	dls, err := d.TypeMigrationDeadLetters("person", 0, 10)
	require.NoError(t, err)
	require.Len(t, dls, 1)
	require.Equal(t, "person", dls[0].TypeID)
	require.Equal(t, 1, dls[0].FromVersion, "the failing step starts at the stamped version")
	require.Equal(t, 2, dls[0].ToVersion, "the v2 program is the one that errored")
	// Redacted: this record is replicated into the permanent log and queryable
	// over HTTP, so it must carry only the classifier — never the jq runtime
	// error, which inlines entity field values (PII). Full detail stays in the
	// node's logs.
	require.NotContains(t, dls[0].Message, "alice",
		"the replicated record must not carry the jq error's inlined entity data")
	require.Contains(t, dls[0].Message, "transform failed",
		"the record carries the redacted classifier instead")
	require.Greater(t, dls[0].Index, uint64(0))
	require.NotZero(t, dls[0].TimestampUnixNano)

	// The syncable twin: same skipped proposal, recorded as permanent.
	var sdls []cluster.SyncableDeadLetter
	require.Eventually(t, func() bool {
		var err error
		sdls, err = d.SyncableDeadLetters(syncID, 0, 10)
		return err == nil && len(sdls) == 1
	}, 5*time.Second, 5*time.Millisecond, "the syncable that hit the failure must dead-letter the proposal too")
	require.Equal(t, "permanent", sdls[0].Kind)
	require.Equal(t, index, sdls[0].Index,
		"both records must point at the same skipped proposal")
	require.NotContains(t, sdls[0].Message, "alice",
		"the syncable twin is redacted too, via safeDeadLetterMessage")

	// The migration error counter incremented for the failing step.
	require.Eventually(t, func() bool {
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			return false
		}
		return migrationErrorCount(rm, "person", 1, 2) >= 1
	}, 5*time.Second, 5*time.Millisecond, "committed.type.migration.errors must increment")
}

// TestSync_MigrationSuccess_RecordsDuration proves a successful chain run
// lands in the committed.type.migration.duration histogram keyed by type.
func TestSync_MigrationSuccess_RecordsDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m := metrics.New(provider.Meter("test"))

	dir := t.TempDir()
	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil,
		db.WithTickInterval(testTickInterval), db.WithMetrics(m))
	t.Cleanup(func() { _ = d.Close() })

	proposeTypeTOML(t, d, "person", "Person", `{"type":"object"}`, "")
	v1, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.NoError(t, err)
	require.NoError(t, d.Propose(testCtx(t), &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: v1, Key: []byte("alice"), Data: []byte(`{"name":"alice"}`),
	}}}))
	proposeTypeWithMigration(t, d, "person", "Person",
		`{"type":"object","required":["email"]}`,
		`. + {email: "unknown@example.com"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cap := &captureSyncable{doneAfter: 1, cancel: cancel}
	require.NoError(t, d.Sync(ctx, "ac-sync", migration.Wrap(cap, s, m)))
	<-ctx.Done()
	require.Len(t, cap.proposals(), 1, "the migrated proposal must reach the inner syncable")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.GreaterOrEqual(t, migrationDurationCount(rm, "person"), uint64(1),
		"committed.type.migration.duration must record the successful chain run")
}

// TestReplayTypeMigrationDeadLetter_SuccessAfterFix is the recovery flow:
// fix the program (schema unchanged, so the migration updates in place),
// retry the dead-lettered proposal, and the record clears.
func TestReplayTypeMigrationDeadLetter_SuccessAfterFix(t *testing.T) {
	d, s := newWalDB(t)
	index, v2Schema := deadLetterMigration(t, d, s, "ac-sync", nil)

	proposeTypeWithMigration(t, d, "person", "Person", v2Schema,
		`. + {email: "unknown@example.com"}`)

	require.NoError(t, d.ReplayTypeMigrationDeadLetter(testCtx(t), "person", index))

	dls, err := d.TypeMigrationDeadLetters("person", 0, 10)
	require.NoError(t, err)
	require.Empty(t, dls, "a successful retry must clear the record")

	// A second retry of the same index is now a 404-shaped error.
	require.ErrorIs(t, d.ReplayTypeMigrationDeadLetter(testCtx(t), "person", index),
		cluster.ErrNotDeadLettered)
}

// TestReplayTypeMigrationDeadLetter_StillFailing proves a retry without a
// fix reports the cause and leaves the record in place.
func TestReplayTypeMigrationDeadLetter_StillFailing(t *testing.T) {
	d, s := newWalDB(t)
	index, _ := deadLetterMigration(t, d, s, "ac-sync", nil)

	err := d.ReplayTypeMigrationDeadLetter(testCtx(t), "person", index)
	require.ErrorIs(t, err, cluster.ErrReplayMigrationFailed)
	require.Contains(t, err.Error(), "cannot derive email for alice")

	dls, listErr := d.TypeMigrationDeadLetters("person", 0, 10)
	require.NoError(t, listErr)
	require.Len(t, dls, 1, "a failed retry must leave the record in place")
}

// TestReplayTypeMigrationDeadLetter_NotDeadLettered: an index that was
// never recorded is rejected without touching the log.
func TestReplayTypeMigrationDeadLetter_NotDeadLettered(t *testing.T) {
	d, _ := newWalDB(t)

	require.ErrorIs(t, d.ReplayTypeMigrationDeadLetter(testCtx(t), "person", 999),
		cluster.ErrNotDeadLettered)
}

// migrationErrorCount returns the committed.type.migration.errors counter
// value for a (type_id, from_version, to_version) triple, or 0 if absent.
func migrationErrorCount(rm metricdata.ResourceMetrics, typeID string, from, to int64) int64 {
	m := findMetric(rm, "committed.type.migration.errors")
	if m == nil {
		return 0
	}
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		return 0
	}
	for _, dp := range sum.DataPoints {
		var gotID string
		var gotFrom, gotTo int64
		for _, attr := range dp.Attributes.ToSlice() {
			switch string(attr.Key) {
			case "type_id":
				gotID = attr.Value.AsString()
			case "from_version":
				gotFrom = attr.Value.AsInt64()
			case "to_version":
				gotTo = attr.Value.AsInt64()
			}
		}
		if gotID == typeID && gotFrom == from && gotTo == to {
			return dp.Value
		}
	}
	return 0
}

// migrationDurationCount returns the committed.type.migration.duration
// histogram count for a type_id, or 0 if absent.
func migrationDurationCount(rm metricdata.ResourceMetrics, typeID string) uint64 {
	m := findMetric(rm, "committed.type.migration.duration")
	if m == nil {
		return 0
	}
	h, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		return 0
	}
	for _, dp := range h.DataPoints {
		for _, attr := range dp.Attributes.ToSlice() {
			if string(attr.Key) == "type_id" && attr.Value.AsString() == typeID {
				return dp.Count
			}
		}
	}
	return 0
}
