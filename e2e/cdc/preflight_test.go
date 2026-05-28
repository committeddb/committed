//go:build docker

package cdc_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/e2e/cdc/dataset"
	"github.com/philborlin/committed/e2e/cdc/harness"
	"github.com/philborlin/committed/e2e/cdc/mutation"
	"github.com/philborlin/committed/e2e/cdc/oracle"
)

// TestPreflightWalLevel asserts Postgres is configured for logical
// replication. The harness sets `-c wal_level=logical` at startup;
// this test catches regressions where that flag is dropped or
// overridden.
func TestPreflightWalLevel(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})
	var level string
	err := h.Conn().QueryRow(context.Background(), "SHOW wal_level").Scan(&level)
	require.NoError(t, err, "SHOW wal_level")
	require.Equal(t, "logical", level, "wal_level must be logical for CDC")
}

// TestPreflightSlotAndPublication asserts that posting an ingestable
// causes Postgres to materialize a publication and a logical
// replication slot. Without these, no CDC events flow.
func TestPreflightSlotAndPublication(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	var slotCount int
	err := h.Conn().QueryRow(context.Background(),
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1",
		h.SlotName("region"),
	).Scan(&slotCount)
	require.NoError(t, err, "query pg_replication_slots")
	require.Equal(t, 1, slotCount, "ingestable did not create its replication slot")

	var pubCount int
	err = h.Conn().QueryRow(context.Background(),
		"SELECT count(*) FROM pg_publication WHERE pubname=$1",
		"pub_region",
	).Scan(&pubCount)
	require.NoError(t, err, "query pg_publication")
	require.Equal(t, 1, pubCount, "ingestable did not create its publication")
}

// TestPreflightReplicaIdentity asserts every TPC-H table has REPLICA
// IDENTITY FULL. Without FULL, UPDATE/DELETE events carry only the
// primary key in the old-tuple — the oracle's "full pre-image" oracle
// would silently fail on every delete test.
//
// relreplident values from pg_class: 'd' = default, 'n' = nothing,
// 'f' = full, 'i' = index.
func TestPreflightReplicaIdentity(t *testing.T) {
	h := harness.New(t)
	for _, table := range dataset.Tables {
		// relreplident is a Postgres "char" (one-byte) column; pgx
		// binary mode can't scan it into *string directly. Cast to
		// text in SQL and read back as text.
		var identity string
		err := h.Conn().QueryRow(context.Background(),
			"SELECT relreplident::text FROM pg_class WHERE relname=$1 AND relnamespace=(SELECT oid FROM pg_namespace WHERE nspname='public')",
			table,
		).Scan(&identity)
		require.NoError(t, err, "query pg_class for %s", table)
		require.Equal(t, "f", identity, "table %s must have REPLICA IDENTITY FULL", table)
	}
}

// TestRestartResume verifies that committed resumes ingestion from
// the persisted IngestablePosition after a restart — no re-snapshot,
// no replayed events, no gaps.
//
// Restarts committed (not Postgres) because that's the scenario
// position persistence actually serves: a committed process exits
// (k8s rolling restart, crash, deploy) and the next process picks up
// where the previous one left off. Postgres stays up so the slot's
// LSN cursor is preserved on the server side; the test asserts that
// committed's side also remembers, via the position bucket added in
// internal/cluster/db/wal/ingestable_position.go.
func TestRestartResume(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	// Phase 1: insert two rows, capture, verify.
	pre := mutation.NewScript()
	pre.Insert("region", regionRow(1, "BEFORE_A", "phase1-a"))
	pre.Insert("region", regionRow(2, "BEFORE_B", "phase1-b"))
	require.NoError(t, pre.Run(context.Background(), h.Conn()), "phase 1 run")
	oracle.Assert(t, pre.Expected(), h.Capture(t, pre.ExpectedCounts()))

	// Restart committed against the same data dir. Position persistence
	// means the resumed ingestable starts from where phase 1 left off
	// instead of re-snapshotting region (which would re-emit rows 1
	// and 2 and break the oracle below).
	h.RestartCommitted(t)

	// Phase 2: insert two more rows. The oracle expects exactly 2 new
	// proposals on region — proving phase-1 rows were NOT replayed.
	post := mutation.NewScript()
	post.Insert("region", regionRow(3, "AFTER_A", "phase2-a"))
	post.Insert("region", regionRow(4, "AFTER_B", "phase2-b"))
	require.NoError(t, post.Run(context.Background(), h.Conn()), "phase 2 run")
	oracle.Assert(t, post.Expected(), h.Capture(t, post.ExpectedCounts()))
}
