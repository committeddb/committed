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

// TestRestartResume verifies committed survives a Postgres restart
// without losing or duplicating CDC events.
//
// SKIPPED FOR NOW: testcontainers-go's Stop+Start cycle assigns a new
// host port to the Postgres container, but committed's ingestable
// config has the original connection string baked in (it was POSTed
// at New() time). After restart, committed's reconnect loop bangs on
// the old port forever. To make this test real we need either:
//   (a) a fixed host-port mapping for the Postgres container (loses
//       parallel-test isolation but is fine since we run -p=1), or
//   (b) restarting Postgres inside the container with docker exec so
//       the port doesn't change, or
//   (c) re-POSTing the ingestable with the new connection string.
// All three are post-PR improvements.
func TestRestartResume(t *testing.T) {
	t.Skip("testcontainers Postgres restart reassigns host port; see comment for resolution paths")

	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	pre := mutation.NewScript()
	pre.Insert("region", regionRow(1, "BEFORE", "phase1"))
	require.NoError(t, pre.Run(context.Background(), h.Conn()), "phase 1 run")
	oracle.Assert(t, pre.Expected(), h.Capture(t, pre.ExpectedCounts()))

	ctx := context.Background()
	require.NoError(t, h.RestartPostgres(ctx), "restart postgres")

	post := mutation.NewScript()
	post.Insert("region", regionRow(2, "AFTER", "phase2"))
	require.NoError(t, post.Run(ctx, h.Conn()), "phase 2 run")
	oracle.Assert(t, post.Expected(), h.Capture(t, post.ExpectedCounts()))
}
