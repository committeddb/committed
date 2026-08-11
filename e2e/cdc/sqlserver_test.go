//go:build docker

package cdc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
)

// TestSQLServer_SyncableProjectsToSink is the customer pipeline end to end:
// a row inserted into the SQL Server source flows source → ingestable (Change
// Tracking poll) → topic → the POSTGRES syncable → an external Postgres sink
// table, and an update to the same PK upserts rather than duplicates. This is
// the loop the dialect's own docker suite covers only at the dialect level —
// here it runs through committed's raft log and syncable worker.
func TestSQLServer_SyncableProjectsToSink(t *testing.T) {
	h := harness.NewSQLServer(t)

	h.Exec(t, "INSERT INTO dbo.widget (wid, name) VALUES (@p1, @p2)", "w1", "ALPHA")
	h.WaitForSinkValue(t, "w1", "name", "ALPHA", 45*time.Second)

	// Upsert semantics: updating the same PK replaces, not duplicates.
	h.Exec(t, "UPDATE dbo.widget SET name = @p1 WHERE wid = @p2", "ALPHA-2", "w1")
	h.WaitForSinkValue(t, "w1", "name", "ALPHA-2", 45*time.Second)
	require.Equal(t, 1, h.SinkCount(t), "upsert must not duplicate the row")
}

// TestSQLServer_DeleteHonoredEndToEnd is the SQL Server right-to-be-forgotten
// chain: a row inserted then deleted at the source reaches the Postgres sink
// after the insert and is GONE after the delete. Change Tracking reports the
// delete PK-only; the dialect must tombstone it (never upsert a pre-image),
// and the syncable must translate the tombstone into a sink DELETE.
func TestSQLServer_DeleteHonoredEndToEnd(t *testing.T) {
	h := harness.NewSQLServer(t)

	h.Exec(t, "INSERT INTO dbo.widget (wid, name) VALUES (@p1, @p2)", "w7", "EPHEMERAL")
	h.WaitForSinkValue(t, "w7", "name", "EPHEMERAL", 45*time.Second)

	h.Exec(t, "DELETE FROM dbo.widget WHERE wid = @p1", "w7")
	h.WaitForSinkAbsent(t, "w7", 45*time.Second)
	require.Equal(t, 0, h.SinkCount(t), "the sink row must be gone after the delete")
}

// TestSQLServer_RestartResumeSyncable: a row inserted AFTER committed restarts
// must still reach the sink — which requires the sqlserver ingestable to
// resume from its persisted Change Tracking version (not re-snapshot) AND the
// syncable worker to respawn from its persisted SyncableIndex. The source and
// sink databases are untouched across the restart.
func TestSQLServer_RestartResumeSyncable(t *testing.T) {
	h := harness.NewSQLServer(t)

	// Phase 1: a row reaches the sink while the original process runs.
	h.Exec(t, "INSERT INTO dbo.widget (wid, name) VALUES (@p1, @p2)", "w1", "BEFORE")
	h.WaitForSinkValue(t, "w1", "name", "BEFORE", 45*time.Second)

	h.RestartCommitted(t)

	// Phase 2: a NEW row inserted after the restart must reach the sink.
	h.Exec(t, "INSERT INTO dbo.widget (wid, name) VALUES (@p1, @p2)", "w2", "AFTER")
	h.WaitForSinkValue(t, "w2", "name", "AFTER", 45*time.Second)

	// Both rows present exactly once: the resume neither lost phase 1 nor
	// double-wrote anything.
	require.Equal(t, 2, h.SinkCount(t),
		"sink should hold exactly the phase-1 and phase-2 rows after restart")
}
