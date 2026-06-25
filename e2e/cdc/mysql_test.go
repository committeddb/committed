//go:build docker

package cdc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
)

// TestMySQL_SyncableProjectsToSink is the MySQL counterpart of
// TestSyncableProjectsToSink: a row inserted into the MySQL CDC source flows
// source → ingestable (binlog) → topic → syncable → external MySQL sink table,
// and an update to the same PK upserts rather than duplicates. It exercises the
// MySQL ingest dialect and the MySQL syncable dialect (CreateDDL, CreateSQL's
// INSERT ... ON DUPLICATE KEY UPDATE, BindArgs) end-to-end through committed —
// the loop that mysql_test.go covers only at the dialect level.
func TestMySQL_SyncableProjectsToSink(t *testing.T) {
	h := harness.NewMySQL(t)

	h.Exec(t, "INSERT INTO widget (wid, name) VALUES (?, ?)", "w1", "ALPHA")
	h.WaitForSinkValue(t, "w1", "name", "ALPHA", 30*time.Second)

	// Upsert semantics: updating the same PK replaces, not duplicates.
	h.Exec(t, "UPDATE widget SET name = ? WHERE wid = ?", "ALPHA-2", "w1")
	h.WaitForSinkValue(t, "w1", "name", "ALPHA-2", 30*time.Second)
	require.Equal(t, 1, h.SinkCount(t), "upsert must not duplicate the row")
}

// TestMySQL_DeleteHonoredEndToEnd is the MySQL right-to-be-forgotten chain: a row
// is inserted then deleted on the MySQL source, and the test asserts the row
// reaches the sink after the insert and is gone after the delete. A source
// DELETE must ingest as a delete entity (tombstone), not an upsert of the old
// row, and the syncable must translate it into a DELETE on the sink.
func TestMySQL_DeleteHonoredEndToEnd(t *testing.T) {
	h := harness.NewMySQL(t)

	h.Exec(t, "INSERT INTO widget (wid, name) VALUES (?, ?)", "w7", "EPHEMERAL")
	h.WaitForSinkValue(t, "w7", "name", "EPHEMERAL", 30*time.Second)

	h.Exec(t, "DELETE FROM widget WHERE wid = ?", "w7")
	h.WaitForSinkAbsent(t, "w7", 30*time.Second)
	require.Equal(t, 0, h.SinkCount(t), "the sink row must be gone after the delete")
}

// TestMySQL_RestartResumeSyncable is the MySQL analogue of
// TestRestartResumeSyncable: a row inserted AFTER committed restarts must still
// reach the sink. That only happens if, on restart, the MySQL ingestable resumes
// from its persisted binlog position (rather than re-snapshotting) AND the
// syncable worker respawns and resumes from its persisted SyncableIndex. MySQL
// itself is untouched across the restart.
func TestMySQL_RestartResumeSyncable(t *testing.T) {
	h := harness.NewMySQL(t)

	// Phase 1: a row reaches the sink while the original process runs.
	h.Exec(t, "INSERT INTO widget (wid, name) VALUES (?, ?)", "w1", "BEFORE")
	h.WaitForSinkValue(t, "w1", "name", "BEFORE", 30*time.Second)

	h.RestartCommitted(t)

	// Phase 2: a NEW row inserted after the restart must reach the sink.
	h.Exec(t, "INSERT INTO widget (wid, name) VALUES (?, ?)", "w2", "AFTER")
	h.WaitForSinkValue(t, "w2", "name", "AFTER", 30*time.Second)

	// Both rows present, exactly once each: the resume neither lost phase-1 nor
	// double-wrote anything.
	require.Equal(t, 2, h.SinkCount(t),
		"sink should hold exactly the phase-1 and phase-2 rows after restart")
}
