//go:build docker

package cdc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// TestSyncableProjectsToSink is the baseline that the restart test builds on:
// a row inserted into the CDC source flows source → ingestable → topic →
// syncable → external sink table. It also exercises the postgres syncable
// dialect end-to-end (CreateDDL, CreateSQL, and the BindArgs upsert binding),
// which only became reachable once dbParser registered the dialect.
func TestSyncableProjectsToSink(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "AMERICA", "comment-1"))
	require.NoError(t, h.RunScript(context.Background(), s), "insert region row")

	// The value lands in region_sink with the same text encoding the topic
	// carries (pgoutput text-encodes everything, so r_regionkey is "1").
	h.WaitForSinkValue(t, "region", "1", "r_name", "AMERICA", 30*time.Second)

	// Upsert semantics: updating the same PK replaces, not duplicates.
	u := mutation.NewScript()
	u.Update("region", regionRow(1, "AMERICA-2", "comment-1b"))
	require.NoError(t, h.RunScript(context.Background(), u), "update region row")
	h.WaitForSinkValue(t, "region", "1", "r_name", "AMERICA-2", 30*time.Second)
	require.Equal(t, 1, h.SinkCount(t, "region"), "upsert must not duplicate the row")
}

// TestRestartResumeSyncable is the syncable analogue of TestRestartResume: it
// proves a configured syncable resumes projecting topic data to its external
// SQL sink AFTER committed restarts. This exercises, end-to-end, the three
// fixes that make a postgres syncable survive a restart:
//
//   - RestoreSyncableWorkers respawns the worker (the apply-path send does not
//     re-fire on restart because appliedIndex makes ApplyCommitted idempotent);
//   - the database sub-parser is registered before wal.Open, so loadDatabases
//     rebuilds the sink handle that ParseSyncable resolves on restore;
//   - the postgres dialect is registered and its BindArgs matches its
//     placeholder count, so the resumed worker's upserts actually execute.
//
// Without any one of these, phase-2 data never reaches the sink.
func TestRestartResumeSyncable(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})

	// Phase 1: a row reaches the sink while the original process runs.
	pre := mutation.NewScript()
	pre.Insert("region", regionRow(1, "BEFORE", "phase1"))
	require.NoError(t, h.RunScript(context.Background(), pre), "phase 1 insert")
	h.WaitForSinkValue(t, "region", "1", "r_name", "BEFORE", 30*time.Second)

	// Restart committed against the same data dir. RestartCommitted waits for
	// the ingestable slot to stream again; the syncable has no slot — its
	// resumption is what the phase-2 assertion below actually proves.
	h.RestartCommitted(t)

	// Phase 2: a NEW row inserted after the restart must reach the sink. The
	// only way it can is if the syncable worker respawned and resumed from its
	// persisted SyncableIndex.
	post := mutation.NewScript()
	post.Insert("region", regionRow(2, "AFTER", "phase2"))
	require.NoError(t, h.RunScript(context.Background(), post), "phase 2 insert")
	h.WaitForSinkValue(t, "region", "2", "r_name", "AFTER", 30*time.Second)

	// Both rows present, exactly once each: the resume neither lost phase-1
	// nor double-wrote anything (PK upsert would collapse a re-sync anyway).
	require.Equal(t, 2, h.SinkCount(t, "region"),
		"sink should hold exactly the phase-1 and phase-2 rows after restart")
}

// TestDeleteHonoredEndToEnd is the full right-to-be-forgotten chain through
// CDC: a row is inserted then deleted on the Postgres SOURCE, and the test
// asserts with TWO syncables that both halves landed:
//
//   - a webhook syncable (the harness collector) proves the ingestable emits a
//     proper op="upsert" then op="delete" for the same key — a source DELETE is
//     no longer ingested as an upsert of the old row;
//   - a Postgres SQL syncable (the sink) proves the delete is honored all the
//     way out: the row appears in region_sink after the insert, then is gone
//     after the delete.
//
// This is the e2e regression guard for the zombie: before the fix, the source
// DELETE rode through as an upsert and the projection kept the PII forever.
func TestDeleteHonoredEndToEnd(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})

	row := regionRow(7, "EPHEMERAL", "to-be-forgotten")

	// Phase 1: INSERT on the source. The webhook observes an upsert, and the
	// row reaches the SQL sink.
	ins := mutation.NewScript()
	ins.Insert("region", row)
	require.NoError(t, h.RunScript(context.Background(), ins), "insert region 7")

	add := h.Capture(t, map[string]int{"region": 1})["region"]
	require.Len(t, add, 1)
	require.Len(t, add[0].Entities, 1)
	require.Equal(t, "upsert", add[0].Entities[0].Op, "an INSERT must ingest as an upsert")
	require.Equal(t, "7", add[0].Entities[0].Key)

	h.WaitForSinkValue(t, "region", "7", "r_name", "EPHEMERAL", 30*time.Second)

	// Phase 2: DELETE the same row on the source. The webhook observes a delete
	// (not an upsert of the old row), and the sink row is removed.
	del := mutation.NewScript()
	del.Delete("region", row)
	require.NoError(t, h.RunScript(context.Background(), del), "delete region 7")

	gone := h.Capture(t, map[string]int{"region": 1})["region"]
	require.Len(t, gone, 1)
	require.Len(t, gone[0].Entities, 1)
	require.Equal(t, "delete", gone[0].Entities[0].Op, "a source DELETE must ingest as a delete entity")
	require.Equal(t, "7", gone[0].Entities[0].Key)
	require.Empty(t, gone[0].Entities[0].Data, "a delete carries no payload")

	h.WaitForSinkAbsent(t, "region", "7", 30*time.Second)
	require.Equal(t, 0, h.SinkCount(t, "region"), "the sink row must be gone after the delete")
}
