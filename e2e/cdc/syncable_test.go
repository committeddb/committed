//go:build docker

package cdc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/e2e/cdc/harness"
	"github.com/philborlin/committed/e2e/cdc/mutation"
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
	require.NoError(t, s.Run(context.Background(), h.Conn()), "insert region row")

	// The value lands in region_sink with the same text encoding the topic
	// carries (pgoutput text-encodes everything, so r_regionkey is "1").
	h.WaitForSinkValue(t, "region", "1", "r_name", "AMERICA", 30*time.Second)

	// Upsert semantics: updating the same PK replaces, not duplicates.
	u := mutation.NewScript()
	u.Update("region", regionRow(1, "AMERICA-2", "comment-1b"))
	require.NoError(t, u.Run(context.Background(), h.Conn()), "update region row")
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
	require.NoError(t, pre.Run(context.Background(), h.Conn()), "phase 1 insert")
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
	require.NoError(t, post.Run(context.Background(), h.Conn()), "phase 2 insert")
	h.WaitForSinkValue(t, "region", "2", "r_name", "AFTER", 30*time.Second)

	// Both rows present, exactly once each: the resume neither lost phase-1
	// nor double-wrote anything (PK upsert would collapse a re-sync anyway).
	require.Equal(t, 2, h.SinkCount(t, "region"),
		"sink should hold exactly the phase-1 and phase-2 rows after restart")
}
