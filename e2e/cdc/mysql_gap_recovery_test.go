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

// TestMySQL_BinlogPurgeGapRecovery is the MySQL analog of the Postgres
// slot-recreate scenario: the source discards change data the ingestable never
// consumed, and committed must (a) DETECT it — status reports
// reSnapshotRequired instead of a lag number that understates an uncloseable
// gap — and (b) RECOVER through the documented path: delete + recreate the
// ingestable, whose fresh full snapshot re-reads the table (including the rows
// whose binlog events were destroyed) and epoch-sweeps the sink.
//
// The gap is staged the only way it can really happen: committed is STOPPED
// (nothing consuming), the source commits a row and then rotates + purges all
// binary logs (root-privileged PURGE BINARY LOGS), and committed restarts into
// a consumed-GTID position the source has partially discarded
// (@@gtid_purged ⊄ consumed — needsResnapshot).
func TestMySQL_BinlogPurgeGapRecovery(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(),
		harness.Options{Tables: []string{"region"}, Syncable: true})
	ctx := context.Background()

	// Phase 1: two rows stream through to the sink — the ingestable is in
	// Phase-B streaming with a non-empty consumed GTID set (the position the
	// purge must invalidate).
	pre := mutation.NewScript()
	pre.Insert("region", regionRow(1, "KEEP_A", "pre-gap-a"))
	pre.Insert("region", regionRow(2, "KEEP_B", "pre-gap-b"))
	require.NoError(t, h.RunScript(ctx, pre), "phase 1 inserts")
	h.WaitForSinkValue(t, "region", "1", "r_name", "KEEP_A", 30*time.Second)
	h.WaitForSinkValue(t, "region", "2", "r_name", "KEEP_B", 30*time.Second)

	// Phase 2: stop committed, commit a row nobody consumes, then destroy the
	// binlog history that carries it. Row 3 now exists ONLY in the table —
	// its change event is gone forever.
	h.StopCommitted()
	require.NoError(t, h.SourceTxn(ctx, func(q mutation.Querier) error {
		return q.Exec(ctx,
			"INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (?, ?, ?)",
			3, "PURGED_ROW", "committed-while-down")
	}), "insert during the outage")
	h.PurgeSourceBinlogs(t)

	// Phase 3: restart into the gap. Streaming can never resume (the ordinary
	// readiness gate would hang, hence NoWait); the status endpoint must
	// surface the distinct re-snapshot state.
	h.StartCommittedNoWait(t)
	h.WaitForIngestableReSnapshotRequired(t, "region", 60*time.Second)

	// Phase 4: the documented recovery — delete + recreate. The fresh full
	// snapshot reads the TABLE (rows 1, 2, and the purged row 3) and re-emits
	// everything at a bumped refresh epoch; the closing marker sweeps the sink.
	// Row 3 arriving proves the purged range's DATA was recovered even though
	// its change events were destroyed.
	h.DeleteIngestable(t, "region")
	h.RecreateIngestable(t, "region")
	h.WaitForSinkValue(t, "region", "3", "r_name", "PURGED_ROW", 60*time.Second)
	h.WaitForSinkValue(t, "region", "1", "r_name", "KEEP_A", 30*time.Second)
	h.WaitForSinkValue(t, "region", "2", "r_name", "KEEP_B", 30*time.Second)
	require.Equal(t, 3, h.SinkCount(t, "region"), "exactly the three live rows after recovery")
}
