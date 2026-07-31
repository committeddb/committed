//go:build docker

package cdc_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
	"github.com/committeddb/committed/e2e/cdc/oracle"
)

// TestMultiTopicIngestableOneSlot drives the multi-topic demux end to end: ONE
// [[sql.topics]] ingestable produces two topics (region, nation) over a SINGLE
// replication slot. Interleaved inserts/updates/deletes across both tables must
// converge per topic — the oracle proves each topic's stream through the real path
// (POST [[sql.topics]] → propose → apply → per-topic webhook syncable) — and the
// whole ingestable must hold exactly one slot, which is the point of the feature.
func TestMultiTopicIngestableOneSlot(t *testing.T) {
	h := harness.New(t, harness.Options{
		Tables:           []string{"region", "nation"},
		SingleIngestable: true,
	})

	// region 100 is inserted before nation 200 (n_regionkey=100) so the FK holds;
	// each statement is its own commit (the mutation DSL), so the streams interleave
	// across the two topics on one slot.
	s := mutation.NewScript()
	s.Insert("region", regionRow(100, "R100", "r0"))
	s.Insert("nation", nationRow(200, "N200", 100, "n0"))
	s.Update("region", regionRow(100, "R100_v2", "r1"))
	s.Insert("region", regionRow(101, "R101", "r0"))
	s.Insert("nation", nationRow(201, "N201", 100, "n0"))
	s.Delete("nation", nationRow(200, "N200", 100, "n0")) // pre-image (REPLICA IDENTITY FULL)

	require.NoError(t, h.RunScript(context.Background(), s), "script run")

	// The oracle asserts per topic: region got [insert, update, insert] and nation
	// got [insert, insert, delete], each correctly keyed — proving the two tables
	// routed to their own topics through the whole cluster.
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))

	// Both topics are produced by ONE ingestable, so they share ONE slot, and that
	// slot exists exactly once — a whole-database feed on a single slot.
	slot := h.SlotName("region")
	require.Equal(t, slot, h.SlotName("nation"), "both topics resolve to one shared slot")
	var count int
	require.NoError(t, h.Conn().QueryRow(context.Background(),
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&count))
	require.Equal(t, 1, count, "a multi-topic ingestable holds exactly one replication slot")
}
