//go:build docker

package cdc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// tripwireEventCount counts rows in the schema-changes events sink table; 0
// while the syncable hasn't created it yet.
func tripwireEventCount(t *testing.T, h *harness.Harness) int {
	t.Helper()
	var n int
	if err := h.Conn().QueryRow(context.Background(),
		`SELECT count(*) FROM schema_changes_sink`).Scan(&n); err != nil {
		return 0
	}
	return n
}

func waitForTripwireEvents(t *testing.T, h *harness.Harness, want int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return tripwireEventCount(t, h) == want },
		30*time.Second, 200*time.Millisecond, "%s: want %d events, have %d", msg, want, tripwireEventCount(t, h))
}

// TestValidationTripwireAnnouncesToSQLSink is the ticket's e2e: an
// announce-typed CDC topic whose reality diverges from its blessed contract
// announces a ContractExtension event that lands in a SQL sink table via a
// PLAIN sql syncable — while ingest never pauses (every divergent row still
// reaches its own sink). One event per distinct divergent shape, the dedupe
// surviving a full committed restart.
func TestValidationTripwireAnnouncesToSQLSink(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})
	ctx := context.Background()

	// The events topic (declare it first — announce admission requires it),
	// keyed snapshot so at-least-once duplicates converge.
	h.PostConfig(t, "/v1/type/schema-changes", "[type]\nname = \"SchemaChanges\"\nentityKind = \"snapshot\"\n")

	// Bless a contract on the already-flowing region topic that never heard
	// of r_comment: every real row diverges on the undeclared field — the
	// SmugMug case, where reality carries a field the blessed contract does
	// not know, and committed says so instead of gating.
	h.PostConfig(t, "/v1/type/region", fmt.Sprintf(`[type]
name = "region"
schemaType = "JSONSchema"
schema = '%s'
validate = 2
schemaChangeTopic = "schema-changes"

[migration]
none = true
`, `{"type":"object","properties":{"r_regionkey":{"type":"number"},"r_name":{"type":"string"}},"additionalProperties":false}`))

	// A plain sql syncable delivers the events topic to a warehouse-style
	// sink table, keyed by the shape fingerprint.
	h.PostConfig(t, "/v1/syncable/schema-changes", fmt.Sprintf(`[syncable]
name = "schema-changes"
type = "sql"

[sql]
topic = "schema-changes"
db = %q
table = "schema_changes_sink"
primaryKey = "fingerprint"

[[sql.mappings]]
jsonPath = "$.fingerprint"
column = "fingerprint"
type = "TEXT"

[[sql.mappings]]
jsonPath = "$.typeID"
column = "type_id"
type = "TEXT"

[[sql.mappings]]
jsonPath = "$"
column = "payload"
type = "TEXT"
`, h.SinkDatabaseID()))

	// A running worker stamps the type resolved when it was built; restart so
	// the region worker picks up the announce-typed v2 contract.
	h.RestartCommitted(t)

	// Two rows of one divergent shape (all-strings payload vs the number
	// contract): both must reach the region sink — the tripwire never pauses
	// ingest — and announce exactly ONE event.
	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "AMERICA", "keep-1"))
	s.Insert("region", regionRow(2, "EUROPE", "keep-2"))
	require.NoError(t, h.RunScript(ctx, s), "divergent rows must ingest normally")
	h.WaitForSinkValue(t, "region", "1", "r_name", "AMERICA", 30*time.Second)
	h.WaitForSinkValue(t, "region", "2", "r_name", "EUROPE", 30*time.Second)
	waitForTripwireEvents(t, h, 1, "one distinct divergent shape announces once")

	var typeID string
	require.NoError(t, h.Conn().QueryRow(ctx,
		`SELECT type_id FROM schema_changes_sink`).Scan(&typeID))
	require.Equal(t, "region", typeID)

	// Restart, then more rows of the SAME shape: the dedupe mark is
	// replicated state, so nothing re-announces.
	h.RestartCommitted(t)
	s = mutation.NewScript()
	s.Insert("region", regionRow(3, "ASIA", "keep-3"))
	require.NoError(t, h.RunScript(ctx, s))
	h.WaitForSinkValue(t, "region", "3", "r_name", "ASIA", 30*time.Second)
	waitForTripwireEvents(t, h, 1, "an announced shape stays announced across a restart")

	// A row with a NULL comment is a DIFFERENT shape (null vs string) — a
	// second event, with its own fingerprint.
	s = mutation.NewScript()
	s.Insert("region", map[string]any{"r_regionkey": 4, "r_name": "AFRICA", "r_comment": nil})
	require.NoError(t, h.RunScript(ctx, s))
	h.WaitForSinkValue(t, "region", "4", "r_name", "AFRICA", 30*time.Second)
	waitForTripwireEvents(t, h, 2, "a new divergent shape announces its own event")

	var distinct int
	require.NoError(t, h.Conn().QueryRow(ctx,
		`SELECT count(DISTINCT fingerprint) FROM schema_changes_sink`).Scan(&distinct))
	require.Equal(t, 2, distinct, "each shape carries its own fingerprint")
}
