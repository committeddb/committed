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

// TestStagedProjectionEndToEnd drives the stage engine through a REAL
// committed node over the full path: Postgres CDC → ingestable → topic →
// internal stage (aggregate by a payload field) → stage-fed table source →
// sink. It proves the production wiring the unit tiers cannot: the stage
// store landing under the node's COMMITTED_DATA_DIR, the drain running
// inside the worker's Sync, rekeying through CDC updates, and retraction
// through CDC deletes.
func TestStagedProjectionEndToEnd(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}, Syncable: true})

	// A staged projection over the region topic: count regions per name.
	// (pgoutput text-encodes payloads, so counts arrive as "1"/"2" text.)
	toml := fmt.Sprintf(`[syncable]
name = "region-stats"
type = "projection"
mode = "always-current"

[projection]
db         = %q
table      = "region_stats"
primaryKey = "name"

[[projection.columns]]
name = "name"
type = "TEXT"

[[projection.columns]]
name = "n"
type = "INT"

[[projection.stage]]
name    = "by-name"
from    = "region"
keyPath = "$.r_name"
reduce  = "aggregate"
emit    = [ { field = "n", count = true } ]

[[projection.source]]
from = "by-name"
[[projection.source.rules]]
set = [ { column = "n", from = "$.n" } ]
`, harness.SinkDatabaseID())
	harness.PostSyncableTOML(t, "region-stats", toml)

	// Two regions share a name; one is alone.
	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "AMERICA", "c1"))
	s.Insert("region", regionRow(2, "AMERICA", "c2"))
	s.Insert("region", regionRow(3, "ASIA", "c3"))
	require.NoError(t, h.RunScript(context.Background(), s))

	h.WaitForRawSinkValue(t, "region_stats", "name", "AMERICA", "n", "2", 30*time.Second)
	h.WaitForRawSinkValue(t, "region_stats", "name", "ASIA", "n", "1", 30*time.Second)

	// A CDC update REKEYS region 2 through the stage: both counts move.
	u := mutation.NewScript()
	u.Update("region", regionRow(2, "ASIA", "c2"))
	require.NoError(t, h.RunScript(context.Background(), u))
	h.WaitForRawSinkValue(t, "region_stats", "name", "AMERICA", "n", "1", 30*time.Second)
	h.WaitForRawSinkValue(t, "region_stats", "name", "ASIA", "n", "2", 30*time.Second)

	// CDC deletes drain the AMERICA key entirely: its row RETRACTS.
	d := mutation.NewScript()
	d.Delete("region", regionRow(1, "AMERICA", "c1"))
	require.NoError(t, h.RunScript(context.Background(), d))
	h.WaitForRawSinkAbsent(t, "region_stats", "name", "AMERICA", 30*time.Second)
	h.WaitForRawSinkValue(t, "region_stats", "name", "ASIA", "n", "2", 30*time.Second)
}
