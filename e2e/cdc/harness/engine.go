//go:build docker

package harness

import (
	"context"
	"testing"
	"time"

	"github.com/committeddb/committed/e2e/cdc/dataset"
	"github.com/committeddb/committed/e2e/cdc/mutation"
)

// Engine abstracts the source-database-specific behavior of the CDC harness so
// the same scenario/oracle flow can eventually run against Postgres or MySQL.
// The harness owns the engine-agnostic pieces (the committed process, the
// collector, capture/baseline); the Engine owns how committed is told to ingest
// from this source and how the syncable sink is configured.
//
// This is the first slice of the seam — config generation. Later slices move
// readiness gating, sink reads, the source connection, and bulk load behind it
// too (see the mysql-cdc-a0 ticket). Implemented today by postgresEngine.
type Engine interface {
	// Dialect is the committed ingest/syncable dialect name ("postgres"|"mysql").
	Dialect() string
	// PostIngestable registers the per-table ingestable config for this source.
	PostIngestable(t *testing.T, table string)
	// PostSinkDatabase registers the database config the sink syncables target.
	PostSinkDatabase(t *testing.T)
	// PostSyncable registers the per-table sink syncable (output side).
	PostSyncable(t *testing.T, table string)
	// SlotName returns the Postgres replication slot for a table (preflight +
	// readiness gating). Empty for engines without slots.
	SlotName(table string) string

	// WaitReady gates until the table's ingestable is streaming (snapshot done).
	WaitReady(t *testing.T, table string)
	// WaitReadyCtx is the context-bounded readiness gate for source-restart paths.
	WaitReadyCtx(ctx context.Context, table string, timeout time.Duration)
	// SinkValue reads column col of the sink row keyed by pk, and whether it exists.
	SinkValue(table, pk, col string) (string, bool)
	// SinkCount returns the number of rows in a topic's sink table.
	SinkCount(t *testing.T, table string) int
	// Load bulk-inserts the dataset into the source database.
	Load(ctx context.Context, ds dataset.Dataset) error

	// Execer runs mutation scripts against the source (one Txn per recordedTxn).
	mutation.Execer
}
