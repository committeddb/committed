//go:build docker

package harness

import (
	"fmt"
	"testing"
)

// postgresEngine is the Postgres backing of Engine. In this first slice it owns
// only the source connection string and the per-table replication-slot names,
// and delegates config generation to the existing postIngestable / postSink*
// helpers. Later slices fold the source connection, readiness gating, sink
// reads, and bulk load in here too.
type postgresEngine struct {
	connStr   string
	slotNames map[string]string // table → replication slot name
}

func newPostgresEngine(connStr string) *postgresEngine {
	return &postgresEngine{connStr: connStr, slotNames: map[string]string{}}
}

func (*postgresEngine) Dialect() string { return "postgres" }

func (e *postgresEngine) PostIngestable(t *testing.T, table string) {
	t.Helper()
	slot := fmt.Sprintf("slot_%s", table)
	pub := fmt.Sprintf("pub_%s", table)
	e.slotNames[table] = slot
	postIngestable(t, table, e.connStr, slot, pub)
}

func (e *postgresEngine) PostSinkDatabase(t *testing.T) {
	t.Helper()
	postSinkDatabase(t, e.connStr)
}

func (*postgresEngine) PostSyncable(t *testing.T, table string) {
	t.Helper()
	postSyncable(t, table)
}

func (e *postgresEngine) SlotName(table string) string { return e.slotNames[table] }
