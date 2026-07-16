package db_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	sqlsync "github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// newWalDBWithSQLParsers is newWalDB plus the real SQL database + syncable
// parsers registered (before Open, per the parser-ordering contract), so
// ProposeDatabase builds a real *sql.DB (lazily — no connection) and
// SyncableDatabases can extract a syncable's sql.db reference.
func newWalDBWithSQLParsers(t *testing.T) (*db.DB, *wal.Storage) {
	t.Helper()
	dir := t.TempDir()
	p := parser.New()
	p.AddDatabaseParser("sql", &sqlsync.DBParser{Dialects: map[string]sqlsync.Dialect{
		"postgres": &dialects.PostgreSQLDialect{},
	}})
	p.AddSyncableParser("sql", &sqlsync.SyncableParser{})
	s, err := wal.Open(dir, p, nil, nil, wal.WithoutFsync())
	require.NoError(t, err)
	d := db.New(uint64(1), db.Peers{1: ""}, s, p, nil, nil, db.WithTickInterval(testTickInterval))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })
	return d, s
}

// TestProposeDatabase_GuardsConnectionChangeWithDependents drives the A8a guard
// through its real entry point (ticket resource-teardown-lifecycle-close): a
// re-POST that changes a database's connection pool is rejected with a 409
// (DatabaseInUseError) when a syncable still references it — because the syncable
// captured that pool at build time and a database apply does not rebuild it.
// A first POST, a change with no dependents, and an identical re-POST are all
// allowed. Fails on the pre-fix code, which had no guard and accepted every
// re-POST.
func TestProposeDatabase_GuardsConnectionChangeWithDependents(t *testing.T) {
	d, _ := newWalDBWithSQLParsers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbCfg := func(conn string) *cluster.Configuration {
		return &cluster.Configuration{ID: "sink", MimeType: "text/toml", Data: []byte(
			"[database]\ntype = \"sql\"\nname = \"sink\"\n[sql]\ndialect = \"postgres\"\nconnectionString = \"" + conn + "\"\n")}
	}
	const connA = "postgres://u:p@hostA:5432/db"
	const connB = "postgres://u:p@hostB:5432/db"

	// First POST — allowed (no prior config to preserve).
	require.NoError(t, d.ProposeDatabase(ctx, dbCfg(connA)))

	// Changed connection while nothing references the database — allowed (the
	// apply path safely closes the old pool and builds the new one).
	require.NoError(t, d.ProposeDatabase(ctx, dbCfg(connB)))
	require.NoError(t, d.ProposeDatabase(ctx, dbCfg(connA))) // back to A

	// A syncable now references the database. Proposed raw (not via ProposeSyncable)
	// with an intentionally-incomplete config — it names sql.db but omits sql.topic —
	// so the apply-path build degrades: saveSyncable persists the bytes first, then
	// records a config error and starts NO worker (no sync-channel handoff). The
	// config, and its database reference, is persisted for the guard to enumerate.
	seed, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{
		ID: "mysync", Name: "mysync", MimeType: "text/toml",
		Data: []byte("[syncable]\ntype = \"sql\"\nname = \"mysync\"\n[sql]\ndb = \"sink\"\n"),
	})
	require.NoError(t, err)
	require.NoError(t, d.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{seed}}))

	// Identical re-POST with the dependent present — allowed: the pool is kept, so
	// nothing is swapped out from under the syncable.
	require.NoError(t, d.ProposeDatabase(ctx, dbCfg(connA)))

	// Changed connection WITH the dependent — rejected 409 naming the syncable.
	err = d.ProposeDatabase(ctx, dbCfg(connB))
	var inUse *cluster.DatabaseInUseError
	require.ErrorAs(t, err, &inUse, "a connection change with a dependent syncable must be rejected")
	require.Equal(t, "sink", inUse.DatabaseID)
	require.Len(t, inUse.DependentSyncables, 1)
	require.Equal(t, "mysync", inUse.DependentSyncables[0].ID)
	require.Equal(t, "database_in_use_requires_syncable_teardown", inUse.Code())
}
