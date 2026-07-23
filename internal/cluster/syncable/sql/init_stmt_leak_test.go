package sql_test

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

// TestSyncableInit_ClosesPreparedStmtsOnFailure pins #11: Init prepares statements
// sequentially against the shared, long-lived *sql.DB pool (preserved across
// config re-POSTs). If a later prepare fails, the parser discards the half-built
// Syncable — so the statements already prepared must be closed by Init itself, or
// they leak server-side on the destination and accumulate across every
// restart/reconcile re-parse until it hits its prepared-statement ceiling.
//
// Here the insert prepare succeeds (and is expected to be Closed) and the delete
// prepare fails; ExpectationsWereMet enforces the insert Close. Pre-fix Init
// returned without closing it and this fails.
func TestSyncableInit_ClosesPreparedStmtsOnFailure(t *testing.T) {
	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)

	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	defer db.Close()

	bs, err := os.ReadFile("./simple_syncable.toml")
	require.NoError(t, err)
	v := readConfig(t, "toml", bytes.NewReader(bs))
	config, err := (&sql.SyncableParser{}).ParseConfig(v, &TestDatabaseStorage{dbs: map[string]cluster.Database{"testdb": db}})
	require.NoError(t, err)

	mock.ExpectExec(dialect.CreateDDL(config)).WillReturnResult(driver.ResultNoRows)
	// The generation upsert prepares successfully — and MUST be closed when a
	// later prepare fails.
	mock.ExpectPrepare(dialect.CreateGenerationUpsertSQL(config)).WillBeClosed()
	// The DELETE-by-key prepare fails, aborting Init after the upsert was prepared.
	mock.ExpectPrepare(dialect.CreateDeleteSQL(config)).WillReturnError(errors.New("boom: prepare delete failed"))

	syncable := sql.New(db, config)
	require.Error(t, syncable.Init(), "Init must surface the delete-prepare failure")

	require.NoError(t, mock.ExpectationsWereMet(),
		"Init must close the already-prepared upsert statement when a later prepare fails, not leak it (#11)")
}

// ruleSQL recomputes the per-rule prepared SQL exactly as Projection.Init does
// (PK column plus the rule's Set columns), so the test can register the matching
// sqlmock prepare expectation.
func ruleSQL(dialect sql.Dialect, cfg *sql.ProjectionConfig, r sql.ProjectionRule) string {
	mappings := make([]sql.Mapping, 0, len(r.Set)+1)
	mappings = append(mappings, sql.Mapping{Column: cfg.PrimaryKey})
	for _, s := range r.Set {
		mappings = append(mappings, sql.Mapping{Column: s.Column})
	}
	return dialect.CreateSQL(&sql.Config{Table: cfg.Table, Mappings: mappings, PrimaryKey: cfg.PrimaryKey})
}

// TestProjectionInit_ClosesPreparedStmtsOnFailure is the projection twin of the
// syncable #11 red-proof, and the one that matters more: a projection prepares
// many statements (one per rule, per source, plus aggregate/lookup runtimes), so
// a mid-Init failure orphans more of them. It specifically guards that a source's
// already-prepared rule statements are reachable for cleanup even though the
// source failed before Init finished with it (the fix registers each source in
// p.sources before preparing its statements). Rule 0 prepares and must be Closed;
// rule 1 fails.
func TestProjectionInit_ClosesPreparedStmtsOnFailure(t *testing.T) {
	cfg := tenantProjectionConfig()

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	defer db.Close()

	ddlMappings := make([]sql.Mapping, 0, len(cfg.Columns))
	for _, c := range cfg.Columns {
		ddlMappings = append(ddlMappings, sql.Mapping{Column: c.Name, SQLType: c.SQLType})
	}
	mock.ExpectExec(dialect.CreateDDL(&sql.Config{Table: cfg.Table, Mappings: ddlMappings, PrimaryKey: cfg.PrimaryKey})).
		WillReturnResult(driver.ResultNoRows)
	// Rule 0 prepares successfully — and MUST be closed when rule 1 fails.
	mock.ExpectPrepare(ruleSQL(dialect, cfg, cfg.Rules[0])).WillBeClosed()
	mock.ExpectPrepare(ruleSQL(dialect, cfg, cfg.Rules[1])).WillReturnError(errors.New("boom: prepare rule 1 failed"))

	projection := sql.NewProjection(db, cfg, nil, "tenants")
	require.Error(t, projection.Init(), "Init must surface the rule-prepare failure")

	require.NoError(t, mock.ExpectationsWereMet(),
		"Init must close the source's already-prepared rule statements when a later prepare fails, not leak them (#11)")
}
