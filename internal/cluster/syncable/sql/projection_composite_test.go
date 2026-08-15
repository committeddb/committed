package sql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	sql "github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects/testdialects"
)

var visitEventType = &cluster.Type{ID: "visit-workarea-event", Name: "VisitWorkareaEvent"}

// visitStatusConfig is the field shape that flipped the YAGNI gate: a
// latest-event-wins reduction keyed by a composite identity (the
// VisitWorkareaStatuses fold the pilot had to leave as a window function).
func visitStatusConfig() *sql.ProjectionConfig {
	return &sql.ProjectionConfig{
		Topic:      "visit-workarea-event",
		Table:      "visit_workarea_statuses",
		PrimaryKey: []string{"visit_id", "workarea_id"},
		Columns: []sql.ProjectionColumn{
			{Name: "visit_id", SQLType: "VARCHAR(64)"},
			{Name: "workarea_id", SQLType: "VARCHAR(64)"},
			{Name: "status", SQLType: "VARCHAR(32)"},
		},
		Rules: []sql.ProjectionRule{{
			Set: []sql.ProjectionSet{{Column: "status", From: "$.status"}},
		}},
	}
}

// A composite-keyed rule upsert binds the key columns first, positionally
// (visit_id then workarea_id, the primaryKey order), then the set values —
// each key read from its own defaulted $.<col> path.
func TestProjectionCompositeUpsertBindsKeysPositionally(t *testing.T) {
	projection, mock, rules, _ := newMockProjection(t, visitStatusConfig(), nil)

	mock.ExpectBegin()
	args := []driver.Value{"v1", "w2", "done"}
	rules[0].ExpectExec().WithArgs(append(args, args...)...).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(visitEventType, []byte(`["v1","w2"]`), eventJSON(t, map[string]any{
			"visit_id": "v1", "workarea_id": "w2", "status": "done",
		})),
	}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// A composite tombstone decodes to one value per key column — the delete
// addresses exactly the encoded row (WHERE visit_id=? AND workarea_id=?),
// never a raw-encoding bind that would match nothing.
func TestProjectionCompositeDeleteDecodesTombstone(t *testing.T) {
	projection, mock, _, deletePrepare := newMockProjection(t, visitStatusConfig(), nil)

	mock.ExpectBegin()
	deletePrepare.ExpectExec().WithArgs("v1", "w2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(visitEventType, []byte(`["v1","w2"]`)),
	}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// onDelete = "clear" on a composite source: the clear UPDATE's WHERE is the
// full key-column set, so a composite tombstone NULLs the owned columns of
// exactly the addressed row — the same arity contract as delete-row.
func TestProjectionCompositeClearBindsAllKeyColumns(t *testing.T) {
	cfg := visitStatusConfig()
	cfg.Sources = []sql.ProjectionSource{{
		Topic:    cfg.Topic,
		OnDelete: "clear",
		Rules:    cfg.Rules,
	}}
	cfg.Topic, cfg.Rules = "", nil

	dialect, mock, err := testdialects.NewSQLMockDialect()
	require.NoError(t, err)
	db, err := sql.NewDB(dialect, "")
	require.NoError(t, err)
	defer db.Close()

	ddlMappings := []sql.Mapping{
		{Column: "visit_id", SQLType: "VARCHAR(64)"},
		{Column: "workarea_id", SQLType: "VARCHAR(64)"},
		{Column: "status", SQLType: "VARCHAR(32)"},
	}
	ddlConfig := &sql.Config{Table: cfg.Table, Mappings: ddlMappings, PrimaryKey: cfg.PrimaryKey}
	mock.ExpectExec(dialect.CreateDDL(ddlConfig)).WillReturnResult(driver.ResultNoRows)
	ruleMappings := []sql.Mapping{{Column: "visit_id"}, {Column: "workarea_id"}, {Column: "status"}}
	mock.ExpectPrepare(dialect.CreateSQL(&sql.Config{Table: cfg.Table, Mappings: ruleMappings, PrimaryKey: cfg.PrimaryKey}))
	clearPrepare := mock.ExpectPrepare(dialect.CreateClearSQL(ddlConfig, []string{"status"}))
	mock.ExpectPrepare(dialect.CreateDeleteSQL(ddlConfig))

	p := sql.NewProjection(db, cfg, nil, "visit_statuses")
	require.NoError(t, p.Init())

	mock.ExpectBegin()
	clearPrepare.ExpectExec().WithArgs("v1", "w2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	_, err = p.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewDeleteEntity(visitEventType, []byte(`["v1","w2"]`)),
	}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// The two key-shape guards, both directions. A silent no-op DELETE is the
// worst outcome (deleted rows persist forever; an RTBF erasure quietly
// fails) — both mismatches must dead-letter loudly, without executing, and
// without the key value in the message (the key IS the erased subject).
func TestProjectionDeleteKeyShapeMismatchesAreLoud(t *testing.T) {
	t.Run("bare key against a composite projection", func(t *testing.T) {
		projection, mock, _, _ := newMockProjection(t, visitStatusConfig(), nil)
		mock.ExpectBegin()
		mock.ExpectRollback()
		_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(visitEventType, []byte("v1")),
		}})
		require.True(t, errors.Is(err, cluster.ErrPermanent), "got: %v", err)
		require.ErrorContains(t, err, "key shapes disagree")
		require.NotContains(t, err.Error(), "v1", "the key value must never appear in the message")
		require.NoError(t, mock.ExpectationsWereMet(), "the mismatched delete must not execute")
	})

	t.Run("composite-encoded key against a single-key projection", func(t *testing.T) {
		projection, mock, _, _ := newMockProjection(t, tenantProjectionConfig(), nil)
		mock.ExpectBegin()
		mock.ExpectRollback()
		_, err := projection.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
			cluster.NewDeleteEntity(tenantEventType, []byte(`["t1","extra"]`)),
		}})
		require.True(t, errors.Is(err, cluster.ErrPermanent), "got: %v", err)
		require.ErrorContains(t, err, "composite entity key")
		require.NotContains(t, err.Error(), "t1", "the key value must never appear in the message")
		require.NoError(t, mock.ExpectationsWereMet(), "the mismatched delete must not execute")
	})
}
