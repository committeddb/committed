//go:build docker || integration

package dialects_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// The field incident end to end against a real Postgres: a payload whose
// string value embeds U+0000 (the JSON \u0000 escape) dead-letters at the
// TEXT column (SQLSTATE 22021), and the error MESSAGE names the offending
// field — the operator no longer hand-hunts the byte across every string
// column — while never carrying the field's value (row data; the message
// becomes a permanent replicated dead-letter record).
func TestPostgreSQLIntegration_NulByteDeadLetterNamesField(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}
	db, err := sql.NewDB(d, pgConnString)
	require.NoError(t, err)
	defer db.Close()

	table := uniqueTable(t)
	defer dropTable(t, table)

	cfg := &sql.Config{
		Topic: eventType.ID,
		Table: table,
		Mappings: []sql.Mapping{
			{JsonPath: "$.id", Column: "id", SQLType: "VARCHAR(32)"},
			{JsonPath: "$.name", Column: "name", SQLType: "TEXT"},
		},
		PrimaryKey: []string{"id"},
	}
	syncable := sql.New(db, cfg)
	require.NoError(t, syncable.Init())
	defer syncable.Close()

	_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(eventType, []byte("r1"),
			[]byte(`{"id":"r1","name":"legacy-padding\u0000\u0000-user@example.com"}`)),
	}})
	require.Error(t, err, "PG must reject the embedded NUL")
	require.ErrorContains(t, err, `"name"`, "the dead-letter message must name the offending field")
	require.ErrorContains(t, err, "U+0000")
	require.NotContains(t, err.Error(), "legacy-padding", "the field VALUE must never appear")
	require.True(t, errors.Is(err, cluster.ErrPermanent),
		"the NUL class is a per-row permanent error (dead-letter, not wedge)")

	// The clean sibling row still lands — one poisoned row never stalls the
	// stream (the incident's posture, now with diagnosis attached).
	_, err = syncable.Sync(context.Background(), &cluster.Actual{Entities: []*cluster.Entity{
		cluster.NewUpsertEntity(eventType, []byte("r2"), []byte(`{"id":"r2","name":"clean"}`)),
	}})
	require.NoError(t, err)
	var count int
	require.NoError(t, db.DB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, table)).Scan(&count))
	require.Equal(t, 1, count)
}
