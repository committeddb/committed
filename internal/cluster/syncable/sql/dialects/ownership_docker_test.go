//go:build docker || integration

package dialects_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/sql"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// dropDestination tears a sink down in drop mode; the tests that rebuild a
// projection from 0 use it between the two halves.
func dropDestination(t *testing.T, s cluster.Teardownable) {
	t.Helper()
	_, err := s.Teardown(false)
	require.NoError(t, err)
}

// The ownership protocol against a real database — the rule committed
// applies to everything it touches: what it created it may drop, what it
// did not create it leaves alone. The note beside the table records which,
// and keepData is the one-way handover that turns the first into the second.
func TestDestinationOwnership_Postgres(t *testing.T) {
	runOwnership(t, &dialects.PostgreSQLDialect{}, pgConnString,
		"SELECT to_regclass($1) IS NOT NULL")
}

func TestDestinationOwnership_MySQL(t *testing.T) {
	runOwnership(t, &dialects.MySQLDialect{}, mysqlConn(t),
		"SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?")
}

func runOwnership(t *testing.T, d sql.Dialect, conn, existsSQL string) {
	t.Helper()
	ctx := context.Background()
	db, err := sql.NewDB(d, conn)
	require.NoError(t, err)
	defer db.Close()
	table := uniqueTable(t)
	config := &sql.Config{Topic: "t", Table: table, PrimaryKey: []string{"id"}, Mappings: []sql.Mapping{{Column: "id", SQLType: "VARCHAR(64)", JsonPath: "$.id"}}}
	exists := func() bool {
		var ok bool
		require.NoError(t, db.DB.QueryRowContext(ctx, existsSQL, table).Scan(&ok))
		return ok
	}
	note := func() (owned, present bool) {
		var v int64
		err := db.DB.QueryRowContext(ctx, d.DestinationSelectSQL(), table).Scan(&v, &owned)
		if err == gosql.ErrNoRows {
			return false, false
		}
		require.NoError(t, err)
		require.Equal(t, int64(sql.RenderingVersion), v)
		return owned, true
	}
	t.Cleanup(func() {
		_, _ = db.DB.ExecContext(ctx, d.DropDDL(config))
		_, _ = db.DB.ExecContext(ctx, d.DestinationDeleteSQL(), table)
	})

	// Created by committed: claimed at Init, dropped (note and all) on delete.
	require.False(t, exists())
	sink := sql.New(db, config)
	require.NoError(t, sink.Init())
	owned, present := note()
	require.True(t, present && owned, "Init must claim the table it created")
	dropped, err := sink.Teardown(false)
	require.NoError(t, err)
	require.True(t, dropped)
	require.False(t, exists(), "an owned table is dropped")
	_, present = note()
	require.False(t, present, "the note goes with the table")
	require.NoError(t, sink.Close())

	// Attached: the operator's table exists before committed does. Init claims
	// nothing; the worker's first-contact stamp notes it not owned; delete
	// leaves table and note alone.
	_, err = db.DB.ExecContext(ctx, d.CreateDDL(config))
	require.NoError(t, err)
	sink = sql.New(db, config)
	require.NoError(t, sink.Init())
	_, present = note()
	require.False(t, present, "attaching writes no note")
	require.NoError(t, sink.StampRendering(ctx))
	owned, present = note()
	require.True(t, present)
	require.False(t, owned, "a stamp never claims")
	dropped, err = sink.Teardown(false)
	require.NoError(t, err)
	require.False(t, dropped)
	require.True(t, exists(), "a table committed did not create stays")
	_, present = note()
	require.True(t, present, "and so does its note")
	require.NoError(t, sink.Close())

	// Handover: the operator drops their table; committed creates the next one
	// (claim on top of the surviving note), then keepData disowns it. After
	// that, a plain delete leaves it too.
	_, err = db.DB.ExecContext(ctx, d.DropDDL(config))
	require.NoError(t, err)
	sink = sql.New(db, config)
	require.NoError(t, sink.Init())
	owned, present = note()
	require.True(t, present && owned, "a claim overwrites a not-owned note")
	dropped, err = sink.Teardown(true)
	require.NoError(t, err)
	require.False(t, dropped)
	require.True(t, exists(), "keepData removes nothing")
	owned, present = note()
	require.True(t, present)
	require.False(t, owned, "keepData relinquishes ownership")
	dropped, err = sink.Teardown(false)
	require.NoError(t, err)
	require.False(t, dropped)
	require.True(t, exists(), "a handed-over table is not dropped by a later delete")
	require.NoError(t, sink.Close())
}
