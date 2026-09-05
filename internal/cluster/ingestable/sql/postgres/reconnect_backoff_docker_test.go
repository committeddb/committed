//go:build docker

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// TestPostgresReconnectBackoffResetsAfterHealthySession pins the reset rule
// of the shared reconnect backoff at its Postgres reset point (the loop that
// climbed without ever resetting before): a session that reached streaming
// and then dropped reconnects at the initial delay. Without the reset, the
// second reconnect below would wait the doubled delay from the first.
func TestPostgresReconnectBackoffResetsAfterHealthySession(t *testing.T) {
	const table = "pgtest_backoff"
	const slot, pub = "slot_backoff", "pub_backoff"
	config := &sql.Config{
		Type:             &cluster.Type{ID: "backoff", Name: "backoff"},
		ConnectionString: connString,
		Tables:           []string{table},
		PrimaryKey:       []string{"pk"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		Options: map[string]string{"slot_name": slot, "publication": pub},
	}

	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS ` + table)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE ` + table + ` (pk VARCHAR(32) PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('seed', 's')`)
	require.NoError(t, err)

	cleanReplication(t, slot, pub)
	t.Cleanup(func() { cleanReplication(t, slot, pub) })

	// Capture the reconnect Warns (each carries the delay it is about to wait).
	core, observed := observer.New(zap.WarnLevel)
	defer zap.ReplaceGlobals(zap.New(core))()
	reconnects := func() []observer.LoggedEntry {
		return observed.FilterMessageSnippet("will reconnect").All()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()

	// Session 1 reaches streaming: the seed snapshot, then a live commit.
	ingesttest.Await(t, pr, po, 30*time.Second, nil, "seed")
	waitForSlot(t, slot)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('m1', 'x')`)
	require.NoError(t, err)
	awaitCommit(t, pr, po, 30*time.Second, "m1")

	// killWalsender ends the session the way a network fault would: the
	// server drops the replication connection, the stream exits with an
	// error, and the loop logs the delay it is about to wait.
	killWalsender := func() {
		_, err := db.Exec(
			`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL`,
			slot)
		require.NoError(t, err)
	}
	awaitReconnects := func(n int) []observer.LoggedEntry {
		require.Eventually(t, func() bool { return len(reconnects()) >= n }, 30*time.Second, 100*time.Millisecond,
			"the dropped replication connection must surface as a reconnect")
		return reconnects()
	}

	killWalsender()
	first := awaitReconnects(1)
	require.Equal(t, sql.ReconnectBackoffMin, first[0].ContextMap()["backoff"],
		"the first failure waits the initial delay")

	// Session 2 reaches streaming again — a live commit proves it — so the
	// delay must have reset. (A slot still held by the dying walsender can
	// cost an extra failed attempt here; count from after the healthy commit.)
	waitForSlot(t, slot)
	_, err = db.Exec(`INSERT INTO ` + table + ` VALUES ('m2', 'y')`)
	require.NoError(t, err)
	awaitCommit(t, pr, po, 30*time.Second, "m2")
	before := len(reconnects())

	killWalsender()
	all := awaitReconnects(before + 1)
	require.Equal(t, sql.ReconnectBackoffMin, all[before].ContextMap()["backoff"],
		"a session that reached streaming resets the reconnect backoff; without the reset this reconnect would wait the doubled delay")
}
