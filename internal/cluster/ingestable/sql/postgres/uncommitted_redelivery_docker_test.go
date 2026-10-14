//go:build docker || integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// A transaction handed to the worker but never committed (a leader change, a
// failed proposal, a crash) must be streamed again when the worker resumes
// from its durable position. Postgres never rewinds a logical slot: a resume
// from below the slot's confirmed_flush_lsn starts at confirmed_flush_lsn, so
// the dialect must not confirm a transaction until the worker reports it
// durable. awaitCommit stands in for a worker that commits what it receives;
// the "b" proposal is received and never committed.
func TestPostgresUncommittedTransactionRedelivered(t *testing.T) {
	const table, slot, pub = "redeliver_table", "slot_redeliver", "pub_redeliver"
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	cleanReplication(t, slot, pub)

	config := &sql.Config{
		Type:             &cluster.Type{ID: "redeliver", Name: "redeliver"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options:          sql.Options{SlotName: slot, Publication: pub},
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 10)
	po1 := make(chan cluster.Position, 10)
	done1 := make(chan struct{})
	go func() {
		defer close(done1)
		_ = (&postgres.PostgreSQLDialect{}).Ingest(ctx1, config, nil, 0, pr1, po1)
	}()
	waitForSlot(t, slot)

	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('a', 'committed')`, table))
	require.NoError(t, err)
	durable := awaitCommit(t, pr1, po1, 20*time.Second, "a").Position
	require.NotZero(t, positionLSN(durable))

	// "b" reaches the worker, which never commits it.
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('b', 'lost')`, table))
	require.NoError(t, err)
	var bEnd uint64
	select {
	case p := <-pr1:
		require.Equal(t, "b", string(p.Entities[0].Key))
		bEnd = positionLSN(p.Position)
		require.NotZero(t, bEnd, "b's proposal carries its post-transaction position")
	case <-time.After(20 * time.Second):
		t.Fatal("b was never handed to the worker")
	}

	// The source must not hear that b is done while it is not.
	require.Never(t, func() bool { return confirmedFlush(t, slot) >= bEnd },
		2*time.Second, 100*time.Millisecond, "the slot was confirmed past a transaction the worker never committed")

	cancel1()
	<-done1
	require.Eventually(t, func() bool { return !slotActive(t, slot) }, 10*time.Second, 100*time.Millisecond)

	// The restart resumes from the durable position and gets b again.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 10)
	po2 := make(chan cluster.Position, 10)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx2, config, durable, 0, pr2, po2) }()
	got := awaitCommit(t, pr2, po2, 20*time.Second, "b")
	require.Equal(t, `{"pk":"b","val":"lost"}`, string(got.Entity("b").Data))
}

// confirmedFlush is the slot's confirmed_flush_lsn as a WAL byte offset.
func confirmedFlush(t *testing.T, slot string) uint64 {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	var lsn uint64
	require.NoError(t, db.QueryRow(
		`SELECT coalesce(pg_wal_lsn_diff(confirmed_flush_lsn, '0/0'), 0)::bigint FROM pg_replication_slots WHERE slot_name = $1`,
		slot).Scan(&lsn))
	return lsn
}

func slotActive(t *testing.T, slot string) bool {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	var active bool
	require.NoError(t, db.QueryRow(`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active))
	return active
}
