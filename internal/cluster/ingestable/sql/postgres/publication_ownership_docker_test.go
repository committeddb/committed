//go:build docker || integration

package postgres_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

func ownershipConfig(slot, pub string, tables ...string) *sql.Config {
	return &sql.Config{
		Type:             &cluster.Type{ID: "own", Name: "own"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           tables,
		Options:          sql.Options{SlotName: slot, Publication: pub},
	}
}

func createKeyTables(t *testing.T, tables ...string) {
	t.Helper()
	for _, tbl := range tables {
		execAll(t,
			fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tbl),
			fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY)`, tbl))
	}
}

// ingestRun is one Ingest call; err receives its return once it stops.
type ingestRun struct {
	pr     chan *cluster.Proposal
	po     chan cluster.Position
	err    chan error
	cancel context.CancelFunc
}

func startIngest(cfg *sql.Config, pos cluster.Position) *ingestRun {
	ctx, cancel := context.WithCancel(context.Background())
	r := &ingestRun{
		pr: make(chan *cluster.Proposal, 100), po: make(chan cluster.Position, 100),
		err: make(chan error, 1), cancel: cancel,
	}
	go func() { r.err <- (&postgres.PostgreSQLDialect{}).Ingest(ctx, cfg, pos, 0, r.pr, r.po) }()
	return r
}

// stop cancels the run and waits for Ingest to return, so the slot is free.
func (r *ingestRun) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.err:
		require.NoError(t, err)
	case <-time.After(20 * time.Second):
		t.Fatal("Ingest did not return after cancel")
	}
}

// parked waits for Ingest to return on its own and returns the error.
func (r *ingestRun) parked(t *testing.T) error {
	t.Helper()
	defer r.cancel()
	select {
	case err := <-r.err:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("Ingest did not park")
		return nil
	}
}

func publicationComment(t *testing.T, db *gosql.DB, pub string) (comment gosql.NullString, exists bool) {
	t.Helper()
	err := db.QueryRow(`SELECT obj_description(oid, 'pg_publication') FROM pg_publication WHERE pubname = $1`, pub).Scan(&comment)
	if err == gosql.ErrNoRows {
		return comment, false
	}
	require.NoError(t, err)
	return comment, true
}

func publicationMembers(t *testing.T, db *gosql.DB, pub string) []string {
	t.Helper()
	rows, err := db.Query(`SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename`, pub)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		out = append(out, n)
	}
	require.NoError(t, rows.Err())
	return out
}

// A publication committed creates carries its ownership record, keeps its
// table list equal to the config, and is dropped on teardown. A table the
// config stops watching leaves the publication, and its changes still in the
// slot's WAL are skipped, not parked on.
func TestPostgresPublicationOwned(t *testing.T) {
	createKeyTables(t, "own_a", "own_b")
	cleanReplication(t, "slot_own", "pub_own")
	db := createDB(t)
	defer db.Close()

	run := startIngest(ownershipConfig("slot_own", "pub_own", "own_a", "own_b"), nil)
	waitForSlot(t, "slot_own")
	execAll(t, `INSERT INTO own_a VALUES ('a1')`, `INSERT INTO own_b VALUES ('b1')`)
	pos := awaitCommit(t, run.pr, run.po, 20*time.Second, "a1", "b1").Position
	run.stop(t)

	comment, ok := publicationComment(t, db, "pub_own")
	require.True(t, ok)
	require.Equal(t, "committed: owned by the ingestable using replication slot slot_own", comment.String)

	// Written while stopped: own_b's change sits in the slot's WAL when the
	// config that stops watching own_b resumes.
	execAll(t, `INSERT INTO own_b VALUES ('b2')`, `INSERT INTO own_a VALUES ('a2')`)
	cfg := ownershipConfig("slot_own", "pub_own", "own_a")
	run = startIngest(cfg, pos)
	res := awaitCommit(t, run.pr, run.po, 20*time.Second, "a2")
	require.False(t, res.Seen["b2"])
	run.stop(t)
	require.Equal(t, []string{"own_a"}, publicationMembers(t, db, "pub_own"))

	require.NoError(t, (&postgres.PostgreSQLDialect{}).TeardownSource(cfg))
	_, ok = publicationComment(t, db, "pub_own")
	require.False(t, ok, "teardown drops the publication committed created")
}

// A publication the user created is the user's: committed adds its tables,
// skips changes to the user's other tables, removes nothing and never drops it.
func TestPostgresPublicationUserOwned(t *testing.T) {
	createKeyTables(t, "user_a", "user_extra")
	cleanReplication(t, "slot_user", "pub_user")
	execAll(t, `CREATE PUBLICATION pub_user FOR TABLE user_extra`)
	db := createDB(t)
	defer db.Close()

	cfg := ownershipConfig("slot_user", "pub_user", "user_a")
	run := startIngest(cfg, nil)
	waitForSlot(t, "slot_user")
	execAll(t, `INSERT INTO user_extra VALUES ('x1')`, `INSERT INTO user_a VALUES ('a1')`)
	res := awaitCommit(t, run.pr, run.po, 20*time.Second, "a1")
	require.False(t, res.Seen["x1"])
	run.stop(t)

	comment, ok := publicationComment(t, db, "pub_user")
	require.True(t, ok)
	require.False(t, comment.Valid, "committed does not mark a publication it did not create")
	require.Equal(t, []string{"user_a", "user_extra"}, publicationMembers(t, db, "pub_user"))

	require.NoError(t, (&postgres.PostgreSQLDialect{}).TeardownSource(cfg))
	require.Equal(t, []string{"user_a", "user_extra"}, publicationMembers(t, db, "pub_user"),
		"teardown leaves the user's publication in place")
}

// A publication another ingestable owns is refused: sharing it would let
// either one's reconcile remove the other's tables.
func TestPostgresPublicationOtherOwnerParks(t *testing.T) {
	createKeyTables(t, "other_a")
	cleanReplication(t, "slot_other", "pub_other")
	execAll(t,
		`CREATE PUBLICATION pub_other FOR TABLE other_a`,
		`COMMENT ON PUBLICATION pub_other IS 'committed: owned by the ingestable using replication slot slot_elsewhere'`)

	err := startIngest(ownershipConfig("slot_other", "pub_other", "other_a"), nil).parked(t)
	require.ErrorContains(t, err, `publication "pub_other" belongs to the ingestable using replication slot "slot_elsewhere"`)
}

// A change for a current member of an owned publication that no watched
// table routes stops the stream before it is acknowledged, instead of being
// dropped.
func TestPostgresPublicationUnroutableMemberParks(t *testing.T) {
	createKeyTables(t, "unr_a", "unr_extra")
	cleanReplication(t, "slot_unr", "pub_unr")

	run := startIngest(ownershipConfig("slot_unr", "pub_unr", "unr_a"), nil)
	waitForSlot(t, "slot_unr")
	execAll(t, `INSERT INTO unr_a VALUES ('a1')`)
	awaitCommit(t, run.pr, run.po, 20*time.Second, "a1")

	execAll(t, `ALTER PUBLICATION pub_unr ADD TABLE unr_extra`, `INSERT INTO unr_extra VALUES ('x1')`)
	err := run.parked(t)
	require.ErrorContains(t, err, `changes to public.unr_extra arrive on publication "pub_unr", but no watched table matches them`)
}

// A publication an earlier version created carries no ownership record. An
// ingestable that has streamed before adopts it; one starting fresh treats an
// uncommented publication as the user's (TestPostgresPublicationUserOwned).
func TestPostgresPublicationLegacyAdopted(t *testing.T) {
	createKeyTables(t, "legacy_a")
	cleanReplication(t, "slot_legacy", "pub_legacy")
	db := createDB(t)
	defer db.Close()

	cfg := ownershipConfig("slot_legacy", "pub_legacy", "legacy_a")
	run := startIngest(cfg, nil)
	waitForSlot(t, "slot_legacy")
	execAll(t, `INSERT INTO legacy_a VALUES ('a1')`)
	pos := awaitCommit(t, run.pr, run.po, 20*time.Second, "a1").Position
	run.stop(t)

	execAll(t, `COMMENT ON PUBLICATION pub_legacy IS NULL`)
	run = startIngest(cfg, pos)
	execAll(t, `INSERT INTO legacy_a VALUES ('a2')`)
	awaitCommit(t, run.pr, run.po, 20*time.Second, "a2")
	run.stop(t)

	comment, _ := publicationComment(t, db, "pub_legacy")
	require.Equal(t, "committed: owned by the ingestable using replication slot slot_legacy", comment.String)
}
