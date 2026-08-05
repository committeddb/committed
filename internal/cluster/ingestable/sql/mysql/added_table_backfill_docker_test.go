//go:build docker

package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

func backfillConfig(tables []string) *sql.Config {
	return &sql.Config{
		Type:             &cluster.Type{ID: "recon", Name: "recon"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           tables,
	}
}

// backfillSetup creates tables A and B, with B already holding the row that
// must (or, in the grandfather test, must not) be backfilled.
func backfillSetup(t *testing.T, tableA, tableB string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	for _, tbl := range []string{tableA, tableB} {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)", tbl))
		require.NoError(t, err)
	}
	_, err := db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('b_existing', 'v')", tableB))
	require.NoError(t, err)
}

// backfillPhase1 ingests tables=[A] from scratch, inserts a1, and returns a
// STREAMING checkpoint (position with no snapshot progress) — the resume
// point a re-POST would hand the next worker.
func backfillPhase1(t *testing.T, tableA string) cluster.Position {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 20)
	po := make(chan cluster.Position, 20)
	go func() {
		_ = (&mysql.MySQLDialect{}).Ingest(ctx, backfillConfig([]string{tableA}), nil, 0, pr, po)
	}()

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('a1', 'v')", tableA))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(20 * time.Second)
	var lastPos cluster.Position
	seenA1 := false
	for !seenA1 || lastPos == nil {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				if string(e.Key) == "a1" {
					seenA1 = true
				}
			}
		case pos := <-po:
			pp := &dialectpb.MySQLBinLogPosition{}
			require.NoError(t, proto.Unmarshal(pos, pp))
			// A streaming checkpoint: position present, snapshot done.
			if pp.Name != "" && pp.SnapshotProgress == nil && seenA1 {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("phase 1: timed out waiting for a1 + a streaming checkpoint")
		}
	}
	return lastPos
}

// TestMysqlAddedTableBackfill is the MySQL twin of
// TestPostgresPublicationReconciledOnAddedTable — the dialect-parity
// regression for mysql-cdc-repost-added-table-no-backfill. A re-POST that
// adds a table to sql.tables used to stream its ongoing changes but never
// backfill its history (a partial, surprising sink). Now the resume detects
// the config-added table via the durable snapshotted-tables registry and
// backfills EXACTLY it: the pre-existing row arrives, no refresh-boundary
// marker is emitted (a topic sweep would delete sibling rows the backfill
// does not re-emit), the sibling is not re-scanned, and the added table's
// live changes keep streaming afterward.
func TestMysqlAddedTableBackfill(t *testing.T) {
	const tableA, tableB = "recon_my_a", "recon_my_b"
	backfillSetup(t, tableA, tableB)
	lastPos := backfillPhase1(t, tableA)

	// Phase 2: the re-POST — resume with tables=[A,B].
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 20)
	po := make(chan cluster.Position, 20)
	go func() {
		_ = (&mysql.MySQLDialect{}).Ingest(ctx, backfillConfig([]string{tableA, tableB}), lastPos, 0, pr, po)
	}()

	// Drain until the backfill's COMPLETION CHECKPOINT (a progress-free
	// position whose registry now includes tableB) — it is emitted after any
	// markers would be, so stopping only at b_existing would exit before a
	// wrongly-emitted marker arrived and the zero-marker assertion below
	// would vacuously pass (it did, in this test's first red-proof).
	deadline := time.After(20 * time.Second)
	seen := map[string]bool{}
	markerCount, a1Replays := 0, 0
	backfillCheckpointed := false
	for !seen["b_existing"] || !backfillCheckpointed {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					markerCount++
					continue
				}
				if string(e.Key) == "a1" {
					a1Replays++
				}
				seen[string(e.Key)] = true
			}
		case pos := <-po:
			pp := &dialectpb.MySQLBinLogPosition{}
			require.NoError(t, proto.Unmarshal(pos, pp))
			if pp.SnapshotProgress == nil {
				for _, tbl := range pp.SnapshottedTables {
					if tbl == tableB {
						backfillCheckpointed = true
					}
				}
			}
		case <-deadline:
			t.Fatal("phase 2: tableB's pre-existing row was never backfilled — added table not detected")
		}
	}
	require.Zero(t, markerCount,
		"a partial backfill must not emit a refresh-boundary marker (it would sweep sibling rows)")
	require.Zero(t, a1Replays, "the sibling table must not be re-scanned")

	// The added table's ongoing changes stream after the backfill.
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('b_live', 'v')", tableB))
	require.NoError(t, err)
	db.Close()
	deadline = time.After(20 * time.Second)
	for !seen["b_live"] {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-po:
		case <-deadline:
			t.Fatal("phase 2: the added table's live change never streamed")
		}
	}
}

// TestMysqlLegacyCheckpointNoRetroactiveBackfill pins the grandfather rule: a
// pre-feature checkpoint carries no snapshotted-tables registry, and an empty
// registry must mean "everything currently configured is already snapshotted"
// — NOT "backfill everything". Otherwise the first restart after upgrading
// would surprise-rescan every table of every existing deployment. The cost of
// the rule, pinned here so it stays deliberate: an add made BEFORE the
// upgrade is not detected (exactly the old behavior); the added table still
// streams its live changes.
func TestMysqlLegacyCheckpointNoRetroactiveBackfill(t *testing.T) {
	const tableA, tableB = "recon_my_leg_a", "recon_my_leg_b"
	backfillSetup(t, tableA, tableB)
	lastPos := backfillPhase1(t, tableA)

	// Strip the registry, simulating a checkpoint written before the feature.
	pp := &dialectpb.MySQLBinLogPosition{}
	require.NoError(t, proto.Unmarshal(lastPos, pp))
	pp.SnapshottedTables = nil
	legacy, err := proto.Marshal(pp)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 20)
	po := make(chan cluster.Position, 20)
	go func() {
		_ = (&mysql.MySQLDialect{}).Ingest(ctx, backfillConfig([]string{tableA, tableB}), legacy, 0, pr, po)
	}()

	// The live change must stream; the pre-existing row must NOT arrive.
	db := createDB(t)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES ('b_live', 'v')", tableB))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(20 * time.Second)
	seen := map[string]bool{}
	for !seen["b_live"] {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case <-po:
		case <-deadline:
			t.Fatal("live change never streamed on the legacy-checkpoint resume")
		}
	}
	require.False(t, seen["b_existing"],
		"a legacy (registry-less) checkpoint must be grandfathered, not retroactively backfilled")
}
