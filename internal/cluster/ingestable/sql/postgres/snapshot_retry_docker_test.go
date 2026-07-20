//go:build docker || integration

package postgres_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// failOnce returns a snapshot-batch hook that fails exactly once when pred
// matches, recording that it fired. Later calls pass, so the reconnect retry
// proceeds — modeling a transient source error at an exact enumeration point.
func failOnce(fired *atomic.Bool, pred func(table string, batch int) bool) func(string, int) error {
	return func(table string, batch int) error {
		if pred(table, batch) && fired.CompareAndSwap(false, true) {
			return fmt.Errorf("injected transient snapshot failure (table=%s batch=%d)", table, batch)
		}
		return nil
	}
}

// TestPostgresSnapshotResumesAfterTransientError pins the first-run branch of
// the snapshot-intent fix: a transient error mid-snapshot must RESUME the
// enumeration on the reconnect retry, not abandon it. Before the fix, the
// retry saw slotIsNew=false + no resume cursor, matched no snapshot case, and
// went straight to streaming — rows never enumerated never reached the log
// while the worker reported healthy streaming (silent data loss).
func TestPostgresSnapshotResumesAfterTransientError(t *testing.T) {
	table := "snapretry_first"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('r%d', 'v%d')`, table, i, i))
		require.NoError(t, err)
	}
	db.Close()

	cleanReplication(t, "slot_snapretry1", "pub_snapretry1")
	defer cleanReplication(t, "slot_snapretry1", "pub_snapretry1")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "snapretry1", Name: "snapretry1"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		// batch_size 2 over 5 rows = 3 batches, so the injected failure at
		// batch 2 lands mid-enumeration with rows on both sides of it.
		Options: map[string]string{"slot_name": "slot_snapretry1", "publication": "pub_snapretry1", "batch_size": "2"},
	}

	var fired atomic.Bool
	d := &postgres.PostgreSQLDialect{}
	d.SetSnapshotBatchHookForTest(failOnce(&fired, func(_ string, batch int) bool { return batch == 2 }))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	go func() { _ = d.Ingest(ctx, config, nil, 0, proposalChan, positionChan) }()

	// All 5 rows plus the closing refresh-boundary marker must arrive despite
	// the injected failure (the reconnect retry resumes the snapshot).
	deadline := time.After(30 * time.Second)
	seen := map[string]bool{}
	total := 0
	sawMarker := false
	for len(seen) < 5 || !sawMarker {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					sawMarker = true
					continue
				}
				seen[string(e.Key)] = true
				total++
			}
		case <-positionChan:
		case <-deadline:
			t.Fatalf("timed out: have %d/5 rows (marker=%v) — a transient snapshot error must resume, not abandon, the snapshot", len(seen), sawMarker)
		}
	}
	require.True(t, fired.Load(), "the injected failure never fired — the test did not exercise the retry path")
	// The retry must RESUME from the last handed-off batch, not restart the
	// enumeration: rows 1-2 were handed off before the failure, so exactly 5
	// entities total means nothing was re-read.
	require.Equal(t, 5, total, "resume must continue from the batch cursor, not re-enumerate handed-off rows")
}

// TestPostgresSlotRecreateRefreshResumesAfterTransientError pins the
// slot-recreate branch: the gap-recovery re-snapshot (bumped epoch + closing
// marker) must survive a transient error. Before the fix, the retry saw
// slotIsNew=false and streamed from the new consistent point — the lost-WAL
// window's rows were skipped and the reconciling marker never ran.
func TestPostgresSlotRecreateRefreshResumesAfterTransientError(t *testing.T) {
	table := "snapretry_slot"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_snapretry2", "pub_snapretry2")
	defer cleanReplication(t, "slot_snapretry2", "pub_snapretry2")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "snapretry2", Name: "snapretry2"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options:          map[string]string{"slot_name": "slot_snapretry2", "publication": "pub_snapretry2"},
	}

	// Phase 1: ingest, stream 'before', capture the commit position that will
	// go stale when the slot is dropped.
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)
	go func() { _ = (&postgres.PostgreSQLDialect{}).Ingest(ctx1, config, nil, 0, proposalChan1, positionChan1) }()
	waitForSlot(t, "slot_snapretry2")

	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('before', 'v1')`, table))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(15 * time.Second)
	seen := map[string]bool{}
	var lastPos cluster.Position
	for !seen["before"] || lastPos == nil {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-positionChan1:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("phase 1: timed out waiting for 'before' + a commit position")
		}
	}
	cancel1()

	// Drop the slot and insert 'gap' — the row only a re-snapshot can recover.
	dropSlotWhenInactive(t, "slot_snapretry2")
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('gap', 'v2')`, table))
	require.NoError(t, err)
	db.Close()

	// Phase 2: resume from the stale checkpoint with a failure injected into
	// the re-snapshot's first batch. The retry must re-run the refresh — rows
	// AND the epoch-bumped closing marker.
	var fired atomic.Bool
	d := &postgres.PostgreSQLDialect{}
	d.SetSnapshotBatchHookForTest(failOnce(&fired, func(_ string, _ int) bool { return true }))

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)
	go func() { _ = d.Ingest(ctx2, config, lastPos, 0, proposalChan2, positionChan2) }()
	waitForSlot(t, "slot_snapretry2")

	deadline = time.After(30 * time.Second)
	genByKey := map[string]uint64{}
	sawMarker := false
	var markerEpoch uint64
	for genByKey["gap"] == 0 || !sawMarker {
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					sawMarker = true
					markerEpoch = e.Generation
					continue
				}
				genByKey[string(e.Key)] = e.Generation
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatalf("phase 2 timed out: gapGen=%d marker=%v — a transient error must not abandon the gap-recovery re-snapshot", genByKey["gap"], sawMarker)
		}
	}
	require.True(t, fired.Load(), "the injected failure never fired")
	require.Equal(t, uint64(2), markerEpoch, "the retried refresh keeps the bumped epoch")
	require.Equal(t, uint64(2), genByKey["gap"], "gap row re-emitted at the bumped epoch")
}

// TestPostgresAddedTableBackfillResumesAfterTransientError pins the
// added-table branch: the backfill set comes from ensurePublication's one-shot
// "newly added" result, so before the fix a transient error abandoned the
// backfill forever (the retry saw the table already in the publication). The
// intent must preserve the set across retries — and still emit NO marker (a
// partial refresh must not arm a topic-wide sweep).
func TestPostgresAddedTableBackfillResumesAfterTransientError(t *testing.T) {
	tableA := "snapretry_a"
	tableB := "snapretry_b"

	db := createDB(t)
	for _, tbl := range []string{tableA, tableB} {
		_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, tbl))
		require.NoError(t, err)
	}
	db.Close()

	cleanReplication(t, "slot_snapretry3", "pub_snapretry3")
	defer cleanReplication(t, "slot_snapretry3", "pub_snapretry3")

	mkConfig := func(tables []string) *sql.Config {
		return &sql.Config{
			Type:             &cluster.Type{ID: "snapretry3", Name: "snapretry3"},
			Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
			PrimaryKey:       []string{"pk"},
			ConnectionString: connString,
			Tables:           tables,
			Options:          map[string]string{"slot_name": "slot_snapretry3", "publication": "pub_snapretry3"},
		}
	}

	// Phase 1: ingest table A only, stream 'a1', capture a commit position.
	ctx1, cancel1 := context.WithCancel(context.Background())
	proposalChan1 := make(chan *cluster.Proposal, 10)
	positionChan1 := make(chan cluster.Position, 10)
	go func() {
		_ = (&postgres.PostgreSQLDialect{}).Ingest(ctx1, mkConfig([]string{tableA}), nil, 0, proposalChan1, positionChan1)
	}()
	waitForSlot(t, "slot_snapretry3")

	db = createDB(t)
	_, err := db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('a1', 'v1')`, tableA))
	require.NoError(t, err)
	db.Close()

	deadline := time.After(15 * time.Second)
	seen := map[string]bool{}
	var lastPos cluster.Position
	for !seen["a1"] || lastPos == nil {
		select {
		case p := <-proposalChan1:
			for _, e := range p.Entities {
				seen[string(e.Key)] = true
			}
		case pos := <-positionChan1:
			if isCommitPosition(t, pos) {
				lastPos = pos
			}
		case <-deadline:
			t.Fatal("phase 1: timed out waiting for 'a1' + a commit position")
		}
	}
	cancel1()

	// Pre-populate table B: these rows exist before B joins the publication, so
	// only the backfill can deliver them.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('b1', 'v1'), ('b2', 'v2')`, tableB))
	require.NoError(t, err)
	db.Close()

	// Phase 2: resume with B added to the config and a failure injected into
	// B's backfill. The retry must still backfill b1+b2.
	var fired atomic.Bool
	d := &postgres.PostgreSQLDialect{}
	d.SetSnapshotBatchHookForTest(failOnce(&fired, func(table string, _ int) bool { return table == tableB }))

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	proposalChan2 := make(chan *cluster.Proposal, 10)
	positionChan2 := make(chan cluster.Position, 10)
	go func() {
		_ = d.Ingest(ctx2, mkConfig([]string{tableA, tableB}), lastPos, 0, proposalChan2, positionChan2)
	}()

	deadline = time.After(30 * time.Second)
	seen = map[string]bool{}
	sawMarker := false
	for !seen["b1"] || !seen["b2"] {
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					sawMarker = true
					continue
				}
				seen[string(e.Key)] = true
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatalf("phase 2 timed out: b1=%v b2=%v — a transient error must not abandon the added-table backfill", seen["b1"], seen["b2"])
		}
	}
	require.True(t, fired.Load(), "the injected failure never fired")

	// Fence: stream a live row through B, proving streaming is up, then assert
	// the partial backfill emitted NO topic-wide marker along the way.
	db = createDB(t)
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES ('b-live', 'v3')`, tableB))
	require.NoError(t, err)
	db.Close()

	deadline = time.After(15 * time.Second)
	for !seen["b-live"] {
		select {
		case p := <-proposalChan2:
			for _, e := range p.Entities {
				if e.IsRefreshBoundary() {
					sawMarker = true
					continue
				}
				seen[string(e.Key)] = true
			}
		case <-positionChan2:
		case <-deadline:
			t.Fatal("timed out waiting for the live CDC row after the backfill")
		}
	}
	require.False(t, sawMarker, "a partial added-table backfill must not emit a refresh-boundary marker (it would sweep sibling tables' rows)")
}
