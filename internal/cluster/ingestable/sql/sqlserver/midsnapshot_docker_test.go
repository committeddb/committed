//go:build docker

package sqlserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// TestSQLServerMidSnapshotResume covers the third resume state: a worker
// killed mid-snapshot resumes from its durable per-table cursor instead of
// restarting the enumeration — and the union of the two runs' entities covers
// every row exactly (converging duplicates allowed at the batch boundary, the
// documented snapshot re-observation semantics; silent GAPS are the defect
// class this pins against).
func TestSQLServerMidSnapshotResume(t *testing.T) {
	const rows = 10
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_midsnap`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_midsnap (pk INT NOT NULL PRIMARY KEY, v NVARCHAR(20))`)
	require.NoError(t, err)
	for i := 1; i <= rows; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO dbo.ct_midsnap (pk, v) VALUES (%d, 'v%d')", i, i))
		require.NoError(t, err)
	}

	typ := &cluster.Type{ID: "ct-midsnap", Name: "ct-midsnap"}
	config := &sql.Config{
		Type:             typ,
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_midsnap"},
		// batch_size 3 → several batches; the hook kills run 1 after batch 2
		// (~6 rows handed off), leaving a genuine mid-table cursor.
		Options: map[string]string{"poll_interval": "300ms", "batch_size": "3"},
	}

	// Run 1: abort after the second batch, capture the durable cursor.
	ctx1, cancel1 := context.WithCancel(context.Background())
	pr1 := make(chan *cluster.Proposal, 64)
	po1 := make(chan cluster.Position, 64)
	d1 := &sqlserver.SQLServerDialect{}
	injected := fmt.Errorf("injected mid-snapshot failure")
	d1.SetSnapshotBatchHookForTest(func(table string, batch int) error {
		if batch > 2 {
			return injected
		}
		return nil
	})
	go func() { _ = d1.Ingest(ctx1, config, nil, 0, pr1, po1) }()

	// Collect what run 1 emitted before wedging into its retry loop (the hook
	// fails every attempt's third batch). The inline checkpoints ride the
	// proposals; keep the LAST position seen — the durable resume cursor.
	seen1 := map[string]bool{}
	var checkpoint cluster.Position
	deadline := time.After(2 * time.Minute)
collect:
	for {
		select {
		case p := <-pr1:
			for _, e := range p.Entities {
				if !e.IsRefreshBoundary() {
					seen1[string(e.Key)] = true
				}
			}
			if len(p.Position) > 0 {
				checkpoint = append(cluster.Position(nil), p.Position...)
			}
		case pos := <-po1:
			checkpoint = append(cluster.Position(nil), pos...)
		case <-deadline:
			t.Fatal("run 1 never emitted its first batches")
		default:
			if len(seen1) >= 6 && len(checkpoint) > 0 {
				break collect
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	cancel1()
	require.Less(t, len(seen1), rows, "run 1 must have been killed mid-snapshot")

	// Run 2: resume from the cursor with no hook. It must complete the
	// snapshot and reach streaming.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	pr2 := make(chan *cluster.Proposal, 64)
	po2 := make(chan cluster.Position, 64)
	d2 := &sqlserver.SQLServerDialect{}
	go func() { _ = d2.Ingest(ctx2, config, checkpoint, 0, pr2, po2) }()

	seen2 := map[string]bool{}
	deadline2 := time.After(2 * time.Minute)
	for {
		done := true
		for i := 1; i <= rows; i++ {
			k := fmt.Sprintf("%d", i)
			if !seen1[k] && !seen2[k] {
				done = false
			}
		}
		if done {
			break
		}
		select {
		case p := <-pr2:
			for _, e := range p.Entities {
				if !e.IsRefreshBoundary() {
					seen2[string(e.Key)] = true
				}
			}
		case <-po2:
		case <-deadline2:
			missing := []string{}
			for i := 1; i <= rows; i++ {
				k := fmt.Sprintf("%d", i)
				if !seen1[k] && !seen2[k] {
					missing = append(missing, k)
				}
			}
			t.Fatalf("resume left a silent gap: rows %v never emitted by either run", missing)
		}
	}

	// The resume must not have restarted the enumeration from row 1: at least
	// the first batch's rows should NOT re-emit (they sit behind the cursor).
	require.False(t, seen2["1"] && seen2["2"] && seen2["3"] && seen2["4"] && seen2["5"] && seen2["6"],
		"run 2 re-emitted everything run 1 covered — the cursor was ignored (a full restart, not a resume)")
}
