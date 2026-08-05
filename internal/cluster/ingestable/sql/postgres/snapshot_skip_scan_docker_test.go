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

// TestPostgresSnapshotNaNScansCompletely pins two facts around the
// mysql-snapshot-cursor-marshal-skip-rescan fix. First: a float8 NaN does NOT
// trip the marshal-skip branch — this dialect reads every column ::text, so
// NaN arrives as the string "NaN" and the row emits normally (that branch is
// defensive today; the fix made it advance the scan instead of silently
// truncating the table if it ever fires). Second: the whole 5-row table
// arrives at batch_size 1 — if a future decode change ever made NaN (or
// anything else) start failing marshal, THIS assertion is what turns the old
// silent truncation into a loud test failure: rows after the failing row
// would stop arriving.
func TestPostgresSnapshotNaNScansCompletely(t *testing.T) {
	const table = "skip_scan_nan"

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(8) NOT NULL PRIMARY KEY, val DOUBLE PRECISION)`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(
		`INSERT INTO %s (pk, val) VALUES ('1', 1.5), ('2', 2.5), ('3', 'NaN'::float8), ('4', 4.5), ('5', 5.5)`, table))
	require.NoError(t, err)
	db.Close()

	cleanReplication(t, "slot_skipscan", "pub_skipscan")
	defer cleanReplication(t, "slot_skipscan", "pub_skipscan")

	config := &sql.Config{
		Type:             &cluster.Type{ID: "skipscan", Name: "skipscan"},
		Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		// batch_size 1 makes the NaN row a whole batch by itself — the exact
		// shape that used to read as "table exhausted".
		Options: map[string]string{"slot_name": "slot_skipscan", "publication": "pub_skipscan", "batch_size": "1"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 20)
	po := make(chan cluster.Position, 20)
	go func() {
		_ = (&postgres.PostgreSQLDialect{}).Ingest(ctx, config, nil, 0, pr, po)
	}()

	deadline := time.After(30 * time.Second)
	seen := map[string]bool{}
	for len(seen) < 5 {
		select {
		case p := <-pr:
			for _, e := range p.Entities {
				if len(e.Key) > 0 {
					seen[string(e.Key)] = true
				}
			}
		case <-po:
		case <-deadline:
			t.Fatalf("timed out: seen=%v — a skipped or failing row must not end the table scan", seen)
		}
	}
	for _, k := range []string{"1", "2", "3", "4", "5"} {
		require.Truef(t, seen[k], "row %s must arrive (NaN rides as its text form)", k)
	}
}
