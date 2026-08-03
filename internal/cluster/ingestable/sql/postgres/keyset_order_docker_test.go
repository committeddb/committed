//go:build docker

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

// TestPostgresSnapshotIntegerPKKeysetOrder is the regression test for the
// keyset ORDER BY alias-capture bug: readBatch's SELECT list casts every
// column to text ALIASED BACK TO ITS OWN NAME (job_id::text AS job_id) for
// snapshot/CDC byte parity, and SQL resolves a BARE identifier in ORDER BY to
// the output alias first — so the scan walked an integer pk in TEXT order
// while the keyset cursor compared NUMERICALLY. Mixed semantics silently skip
// every short-digit key the numeric cursor has already passed and terminate
// early on a short batch, reporting a partial snapshot as complete (the field
// incident: 82% of a 429K-row table, green status). Integer pks 0..24 with
// batch_size 10 trip it exactly: text order visits 0,1,10..17 first, then
// "pk > 17" numerically excludes 2..9 forever — 17 of 25 rows. The earlier
// chunking test never caught this because its VARCHAR pks are zero-padded to
// uniform width, where text order and value order coincide. The fix qualifies
// the ORDER BY / WHERE columns with the table name, which alias resolution
// never captures.
func TestPostgresSnapshotIntegerPKKeysetOrder(t *testing.T) {
	table := "pg_intpk_order_table"
	const rowCount = 25

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk INTEGER NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (pk, val) VALUES (%d, 'v%d')`, table, i, i))
		require.NoError(t, err)
	}
	db.Close()

	cleanReplication(t, "slot_intpk_order", "pub_intpk_order")

	config := &sql.Config{
		Type: &cluster.Type{ID: "intpk-order", Name: "intpk-order"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: connString,
		Tables:           []string{table},
		Options: map[string]string{
			"slot_name":   "slot_intpk_order",
			"publication": "pub_intpk_order",
			"batch_size":  "10", // < rowCount, and pks span 1-2 digit lengths
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 40)
	positionChan := make(chan cluster.Position, 40)
	dialect := &postgres.PostgreSQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect.Ingest(ctx, config, nil, 0, proposalChan, positionChan)
	}()

	deadline := time.After(20 * time.Second)
	seen := make(map[string]bool)
	for len(seen) < rowCount {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if !e.IsDelete() && e.Type != nil && e.Type.ID == "intpk-order" && len(e.Key) > 0 && string(e.Key) != "" {
					seen[string(e.Key)] = true
				}
			}
		case <-positionChan:
		case <-deadline:
			missing := []string{}
			for i := 0; i < rowCount; i++ {
				if !seen[fmt.Sprintf("%d", i)] {
					missing = append(missing, fmt.Sprintf("%d", i))
				}
			}
			t.Fatalf("timed out with %d of %d rows; missing keys %v — the keyset scan skipped them (ORDER BY alias capture)", len(seen), rowCount, missing)
		}
	}
	for i := 0; i < rowCount; i++ {
		require.Truef(t, seen[fmt.Sprintf("%d", i)], "missing row %d", i)
	}
}
