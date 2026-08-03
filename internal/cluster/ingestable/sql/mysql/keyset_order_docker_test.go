//go:build docker

package mysql_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// TestMysqlSnapshotIntegerPKKeysetOrder is the MySQL twin of the Postgres
// keyset ORDER BY alias-capture regression test. MySQL's readBatch is
// structurally immune TODAY — its row query is `SELECT *` and its phase-2
// JSON type query uses unaliased expressions, so ORDER BY has no output alias
// to capture — but that immunity is an accident of the current select lists:
// one future byte-parity edit that aliases a cast back to a column's own name
// (exactly how the Postgres bug was born) would reintroduce silent partial
// snapshots with a green status. Integer pks 0..24 at batch_size 10 are the
// discriminating shape: any text-ordered walk with a numeric cursor loses
// keys 2..9 forever. The pre-existing chunking test cannot catch this — its
// VARCHAR keys are zero-padded to uniform width, where text order and value
// order coincide.
func TestMysqlSnapshotIntegerPKKeysetOrder(t *testing.T) {
	table := "intpk_order_table"
	const rowCount = 25

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk INT NOT NULL, val TEXT, PRIMARY KEY (pk));", table))
	require.NoError(t, err)
	for i := 0; i < rowCount; i++ {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, val) VALUES (%d, 'v%d');", table, i, i))
		require.NoError(t, err)
	}
	db.Close()

	config := &sql.Config{
		Type: &cluster.Type{ID: "intpk-order", Name: "intpk-order"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "val", SQLColumn: "val"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
		Options:          map[string]string{"batch_size": "10"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 40)
	positionChan := make(chan cluster.Position, 40)
	dialect := &mysql.MySQLDialect{}
	ingestErr := make(chan error, 1)
	go func() {
		ingestErr <- dialect.Ingest(ctx, config, nil, 0, proposalChan, positionChan)
	}()

	deadline := time.After(15 * time.Second)
	seen := make(map[string]bool)
	for len(seen) < rowCount {
		select {
		case p := <-proposalChan:
			for _, e := range p.Entities {
				if !e.IsDelete() && e.Type != nil && e.Type.ID == "intpk-order" && len(e.Key) > 0 {
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
			t.Fatalf("timed out with %d of %d rows; missing keys %v — keyset scan skipped them", len(seen), rowCount, missing)
		}
	}
	for i := 0; i < rowCount; i++ {
		require.Truef(t, seen[fmt.Sprintf("%d", i)], "missing row %d", i)
	}
}
