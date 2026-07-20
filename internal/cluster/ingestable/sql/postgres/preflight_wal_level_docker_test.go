//go:build docker || integration

package postgres_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/postgres"
)

// TestPostgresPreflightWalLevel pins the wal_level=logical preflight gate.
// wal_level requires a server RESTART to change, making it the prerequisite a
// first-time setup most often misses; without the gate the misconfig surfaced
// only at worker start (reconnect loop → supervisor giveup → frozen ingest),
// not as a clean 400 at POST time. The reject path runs against a genuine
// default-config container (wal_level=replica) — no simulation — so the test
// fails if the gate is ever removed.
func TestPostgresPreflightWalLevel(t *testing.T) {
	table := "pf_wal"

	mkConfig := func(cs string) *sql.Config {
		return &sql.Config{
			Type:             &cluster.Type{ID: table, Name: table},
			Mappings:         []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "val", SQLColumn: "val"}},
			PrimaryKey:       []string{"pk"},
			ConnectionString: cs,
			Tables:           []string{table},
			Options:          map[string]string{"slot_name": "slot_pfwal", "publication": "pub_pfwal"},
		}
	}

	// Happy path: the suite's shared container runs wal_level=logical.
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	db.Close()
	require.NoError(t, (&postgres.PostgreSQLDialect{}).Preflight(mkConfig(connString)),
		"preflight must pass on a wal_level=logical server")

	// Reject path: a stock container without the wal_level flag (defaults to
	// "replica"). Only wal_level differs — the same table exists there too, so
	// nothing else can fail first.
	ctx := context.Background()
	stock, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase(dbName),
		tcpostgres.WithUsername(username),
		tcpostgres.WithPassword(password),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	defer func() { _ = testcontainers.TerminateContainer(stock) }()

	stockCS, err := stock.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	sdb, err := gosql.Open("pgx", stockCS)
	require.NoError(t, err)
	_, err = sdb.Exec(fmt.Sprintf(`CREATE TABLE %s (pk VARCHAR(32) NOT NULL PRIMARY KEY, val TEXT)`, table))
	require.NoError(t, err)
	sdb.Close()

	err = (&postgres.PostgreSQLDialect{}).Preflight(mkConfig(stockCS))
	require.Error(t, err, "a non-logical wal_level must be rejected at preflight, not discovered as a frozen worker")
	require.Contains(t, err.Error(), "wal_level")
	require.Contains(t, err.Error(), "logical")
}
