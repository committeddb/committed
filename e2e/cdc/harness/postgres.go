//go:build docker

package harness

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	pgDB   = "harness"
	pgUser = "postgres"
	pgPass = "secret"
)

// startPostgres boots a Postgres container configured for logical
// replication. wal_level=logical, plus generous slot/sender headroom
// because each test creates one slot+publication per TPC-H table (8
// slots minimum) and we leave room for resume tests to layer more on
// top without bumping the container limit.
func startPostgres(t *testing.T) (*tcpostgres.PostgresContainer, string) {
	t.Helper()
	ctx := context.Background()

	c, err := tcpostgres.Run(ctx,
		"postgres:16",
		tcpostgres.WithDatabase(pgDB),
		tcpostgres.WithUsername(pgUser),
		tcpostgres.WithPassword(pgPass),
		tcpostgres.BasicWaitStrategies(),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Cmd: []string{
					"-c", "wal_level=logical",
					"-c", "max_replication_slots=20",
					"-c", "max_wal_senders=20",
				},
			},
		}),
	)
	require.NoError(t, err, "start postgres container")

	connStr, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err, "postgres connection string")

	return c, connStr
}

// applySchema runs each statement in the TPC-H DDL against Postgres.
// Splits on `;` because pgx exec runs one statement at a time when the
// query contains no parameter placeholders — multi-statement strings
// silently execute only the first.
func applySchema(ctx context.Context, conn *pgx.Conn, statements []string) error {
	for i, s := range statements {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("apply schema statement %d: %w", i, err)
		}
	}
	return nil
}
