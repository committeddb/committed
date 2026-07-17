//go:build docker || integration

package mysql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// TestPreflightRejectsMappedSpatialColumn is the "keep our promises" guard:
// committed has no lossless representation for a MySQL spatial/VECTOR column on
// its binary CDC + snapshot paths, so Preflight rejects a config that MAPS one
// rather than silently corrupt it — while still accepting a table whose spatial
// column is simply not mapped.
func TestPreflightRejectsMappedSpatialColumn(t *testing.T) {
	table := "spatial_preflight"
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, name VARCHAR(64), g POINT)", table))
	require.NoError(t, err)
	db.Close()

	cfg := func(mappings []sql.Mapping) *sql.Config {
		return &sql.Config{
			Type:             &cluster.Type{ID: table, Name: table},
			Mappings:         mappings,
			PrimaryKey:       []string{"pk"},
			ConnectionString: ingestURL,
			Tables:           []string{table},
		}
	}

	// Mapping the POINT column is rejected loudly, naming the offending column.
	err = (&mysql.MySQLDialect{}).Preflight(cfg([]sql.Mapping{
		{JsonName: "pk", SQLColumn: "pk"},
		{JsonName: "g", SQLColumn: "g"},
	}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "g (point)")
	require.Contains(t, err.Error(), "spatial")

	// Not mapping it (ingesting only pk + name) passes — the spatial column is
	// read but never rendered into a payload or key, so it is left alone.
	err = (&mysql.MySQLDialect{}).Preflight(cfg([]sql.Mapping{
		{JsonName: "pk", SQLColumn: "pk"},
		{JsonName: "name", SQLColumn: "name"},
	}))
	require.NoError(t, err)
}
