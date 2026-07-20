//go:build docker || integration

package mysql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// TestMysqlPreflightBinlogFormat pins the binlog_format=ROW hard gate: under
// STATEMENT (or MIXED's statement-chosen paths) DML arrives as statement text,
// not row events — committed's row-CDC silently captures nothing while the
// snapshot works (silent source/mirror divergence), and the statement text
// embeds row values that must never reach the query-event log path. Preflight
// must reject the config loudly instead of part-working. Restores the global on
// the way out — the container is shared.
func TestMysqlPreflightBinlogFormat(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	mk := func(q string) { _, err := db.Exec(q); require.NoError(t, err) }
	mk("DROP TABLE IF EXISTS pf_fmt")
	mk("CREATE TABLE pf_fmt (id VARCHAR(32) PRIMARY KEY, v TEXT)")

	var orig string
	require.NoError(t, db.QueryRow("SELECT @@global.binlog_format").Scan(&orig))
	mk("SET GLOBAL binlog_format = 'STATEMENT'")
	defer func() { _, _ = db.Exec("SET GLOBAL binlog_format = '" + orig + "'") }()

	cfg := &sql.Config{
		Type:             &cluster.Type{ID: "pf_fmt", Name: "pf_fmt"},
		Mappings:         []sql.Mapping{{JsonName: "id", SQLColumn: "id"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"id"},
		ConnectionString: ingestURL,
		Tables:           []string{"pf_fmt"},
	}

	err := (&mysql.MySQLDialect{}).Preflight(cfg)
	require.Error(t, err, "a non-ROW binlog_format must be rejected at preflight")
	require.Contains(t, err.Error(), "binlog_format")
	require.Contains(t, err.Error(), "ROW")

	mk("SET GLOBAL binlog_format = '" + orig + "'")
	require.NoError(t, (&mysql.MySQLDialect{}).Preflight(cfg), "with ROW restored the same config passes")
}
