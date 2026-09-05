//go:build docker

package sqlserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/sqlserver"
)

// TestSQLServerTypeMatrixParity runs the dedup-parity contract across the
// common column-type breadth: for every type, the CT-path payload of a
// same-value rewrite must be byte-identical to the snapshot payload (one read
// path, one decode path — the property the CT design leans on). It also pins
// the category mapping: bit → JSON bool, the numeric family → exact unquoted
// numbers (decimal/money keeping source precision), binary → base64, and the
// temporal family → strings.
func TestSQLServerTypeMatrixParity(t *testing.T) {
	db := createDB(t)
	defer db.Close()
	_, err := db.Exec(`DROP TABLE IF EXISTS dbo.ct_types`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE dbo.ct_types (
		pk INT NOT NULL PRIMARY KEY,
		c_bit BIT, c_tiny TINYINT, c_small SMALLINT, c_big BIGINT,
		c_dec DECIMAL(12,4), c_float FLOAT, c_real REAL, c_money MONEY,
		c_char NCHAR(4), c_var NVARCHAR(50), c_date DATE, c_dt2 DATETIME2(3),
		c_dto DATETIMEOFFSET(2), c_time TIME(1), c_bin VARBINARY(16),
		c_guid UNIQUEIDENTIFIER)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO dbo.ct_types VALUES (
		1, 1, 255, -32768, 9223372036854775807,
		12345678.9900, 1.5, 2.5, 922337.1234,
		N'abcd', N'text with ''quote''', '2026-08-06', '2026-08-06 12:34:56.789',
		'2026-08-06 12:34:56.78 +02:00', '23:59:59.9', 0xDEADBEEF,
		'3E11FA47-71CA-11E1-9E33-C80AA9429562')`)
	require.NoError(t, err)

	typ := &cluster.Type{ID: "ct-types", Name: "ct-types"}
	cols := []string{
		"pk", "c_bit", "c_tiny", "c_small", "c_big", "c_dec", "c_float", "c_real",
		"c_money", "c_char", "c_var", "c_date", "c_dt2", "c_dto", "c_time", "c_bin", "c_guid",
	}
	mappings := make([]sql.Mapping, len(cols))
	for i, c := range cols {
		mappings[i] = sql.Mapping{JsonName: c, SQLColumn: c}
	}
	config := &sql.Config{
		Type:             typ,
		Mappings:         mappings,
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{"ct_types"},
		Options:          map[string]string{"poll_interval": "300ms"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 64)
	po := make(chan cluster.Position, 64)
	d := &sqlserver.SQLServerDialect{}
	go func() { _ = d.Ingest(ctx, config, nil, 0, pr, po) }()

	snap := drainEntities(t, pr, po, 1, 2*time.Minute)
	snapData := string(snap[0].Data)

	// Category spot-pins on the snapshot payload.
	require.Contains(t, snapData, `"c_bit":true`, "bit renders as JSON bool")
	require.Contains(t, snapData, `"c_dec":12345678.9900`, "decimal keeps source precision, unquoted")
	require.Contains(t, snapData, `"c_money":922337.1234`, "money renders as an exact number")
	require.Contains(t, snapData, `"c_big":9223372036854775807`, "bigint exact at int64 max")
	require.Contains(t, snapData, `"c_bin":"3q2+7w=="`, "varbinary renders base64 (0xDEADBEEF)")
	require.Contains(t, snapData, `"c_guid":"3e11fa47-71ca-11e1-9e33-c80aa9429562"`, "guid canonical RFC 4122 lowercase (the PostgreSQL uuid spelling)")
	require.Contains(t, snapData, `"c_var":"text with 'quote'"`, "text with embedded quote survives")

	// The parity rewrite: update the row to its exact same values (touch one
	// column to itself — CT records the row changed; the join re-reads all).
	_, err = db.Exec("UPDATE dbo.ct_types SET c_var = c_var WHERE pk = 1")
	require.NoError(t, err)

	live := drainEntities(t, pr, po, 1, 2*time.Minute)
	require.Equal(t, snapData, string(live[0].Data),
		"the CT-path payload must be byte-identical to the snapshot payload across every column type")
}
