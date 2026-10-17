//go:build docker || integration

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

// TestSQLServerSnapshotStreamKeyParity pins that a row's entity KEY is the
// same text on the snapshot path and the Change Tracking path, for every
// column type a primary key can be. A CT update's key comes from the joined
// current row; a CT delete's comes from CHANGETABLE's own key columns, the
// only image a deleted row leaves. A divergence keys a snapshotted row's later
// update or delete to a different entity, so a keyed destination keeps the
// stale row.
//
// Each type gets its own table holding one row: snapshot it, then UPDATE a
// non-key column and DELETE the row through Change Tracking. The upsert and
// the tombstone must both carry the snapshot's key.
func TestSQLServerSnapshotStreamKeyParity(t *testing.T) {
	cases := []struct {
		name string // table suffix
		ddl  string // key column type
		val  string // SQL literal for the key
	}{
		{"int", "INT", "-42"},
		{"bigint", "BIGINT", "-9223372036854775808"},
		{"smallint", "SMALLINT", "-300"},
		{"tinyint", "TINYINT", "200"},
		{"bit", "BIT", "1"},
		{"decimal", "DECIMAL(12,4)", "-12.5"},
		{"numeric0", "NUMERIC(10,0)", "1234567890"},
		{"money", "MONEY", "12.5"},
		{"smallmoney", "SMALLMONEY", "-3.25"},
		{"float", "FLOAT", "1.5"},
		{"real", "REAL", "3.25"},
		{"char", "CHAR(8)", "'abc'"},
		{"nchar", "NCHAR(8)", "N'café'"},
		{"varchar", "VARCHAR(32)", "'abc'"},
		{"nvarchar", "NVARCHAR(32)", "N'café'"},
		{"binary", "BINARY(4)", "0xDEADBEEF"},
		{"binary_pad", "BINARY(6)", "0x4142"},
		{"varbinary", "VARBINARY(8)", "0x00FF41"},
		{"date", "DATE", "'2021-06-15'"},
		{"datetime", "DATETIME", "'2021-06-15 08:30:45.677'"},
		{"smalldatetime", "SMALLDATETIME", "'2021-06-15 08:30:00'"},
		{"datetime2", "DATETIME2(7)", "'2021-06-15 08:30:45.6789012'"},
		{"datetime2_3", "DATETIME2(3)", "'2021-06-15 08:30:45.100'"},
		{"datetimeoffset", "DATETIMEOFFSET(7)", "'2021-06-15 08:30:45.6789012 -06:00'"},
		{"time", "TIME(7)", "'08:30:45.1234000'"},
		{"uniqueidentifier", "UNIQUEIDENTIFIER", "'3E11FA47-71CA-11E1-9E33-C80AA9429562'"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snap, upd, del := sqlserverKeyTrip(t, "kp_"+c.name, c.ddl, c.val)
			t.Logf("%-16s %-18s snap=%q update=%q delete=%q", c.name, c.ddl, snap, upd, del)
			require.Equal(t, snap, upd, "the CT upsert must carry the snapshot's key")
			require.Equal(t, snap, del, "the CT tombstone must carry the snapshot's key")
		})
	}
}

// sqlserverKeyTrip snapshots a one-row table keyed by a column of type ddl,
// then updates and deletes that row through Change Tracking, returning the
// three keys.
func sqlserverKeyTrip(t *testing.T, table, ddl, val string) (snap, upd, del string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	exec := func(q string) {
		t.Helper()
		_, err := db.Exec(q)
		require.NoError(t, err)
	}
	exec(fmt.Sprintf("DROP TABLE IF EXISTS dbo.%s", table))
	exec(fmt.Sprintf("CREATE TABLE dbo.%s (k %s NOT NULL PRIMARY KEY, v INT)", table, ddl))
	exec(fmt.Sprintf("INSERT INTO dbo.%s (k, v) VALUES (%s, 1)", table, val))

	config := &sql.Config{
		Type:             &cluster.Type{ID: table, Name: table},
		Mappings:         []sql.Mapping{{JsonName: "k", SQLColumn: "k"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"k"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
		Options:          sql.Options{PollInterval: 300 * time.Millisecond},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 16)
	po := make(chan cluster.Position, 16)
	go func() {
		_ = (&sqlserver.SQLServerDialect{Features: gate(true)}).Ingest(ctx, config, nil, 0, pr, po)
	}()

	next := func(what string) *cluster.Entity {
		t.Helper()
		deadline := time.After(2 * time.Minute)
		for {
			select {
			case p := <-pr:
				for _, e := range p.Entities {
					if !e.IsRefreshBoundary() {
						return e
					}
				}
			case <-po:
			case <-deadline:
				t.Fatalf("timed out waiting for the %s", what)
			}
		}
	}
	s := next("snapshot row")
	exec(fmt.Sprintf("UPDATE dbo.%s SET v = 2", table))
	u := next("CT update")
	require.False(t, u.IsDelete(), "the update arrives as an upsert")
	exec(fmt.Sprintf("DELETE FROM dbo.%s", table))
	d := next("CT delete")
	require.True(t, d.IsDelete(), "the delete arrives as a tombstone")
	return string(s.Key), string(u.Key), string(d.Key)
}
