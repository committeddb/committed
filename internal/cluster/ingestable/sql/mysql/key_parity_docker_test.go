//go:build docker || integration

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

// TestMysqlSnapshotStreamKeyParity pins that a row's entity KEY is the same
// text on the snapshot path (go-sql-driver scan) and the binlog path
// (go-mysql decode), for every column type a primary key can be. The type
// matrix pins payload bytes with every row keyed by text; this pins the key.
// A divergence keys a snapshotted row's later update and delete to a
// different entity, so a keyed destination keeps the stale row.
//
// Each type gets its own table holding one row: snapshot it, then UPDATE a
// non-key column and DELETE the row through the binlog. The upsert and the
// tombstone must both carry the snapshot's key.
func TestMysqlSnapshotStreamKeyParity(t *testing.T) {
	cases := []struct {
		name string // table suffix
		ddl  string // key column type
		val  string // SQL literal for the key
	}{
		{"int", "INT", "-42"},
		{"tinyint", "TINYINT", "-5"},
		{"tinyint1", "TINYINT(1)", "1"},
		{"smallint", "SMALLINT", "-300"},
		{"mediumint", "MEDIUMINT", "-70000"},
		{"bigint", "BIGINT", "-9223372036854775808"},
		{"bigint_u", "BIGINT UNSIGNED", "18446744073709551615"},
		{"int_u", "INT UNSIGNED", "4294967295"},
		{"tinyint_u", "TINYINT UNSIGNED", "200"},
		{"decimal", "DECIMAL(12,4)", "'-12.5000'"},
		{"decimal0", "DECIMAL(10,0)", "1234567890"},
		{"float", "FLOAT", "1.5"},
		{"double", "DOUBLE", "3.25"},
		{"bit", "BIT(8)", "b'10101010'"},
		{"bit1", "BIT(1)", "b'1'"},
		{"char", "CHAR(8)", "'abc'"},
		{"char_latin1", "CHAR(8) CHARACTER SET latin1", "'café'"},
		{"varchar", "VARCHAR(32)", "'café'"},
		{"binary", "BINARY(4)", "x'DEADBEEF'"},
		{"binary_pad", "BINARY(6)", "x'4142'"},
		{"varbinary", "VARBINARY(8)", "x'00FF41'"},
		{"enum", "ENUM('a','b','c')", "'b'"},
		{"set", "SET('x','y','z')", "'x,z'"},
		{"date", "DATE", "'2021-06-15'"},
		{"datetime", "DATETIME", "'2021-06-15 08:30:45'"},
		{"datetime6", "DATETIME(6)", "'2021-06-15 08:30:45.678900'"},
		{"datetime3", "DATETIME(3)", "'2021-06-15 08:30:45.100'"},
		{"timestamp", "TIMESTAMP", "'2021-06-15 08:30:45'"},
		{"timestamp6", "TIMESTAMP(6)", "'2021-06-15 08:30:45.678901'"},
		{"time", "TIME", "'-08:30:45'"},
		{"time6", "TIME(6)", "'08:30:45.123400'"},
		{"year", "YEAR", "2021"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snap, upd, del := mysqlKeyTrip(t, "kp_"+c.name, c.ddl, c.val)
			t.Logf("%-14s %-30s snap=%q update=%q delete=%q", c.name, c.ddl, snap, upd, del)
			require.Equal(t, snap, upd, "the binlog upsert must carry the snapshot's key")
			require.Equal(t, snap, del, "the binlog tombstone must carry the snapshot's key")
		})
	}
}

// mysqlKeyTrip snapshots a one-row table keyed by a column of type ddl, then
// updates and deletes that row through the binlog, returning the three keys.
func mysqlKeyTrip(t *testing.T, table, ddl, val string) (snap, upd, del string) {
	t.Helper()
	db := createDB(t)
	defer db.Close()
	exec := func(q string) {
		t.Helper()
		_, err := db.Exec(q)
		require.NoError(t, err)
	}
	exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	exec(fmt.Sprintf("CREATE TABLE `%s` (k %s NOT NULL PRIMARY KEY, v INT)", table, ddl))
	exec(fmt.Sprintf("INSERT INTO `%s` (k, v) VALUES (%s, 1)", table, val))

	config := &sql.Config{
		Type:             &cluster.Type{ID: table, Name: table},
		Mappings:         []sql.Mapping{{JsonName: "k", SQLColumn: "k"}, {JsonName: "v", SQLColumn: "v"}},
		PrimaryKey:       []string{"k"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pr := make(chan *cluster.Proposal, 16)
	po := make(chan cluster.Position, 16)
	go func() { _ = (&mysql.MySQLDialect{}).Ingest(ctx, config, nil, 0, pr, po) }()

	next := func(what string) *cluster.Entity {
		t.Helper()
		deadline := time.After(30 * time.Second)
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
	exec(fmt.Sprintf("UPDATE `%s` SET v = 2", table))
	u := next("binlog update")
	require.False(t, u.IsDelete(), "the update arrives as an upsert")
	exec(fmt.Sprintf("DELETE FROM `%s`", table))
	d := next("binlog delete")
	require.True(t, d.IsDelete(), "the delete arrives as a tombstone")
	return string(s.Key), string(u.Key), string(d.Key)
}
