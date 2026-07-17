//go:build docker || integration

package mysql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// runSnapshotThenCDC snapshots the table (collecting row "a" plus a sentinel so
// we know the snapshot fully drained), then CDC-inserts row "b", and returns the
// emitted entity Data for each. The two must be byte-identical — that is the
// snapshot==CDC serialization invariant the whole effectively-once/dedup design
// rests on. Shared by the temporal-parity leads below (U1, U2).
func runSnapshotThenCDC(t *testing.T, table, ddl string, mappings []sql.Mapping, insertCols, insertVals string) (snapshotData, cdcData []byte) {
	t.Helper()

	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(ddl)
	require.NoError(t, err)
	for _, pk := range []string{"a", "__sentinel__"} {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, %s) VALUES ('%s', %s)", table, insertCols, pk, insertVals))
		require.NoError(t, err)
	}
	db.Close()

	config := &sql.Config{
		Type:             &cluster.Type{ID: table, Name: table},
		Mappings:         mappings,
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposalChan := make(chan *cluster.Proposal, 10)
	positionChan := make(chan cluster.Position, 10)
	go func() { _ = (&mysql.MySQLDialect{}).Ingest(ctx, config, nil, 0, proposalChan, positionChan) }()

	seen := map[string][]byte{}
	drainUntil := func(pred func() bool, what string) {
		t.Helper()
		deadline := time.After(30 * time.Second)
		for !pred() {
			select {
			case p := <-proposalChan:
				for _, e := range p.Entities {
					seen[string(e.Key)] = e.Data
				}
			case <-positionChan:
			case <-deadline:
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}
	drainUntil(func() bool {
		_, a := seen["a"]
		_, s := seen["__sentinel__"]
		return a && s
	}, "snapshot")

	mdb := createDB(t)
	defer mdb.Close()
	_, err = mdb.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, %s) VALUES ('b', %s)", table, insertCols, insertVals))
	require.NoError(t, err)
	drainUntil(func() bool { _, ok := seen["b"]; return ok }, "CDC row b")

	return seen["a"], seen["b"]
}

func rawJSONField(t *testing.T, payload []byte, name string) string {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &m))
	return string(m[name])
}

// U1 — a DATE/TIME/DATETIME leaf INSIDE a JSON column, stored opaque (via CAST in
// JSON_OBJECT, not a text literal). The snapshot text-renders the JSON via the
// server; CDC decodes go-mysql's binary-JSON. This settles whether the two paths
// emit byte-identical bytes for embedded temporal scalars.
func TestMysqlSnapshotStreamJSONTemporalByteIdentity(t *testing.T) {
	table := "temporaljson_table"
	jexpr := "JSON_OBJECT(" +
		"'dt', CAST('2021-06-15 08:30:45.678901' AS DATETIME(6)), " +
		"'d', CAST('2021-06-15' AS DATE), " +
		"'t', CAST('08:30:45.123456' AS TIME(6)))"

	snap, cdc := runSnapshotThenCDC(t, table,
		fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, j JSON)", table),
		[]sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}, {JsonName: "j", SQLColumn: "j"}},
		"j", jexpr)

	t.Logf("snapshot j = %s", rawJSONField(t, snap, "j"))
	t.Logf("CDC      j = %s", rawJSONField(t, cdc, "j"))
	require.Equal(t, rawJSONField(t, snap, "j"), rawJSONField(t, cdc, "j"),
		"snapshot and CDC must emit byte-identical JSON for embedded temporal leaves")
}

// U2 — out-of-clock-range TIME values: negative, and >= 24:00:00 (up to the TIME
// type max 838:59:59). Three formatters are in play (server text, go-sql-driver,
// go-mysql binlog); this settles whether they agree at these boundary values.
func TestMysqlSnapshotStreamOutOfRangeTIMEByteIdentity(t *testing.T) {
	table := "oorange_time_table"
	cols := "tneg, tbig, tmax"
	vals := "'-100:30:45.500000', '25:00:00.000000', '838:59:59.000000'"

	snap, cdc := runSnapshotThenCDC(t, table,
		fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, tneg TIME(6), tbig TIME(6), tmax TIME(6))", table),
		[]sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "tneg", SQLColumn: "tneg"},
			{JsonName: "tbig", SQLColumn: "tbig"},
			{JsonName: "tmax", SQLColumn: "tmax"},
		},
		cols, vals)

	for _, f := range []string{"tneg", "tbig", "tmax"} {
		t.Logf("%s snapshot=%s cdc=%s", f, rawJSONField(t, snap, f), rawJSONField(t, cdc, f))
		require.Equal(t, rawJSONField(t, snap, f), rawJSONField(t, cdc, f),
			"snapshot and CDC must emit byte-identical bytes for out-of-range TIME %q", f)
	}
}
