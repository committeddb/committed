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

// TestMysqlSnapshotStreamJSONDecimalByteIdentity is the snapshot==CDC invariant
// for a JSON column that carries MySQL *opaque DECIMAL* leaves — the class the
// text-literal JSON test (TestMysqlSnapshotStreamJSONBitByteIdentity) does not
// reach, because a JSON *text* number is stored as DOUBLE. Here the doc is built
// with JSON_OBJECT over decimal-typed SQL expressions, so the leaves are stored
// as opaque DECIMAL:
//
//   - snapshot renders MySQL's exact text and (before the fix) float-rounds it,
//     so a value past 2^53 is corrupted (9007199254740993.5 -> 9007199254740994)
//     and a trailing zero is lost (1.50 -> 1.5);
//   - CDC (go-mysql) renders a decimal leaf as a QUOTED string ("1.50").
//
// Both are wrong and they disagree, breaking the byte-compare replay/dedup
// relies on. The fix makes both emit the decimal as an exact, unquoted number.
func TestMysqlSnapshotStreamJSONDecimalByteIdentity(t *testing.T) {
	table := "decimaljson_table"
	db := createDB(t)
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, j JSON)", table))
	require.NoError(t, err)

	// Opaque DECIMAL leaves (d: past 2^53, small: trailing zero) alongside real
	// DOUBLE leaves (dbl, whole) — the whole-valued double pins that the existing,
	// working double normalization is preserved.
	jexpr := "JSON_OBJECT(" +
		"'d', CAST('9007199254740993.5' AS DECIMAL(30,1)), " +
		"'small', CAST('1.50' AS DECIMAL(5,2)), " +
		"'dbl', CAST(1.5 AS DOUBLE), " +
		"'whole', CAST(100 AS DOUBLE))"
	for _, pk := range []string{"a", "__sentinel__"} {
		_, err = db.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, j) VALUES ('%s', %s)", table, pk, jexpr))
		require.NoError(t, err)
	}
	db.Close()

	config := &sql.Config{
		Type: &cluster.Type{ID: "decimaljson", Name: "decimaljson"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "j", SQLColumn: "j"},
		},
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
	_, err = mdb.Exec(fmt.Sprintf("INSERT INTO `%s` (pk, j) VALUES ('b', %s)", table, jexpr))
	require.NoError(t, err)
	drainUntil(func() bool { _, ok := seen["b"]; return ok }, "CDC row b")

	rawField := func(payload []byte, name string) string {
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(payload, &m))
		return string(m[name])
	}
	t.Logf("snapshot j = %s", rawField(seen["a"], "j"))
	t.Logf("CDC      j = %s", rawField(seen["b"], "j"))

	require.Equal(t, rawField(seen["a"], "j"), rawField(seen["b"], "j"),
		"snapshot and CDC must emit byte-identical JSON for opaque-decimal leaves")

	// The canonical form both paths must converge on: keys sorted; decimals exact
	// (incl. past 2^53) as unquoted numbers; doubles still normalized (100.0->100).
	require.Equal(t,
		`{"d":9007199254740993.5,"dbl":1.5,"small":1.50,"whole":100}`,
		rawField(seen["a"], "j"),
		"JSON decimals exact + unquoted, doubles normalized, keys sorted")
}
