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

// drainCharsetRows runs an ingest of table and collects entity payloads by key
// until every wantKey is seen or it times out. Shared by the charset parity
// tests: they snapshot some rows, then INSERT one more to force a CDC row, and
// compare the two renders byte-for-byte.
func drainCharsetRows(t *testing.T, table string, config *sql.Config, insertAfter func(), snapshotKeys, cdcKeys []string) map[string][]byte {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proposalChan := make(chan *cluster.Proposal, 16)
	positionChan := make(chan cluster.Position, 16)
	go func() { _ = (&mysql.MySQLDialect{}).Ingest(ctx, config, nil, 0, proposalChan, positionChan) }()

	seen := map[string][]byte{}
	drainUntil := func(keys []string, what string) {
		t.Helper()
		deadline := time.After(30 * time.Second)
		have := func() bool {
			for _, k := range keys {
				if _, ok := seen[k]; !ok {
					return false
				}
			}
			return true
		}
		for !have() {
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

	drainUntil(snapshotKeys, "snapshot")
	insertAfter()
	drainUntil(cdcKeys, "CDC")
	return seen
}

// fieldsExceptPK decodes a payload to string fields and drops the primary key, so
// a snapshot row and a CDC row (which differ only in their pk) can be compared on
// the value fields that must render identically.
func fieldsExceptPK(t *testing.T, payload []byte) map[string]string {
	t.Helper()
	var m map[string]string
	require.NoError(t, json.Unmarshal(payload, &m))
	delete(m, "pk")
	return m
}

// TestMysqlCharsetSnapshotStreamParity_Latin1 is the A4a regression: a latin1
// CHAR/VARCHAR/TEXT value, a non-ASCII ENUM label, and a non-ASCII SET label all
// carry bytes (é = 0xE9) that are invalid UTF-8 on the CDC path unless
// transcoded. Before the fix the snapshot rendered correct UTF-8 ("café") while
// CDC rendered "caf�" (silent corruption + a key mismatch); after the fix
// both render "café", byte-identically.
func TestMysqlCharsetSnapshotStreamParity_Latin1(t *testing.T) {
	table := "charset_latin1_table"
	db := createDB(t)
	mk := func(q string) { _, err := db.Exec(q); require.NoError(t, err) }
	mk(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
	mk(fmt.Sprintf("CREATE TABLE `%s` ("+
		"pk VARCHAR(32) PRIMARY KEY, "+
		"vc VARCHAR(64) CHARACTER SET latin1, "+
		"tx TEXT CHARACTER SET latin1, "+
		"en ENUM('café','thé') CHARACTER SET latin1, "+
		"st SET('café','thé') CHARACTER SET latin1)", table))

	ins := func(pk string) string {
		return fmt.Sprintf("INSERT INTO `%s` (pk, vc, tx, en, st) VALUES ('%s', 'café', 'résumé', 'café', 'café,thé')", table, pk)
	}
	mk(ins("a"))
	mk(ins("__sentinel__"))
	db.Close()

	config := &sql.Config{
		Type: &cluster.Type{ID: "charset_latin1", Name: "charset_latin1"},
		Mappings: []sql.Mapping{
			{JsonName: "pk", SQLColumn: "pk"},
			{JsonName: "vc", SQLColumn: "vc"},
			{JsonName: "tx", SQLColumn: "tx"},
			{JsonName: "en", SQLColumn: "en"},
			{JsonName: "st", SQLColumn: "st"},
		},
		PrimaryKey:       []string{"pk"},
		ConnectionString: ingestURL,
		Tables:           []string{table},
	}

	seen := drainCharsetRows(t, table, config,
		func() {
			mdb := createDB(t)
			defer mdb.Close()
			_, err := mdb.Exec(ins("b"))
			require.NoError(t, err)
		},
		[]string{"a", "__sentinel__"}, []string{"b"})

	t.Logf("snapshot = %s", seen["a"])
	t.Logf("CDC      = %s", seen["b"])

	snap := fieldsExceptPK(t, seen["a"])
	cdc := fieldsExceptPK(t, seen["b"])
	require.Equal(t, snap, cdc, "snapshot and CDC must render every non-pk field identically for a latin1 row")

	require.Equal(t, "café", cdc["vc"], "latin1 VARCHAR is correct UTF-8, not U+FFFD")
	require.Equal(t, "résumé", cdc["tx"], "latin1 TEXT is correct UTF-8")
	require.Equal(t, "café", cdc["en"], "non-ASCII ENUM label is correct UTF-8")
	require.Equal(t, "café,thé", cdc["st"], "non-ASCII SET labels are correct UTF-8")
}

// TestMysqlCharsetSnapshotStreamParity_Charsets seeds the cross-charset forcing
// function: for each non-utf8mb4 single/multi-byte charset, a value representable
// in that charset must render byte-identically on the snapshot and CDC paths and
// equal the expected UTF-8. It is the pattern the full A4b type/charset matrix
// (see its ticket) extends.
func TestMysqlCharsetSnapshotStreamParity_Charsets(t *testing.T) {
	cases := []struct {
		name    string
		charset string
		value   string // the UTF-8 value to store (representable in charset)
	}{
		{"latin1_accents", "latin1", "café"},
		{"latin2_polish", "latin2", "łódź"},
		{"cp1251_cyrillic", "cp1251", "привет"},
		{"gbk_chinese", "gbk", "中文"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := "charset_" + tc.name
			db := createDB(t)
			mk := func(q string) { _, err := db.Exec(q); require.NoError(t, err) }
			mk(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table))
			mk(fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, v VARCHAR(64) CHARACTER SET %s)", table, tc.charset))
			ins := func(pk string) string {
				return fmt.Sprintf("INSERT INTO `%s` (pk, v) VALUES ('%s', '%s')", table, pk, tc.value)
			}
			mk(ins("a"))
			mk(ins("__sentinel__"))
			db.Close()

			config := &sql.Config{
				Type: &cluster.Type{ID: table, Name: table},
				Mappings: []sql.Mapping{
					{JsonName: "pk", SQLColumn: "pk"},
					{JsonName: "v", SQLColumn: "v"},
				},
				PrimaryKey:       []string{"pk"},
				ConnectionString: ingestURL,
				Tables:           []string{table},
			}

			seen := drainCharsetRows(t, table, config,
				func() {
					mdb := createDB(t)
					defer mdb.Close()
					_, err := mdb.Exec(ins("b"))
					require.NoError(t, err)
				},
				[]string{"a", "__sentinel__"}, []string{"b"})

			snap := fieldsExceptPK(t, seen["a"])
			cdc := fieldsExceptPK(t, seen["b"])
			require.Equal(t, snap, cdc,
				"snapshot and CDC must render the value identically for charset %s", tc.charset)
			require.Equal(t, tc.value, cdc["v"], "charset %s value is correct UTF-8", tc.charset)
		})
	}
}
