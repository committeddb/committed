//go:build docker || integration

package mysql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// typeMatrixRow is one column in the golden type matrix: a SQL type, a literal to
// store, and the canonical JSON it must render to on BOTH paths.
type typeMatrixRow struct {
	name string // column + JSON field name
	ddl  string // SQL column type
	val  string // SQL literal inserted for every row
	want string // expected canonical raw JSON (snapshot == CDC == want)
}

// TestMysqlSnapshotStreamTypeMatrix is THE snapshot↔CDC parity forcing function
// for MySQL.
//
// CONTRACT: every MySQL type committed supports must appear as a row here. MySQL
// scans typed values via go-sql-driver on the snapshot path and reconciles each
// type against go-mysql's INDEPENDENT binlog decoder on the CDC path — unlike
// Postgres, which casts every column ::text so both paths share one output
// function. There is no shared decoder to make parity automatic, so this matrix
// is the only thing forcing a new or refactored type to stay byte-identical
// across the two paths. Adding a type without a row here is a silent gap: a
// divergence (snapshot text ≠ CDC bytes) corrupts data downstream and breaks the
// replay/dedup key with no test failure until it manifests in production.
//
// A row asserts snapshot bytes == CDC bytes == the canonical render. If a new row
// diverges, that is a real per-type bug — fix it (or file it and mark the row) —
// do not loosen the assertion.
//
// This matrix pins the BASIC per-type render. Edge cases beyond it have their own
// focused tests alongside this file, which this matrix does not replace: JSON
// with embedded decimal/bit/temporal leaves (TestMysqlSnapshotStreamJSON*),
// out-of-clock-range TIME (TestMysqlSnapshotStreamOutOfRangeTIMEByteIdentity),
// and the cross-charset sweep (TestMysqlCharsetSnapshotStreamParity_*).
func TestMysqlSnapshotStreamTypeMatrix(t *testing.T) {
	rows := []typeMatrixRow{
		{"c_int", "INT", "2147483647", `2147483647`},
		{"c_bigint", "BIGINT", "9223372036854775807", `9223372036854775807`},
		{"c_decimal", "DECIMAL(30,4)", "'123456789012345678.9012'", `123456789012345678.9012`},
		{"c_double", "DOUBLE", "3.5", `3.5`},
		{"c_bit", "BIT(8)", "b'10101010'", `170`},
		{"c_vc_utf8", "VARCHAR(64) CHARACTER SET utf8mb4", "'café'", `"café"`},
		{"c_vc_latin1", "VARCHAR(64) CHARACTER SET latin1", "'café'", `"café"`},
		{"c_char", "CHAR(8) CHARACTER SET utf8mb4", "'abc'", `"abc"`},
		{"c_text", "TEXT", "'hello world'", `"hello world"`},
		// BLOB is binary: committed renders it CatText (bytes→string), so non-UTF8
		// content becomes U+FFFD identically on both paths. Parity holds (both
		// paths produce the same bytes → replay/dedup is safe), but there is no
		// canonical UTF-8 for arbitrary bytes, so this row pins parity only, not a
		// render. (Binary-BLOB fidelity — e.g. base64 — is a separate concern.)
		{"c_blob", "BLOB", "x'DEADBEEF'", ``},
		{"c_enum", "ENUM('a','b','c')", "'b'", `"b"`},
		{"c_set", "SET('x','y','z')", "'x,z'", `"x,z"`},
		{"c_json", "JSON", `'{"z":1,"a":"x"}'`, `{"a":"x","z":1}`},
		{"c_date", "DATE", "'2021-06-15'", `"2021-06-15"`},
		{"c_datetime", "DATETIME(6)", "'2021-06-15 08:30:45.678901'", `"2021-06-15 08:30:45.678901"`},
		{"c_time", "TIME(6)", "'08:30:45.123456'", `"08:30:45.123456"`},
		{"c_time0", "TIME", "'08:30:45'", `"08:30:45"`},
		{"c_timestamp", "TIMESTAMP(6)", "'2021-06-15 08:30:45.678901'", `"2021-06-15 08:30:45.678901"`},
		{"c_year", "YEAR", "2021", `2021`},
	}

	table := "type_matrix_table"
	colDefs := make([]string, 0, len(rows))
	insertCols := make([]string, 0, len(rows))
	insertVals := make([]string, 0, len(rows))
	mappings := make([]sql.Mapping, 0, len(rows)+1)
	mappings = append(mappings, sql.Mapping{JsonName: "pk", SQLColumn: "pk"})
	for _, r := range rows {
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", r.name, r.ddl))
		insertCols = append(insertCols, r.name)
		insertVals = append(insertVals, r.val)
		mappings = append(mappings, sql.Mapping{JsonName: r.name, SQLColumn: r.name})
	}
	ddl := fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, %s)", table, strings.Join(colDefs, ", "))

	snap, cdc := runSnapshotThenCDC(t, table, ddl, mappings,
		strings.Join(insertCols, ", "), strings.Join(insertVals, ", "))

	// Log every field first so one run shows the whole matrix, then assert (assert,
	// not require, so every divergence is reported in a single run).
	for _, r := range rows {
		t.Logf("%-13s %-34s | snap=%-30s cdc=%-30s want=%s",
			r.name, r.ddl, rawJSONField(t, snap, r.name), rawJSONField(t, cdc, r.name), r.want)
	}
	for _, r := range rows {
		s := rawJSONField(t, snap, r.name)
		d := rawJSONField(t, cdc, r.name)
		assert.Equalf(t, s, d, "snapshot==CDC parity broken for %s (%s)", r.name, r.ddl)
		if r.want != "" {
			assert.Equalf(t, r.want, d, "canonical render changed for %s (%s)", r.name, r.ddl)
		}
	}
}
