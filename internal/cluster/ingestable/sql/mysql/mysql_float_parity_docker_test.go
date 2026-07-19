//go:build docker || integration

package mysql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestMysqlSnapshotStreamFloatParity pins snapshot↔CDC byte-parity for top-level
// FLOAT/DOUBLE columns at magnitude extremes and at the decimal↔scientific format
// boundaries — coverage the basic type matrix (DOUBLE 3.5 only) lacks, since 3.5
// renders identically no matter how each path formats floats.
//
// Both paths converge because go-sql-driver hands a FLOAT/DOUBLE back as a typed
// float32/float64 (not text), so the snapshot value flows through JSONValue's
// "already-typed scalar" branch and is json.Marshal'd exactly as the CDC path
// marshals go-mysql's decoded float — so both emit Go's float formatting
// (1e20 → "100000000000000000000", 1e21 → "1e+21", 1e-7 → "1e-7"), byte-for-byte.
// This guards that invariant: if the snapshot ever regressed to rendering a float
// as MySQL driver text (json.Number), the boundary rows below would diverge (Go
// spells exponents "1e+21"/"1e-7"; MySQL text would not), catching it.
func TestMysqlSnapshotStreamFloatParity(t *testing.T) {
	rows := []struct {
		name string
		ddl  string
		val  string
	}{
		{"c_d_big", "DOUBLE", "1e20"},
		{"c_d_small", "DOUBLE", "1e-7"},
		{"c_d_max", "DOUBLE", "1.7976931348623157e308"},
		{"c_d_prec", "DOUBLE", "0.30000000000000004"},
		{"c_d_mid", "DOUBLE", "12345678901234.567"},
		{"c_d_norm", "DOUBLE", "3.5"}, // a normal-range value: must stay identical
		// Boundary probes: Go's json.Marshal switches decimal<->scientific at
		// |x| >= 1e21 and |x| < 1e-6. If MySQL's text rendering flips at a
		// different point, the snapshot (text) and CDC (Go float) diverge there.
		{"c_d_1e21", "DOUBLE", "1e21"},                        // Go scientific "1e+21"
		{"c_d_just_under", "DOUBLE", "999999999999999999999"}, // just under 1e21, Go decimal
		{"c_d_1e6neg", "DOUBLE", "0.000001"},                  // Go decimal "0.000001" (1e-6)
		{"c_d_1e5neg", "DOUBLE", "0.00001"},                   // Go decimal "0.00001"
		{"c_d_huge", "DOUBLE", "1.5e300"},                     // far scientific
		{"c_d_neg", "DOUBLE", "-2.5e-8"},                      // negative + scientific
		{"c_f_big", "FLOAT", "1e20"},
		{"c_f_small", "FLOAT", "1.5e-30"},
	}

	table := "float_parity_table"
	colDefs := make([]string, 0, len(rows))
	insertCols := make([]string, 0, len(rows))
	insertVals := make([]string, 0, len(rows))
	mappings := []sql.Mapping{{JsonName: "pk", SQLColumn: "pk"}}
	for _, r := range rows {
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", r.name, r.ddl))
		insertCols = append(insertCols, r.name)
		insertVals = append(insertVals, r.val)
		mappings = append(mappings, sql.Mapping{JsonName: r.name, SQLColumn: r.name})
	}
	ddl := fmt.Sprintf("CREATE TABLE `%s` (pk VARCHAR(32) PRIMARY KEY, %s)", table, strings.Join(colDefs, ", "))

	snap, cdc := runSnapshotThenCDC(t, table, ddl, mappings,
		strings.Join(insertCols, ", "), strings.Join(insertVals, ", "))

	// Log the whole matrix first so one run shows every field, then assert.
	for _, r := range rows {
		t.Logf("%-10s %-7s val=%-24s | snap=%-28s cdc=%s",
			r.name, r.ddl, r.val, rawJSONField(t, snap, r.name), rawJSONField(t, cdc, r.name))
	}
	for _, r := range rows {
		s := rawJSONField(t, snap, r.name)
		d := rawJSONField(t, cdc, r.name)
		assert.Equalf(t, s, d, "snapshot==CDC parity broken for %s (%s val=%s)", r.name, r.ddl, r.val)
	}
}
