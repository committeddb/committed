//go:build docker

package cdc_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/committeddb/committed/e2e/cdc/harness"
	"github.com/committeddb/committed/e2e/cdc/mutation"
	"github.com/committeddb/committed/e2e/cdc/oracle"
)

// TestMySQL_SimpleInsert runs the simplest scenario against the MySQL engine —
// the first exercise of mysqlEngine through the shared scenario/oracle framework
// (the standalone MySQLHarness tests assert via the sink; this asserts on the
// proposal stream like the Postgres scenarios do).
//
// With no pre-existing data, one INSERT on the MySQL source must produce exactly
// one Proposal on the region topic whose Entity matches the inserted row. This
// validates two things at once: that the engine wiring (container, schema,
// ingestable config, readiness gate) is correct, and that MySQL's binlog decode
// types the payload the same way the oracle expects — int → JSON number, string
// → JSON string — for the simple region columns. A typing mismatch here is the
// first decode finding the safety net is meant to surface, not a regression.
func TestMySQL_SimpleInsert(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(99, "TESTLAND", "smoke-test"))

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}

	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMySQL_DecimalPrecision is the MySQL counterpart of TestDecimalPrecision —
// the first numeric-fidelity probe of the MySQL decode path, and the place a
// canal DECIMAL-scale difference would first show. It pins exact scale across
// both CDC paths: an INSERT of p_retailprice 1234.50 (trailing zero) then an
// UPDATE to 6789.00 (a whole number that DECIMAL(15,2) still stores with two
// places) must arrive as the JSON numbers 1234.50 and 6789.00, digits intact.
//
// On Postgres this exact pattern passes (pgoutput hands back the column text
// verbatim). Whether MySQL matches depends on how canal formats a decimal from
// the binlog — if it drops the scale (1234.5 / 6789) the oracle will flag the
// mismatch, and that is the first real decode finding the safety net is built to
// catch, not a regression. part is FK-free, so the insert needs no parent rows.
func TestMySQL_DecimalPrecision(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"part"}})

	s := mutation.NewScript()
	s.Insert("part", partRow(1, 7, json.Number("1234.50")))
	s.Update("part", partRow(1, 7, json.Number("6789.00")))

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}

	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}
