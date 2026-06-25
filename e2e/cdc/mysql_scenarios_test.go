//go:build docker

package cdc_test

import (
	"context"
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
