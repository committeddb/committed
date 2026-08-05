//go:build docker

package cdc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

// TestMySQL_DeleteCapture is the MySQL counterpart of TestDeleteCapture: a
// binlog DELETE arrives as a delete tombstone keyed by PK.
func TestMySQL_DeleteCapture(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"region"}})

	preDelete := regionRow(42, "DELETED_ROW", "to-be-deleted")
	s := mutation.NewScript()
	s.Insert("region", preDelete)
	s.Delete("region", preDelete) // only the PK is load-bearing for a delete

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMySQL_PrimaryKeyUpdate is the MySQL counterpart of TestPrimaryKeyUpdate:
// a PK-changing UPDATE must arrive as new-row + delete-tombstone-at-old-key in
// ONE proposal (the binlog UpdateRows event carries both images).
func TestMySQL_PrimaryKeyUpdate(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "ORIGINAL", "before-pk-change"))
	s.Txn(func(t *mutation.Txn) {
		t.Exec("UPDATE region SET r_regionkey=?, r_name=?, r_comment=? WHERE r_regionkey=?",
			2, "RENAMED", "after-pk-change", 1)
	})
	expected := mutation.NewScript()
	expected.Insert("region", regionRow(1, "ORIGINAL", "before-pk-change"))
	expected.Txn(func(t *mutation.Txn) {
		t.Update("region", regionRow(2, "RENAMED", "after-pk-change"))
		t.Delete("region", regionRow(1, "ORIGINAL", "before-pk-change"))
	})

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, expected.Expected(), h.Capture(t, expected.ExpectedCounts()))
}

// TestMySQL_TransactionAtomicity is the MySQL counterpart of
// TestTransactionAtomicity: rows written inside an open transaction must not
// leak through CDC before COMMIT (the binlog only carries committed
// transactions), then land together as one proposal. The mid-transaction
// assertion runs inside the SourceTxn callback — the transaction is open for
// its duration and commits when it returns.
func TestMySQL_TransactionAtomicity(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"region"}})
	ctx := context.Background()

	require.NoError(t, h.SourceTxn(ctx, func(q mutation.Querier) error {
		if err := q.Exec(ctx, "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (1, 'A', 'a')"); err != nil {
			return err
		}
		if err := q.Exec(ctx, "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (2, 'B', 'b')"); err != nil {
			return err
		}
		// Hold the txn open; confirm nothing leaked through CDC.
		time.Sleep(500 * time.Millisecond)
		mid := h.Capture(t, map[string]int{"region": 0})
		if len(mid["region"]) != 0 {
			return fmt.Errorf("uncommitted transaction leaked %d proposals", len(mid["region"]))
		}
		return nil // commit
	}), "source transaction")

	// After commit, both rows arrive in one proposal.
	expected := mutation.NewScript()
	expected.Txn(func(t *mutation.Txn) {
		t.Insert("region", regionRow(1, "A", "a"))
		t.Insert("region", regionRow(2, "B", "b"))
	})
	oracle.Assert(t, expected.Expected(), h.Capture(t, expected.ExpectedCounts()))
}

// TestMySQL_RestartResume is the MySQL counterpart of TestRestartResume: a
// committed restart resumes from the persisted GTID position — phase-1 rows
// are NOT re-emitted (which a re-snapshot would do) and phase-2 rows arrive.
func TestMySQL_RestartResume(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"region"}})

	pre := mutation.NewScript()
	pre.Insert("region", regionRow(1, "BEFORE_A", "phase1-a"))
	pre.Insert("region", regionRow(2, "BEFORE_B", "phase1-b"))
	require.NoError(t, h.RunScript(context.Background(), pre), "phase 1 run")
	oracle.Assert(t, pre.Expected(), h.Capture(t, pre.ExpectedCounts()))

	h.RestartCommitted(t)

	post := mutation.NewScript()
	post.Insert("region", regionRow(3, "AFTER_A", "phase2-a"))
	post.Insert("region", regionRow(4, "AFTER_B", "phase2-b"))
	require.NoError(t, h.RunScript(context.Background(), post), "phase 2 run")
	oracle.Assert(t, post.Expected(), h.Capture(t, post.ExpectedCounts()))
}
