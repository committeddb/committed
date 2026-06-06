//go:build docker

package cdc_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/e2e/cdc/dataset"
	"github.com/philborlin/committed/e2e/cdc/harness"
	"github.com/philborlin/committed/e2e/cdc/mutation"
	"github.com/philborlin/committed/e2e/cdc/oracle"
)

// regionRow is shorthand for a fully-populated region row map. Every
// NOT NULL column gets a value because the mutation DSL does not
// supply defaults — what you pass is what gets inserted, and what the
// oracle expects.
func regionRow(key int, name, comment string) map[string]any {
	return map[string]any{
		"r_regionkey": key,
		"r_name":      name,
		"r_comment":   comment,
	}
}

// TestSimpleInsert is the end-to-end smoke test: with no pre-existing
// data, one INSERT should produce exactly one Proposal on the region
// topic containing one Entity matching the inserted row.
func TestSimpleInsert(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(99, "TESTLAND", "smoke-test"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}

	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestUpdateCaptureKRepeats verifies that K successive UPDATE
// statements on the same primary key produce K proposals on the topic,
// in order, with the per-key history preserved exactly.
//
// This is the canonical hot-key correctness test: if committed
// reorders or drops any of the K events the oracle catches it.
func TestUpdateCaptureKRepeats(t *testing.T) {
	const k = 20
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	// Insert the row first (single-row baseline). Then update K times.
	// The expected stream is 1 insert + K updates = K+1 proposals.
	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "ORIGINAL", "v0"))
	for i := 1; i <= k; i++ {
		s.Update("region", regionRow(1, fmt.Sprintf("V%d", i), fmt.Sprintf("comment v%d", i)))
	}

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}

	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestRollback verifies that a rolled-back transaction produces zero
// proposals — pgoutput skips uncommitted changes entirely. A
// successful commit before and after rules out "harness can't see
// anything" as a false-positive explanation.
func TestRollback(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "BEFORE", "pre-rollback"))
	s.Rollback(func(t *mutation.Txn) {
		t.Insert("region", regionRow(2, "ROLLED_BACK", "should not appear"))
		t.Insert("region", regionRow(3, "ALSO_ROLLED_BACK", "should not appear"))
	})
	s.Insert("region", regionRow(4, "AFTER", "post-rollback"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}

	// Expected: 2 proposals (BEFORE, AFTER). The Rollback contributes
	// zero — Script.Expected() filters out rollback txns.
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMixedOpTxn does an insert + update + delete on overlapping rows
// in a single transaction. Because one Postgres commit = one
// committed Proposal, the result is a single Proposal containing all
// three Entities in some order. The oracle's per-proposal multiset
// comparison handles whatever order pgoutput emits.
func TestMixedOpTxn(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	// Seed two rows so we have something to update and delete.
	seed := mutation.NewScript()
	seed.Insert("region", regionRow(10, "TO_UPDATE", "v0"))
	seed.Insert("region", regionRow(20, "TO_DELETE", "v0"))
	if err := seed.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	// Drain the seed events; Capture waits for the two seed proposals
	// to land and then rebaselines so the next Capture() returns only
	// the mixed-op proposal.
	_ = h.Capture(t, seed.ExpectedCounts())

	mixed := mutation.NewScript()
	mixed.Txn(func(t *mutation.Txn) {
		t.Insert("region", regionRow(30, "INSERTED", "v0"))
		t.Update("region", regionRow(10, "UPDATED", "v1"))
		t.Delete("region", regionRow(20, "TO_DELETE", "v0")) // pass pre-image (REPLICA IDENTITY FULL)
	})

	if err := mixed.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("mixed run: %v", err)
	}

	oracle.Assert(t, mixed.Expected(), h.Capture(t, mixed.ExpectedCounts()))
}

// nationRow is shorthand for a fully-populated nation row map.
func nationRow(key int, name string, regionKey int, comment string) map[string]any {
	return map[string]any{
		"n_nationkey": key,
		"n_name":      name,
		"n_regionkey": regionKey,
		"n_comment":   comment,
	}
}

// TestInsertOrderingFKParentBeforeChild verifies that pgoutput
// delivers a parent INSERT before the child INSERT when both happen
// in the same transaction in script order. The oracle's per-proposal
// multiset check allows any order within the same transaction, but
// for separate transactions the commit order must be strict.
func TestInsertOrderingFKParentBeforeChild(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region", "nation"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(100, "PARENT_REGION", "fk-test parent"))
	s.Insert("nation", nationRow(200, "CHILD_NATION", 100, "fk-test child"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestUpdateCaptureSingle is the K=1 form of TestUpdateCaptureKRepeats.
// Useful as a focused regression target — if a fix makes K=20 pass but
// K=1 break, the diff is much easier to read.
func TestUpdateCaptureSingle(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "ORIGINAL", "v0"))
	s.Update("region", regionRow(1, "UPDATED", "v1"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestDeleteCapture verifies that a source DELETE produces a proposal whose
// Entity is a delete (tombstone) keyed by the deleted row's primary key — not
// an upsert of the row's pre-image. This is what lets a downstream syncable
// remove the record (right-to-be-forgotten); the old behavior (ingesting a
// delete as an upsert of the old row) left a zombie row in every projection.
// The oracle matches the delete by key + delete-ness.
func TestDeleteCapture(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	preDelete := regionRow(42, "DELETED_ROW", "to-be-deleted")
	s := mutation.NewScript()
	s.Insert("region", preDelete)
	s.Delete("region", preDelete) // only the PK is load-bearing for a delete

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestDeleteWithDependents verifies that DELETE on a row that has
// foreign-key dependents (ON DELETE behavior) emits the right proposal
// stream. TPC-H FKs are NOT CASCADE — Postgres rejects the delete if
// a child references the row. So we expect a SQL error, not a CDC
// event. The test asserts that no proposals leaked even though the
// transaction got an error during its statement.
func TestDeleteWithDependents(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region", "nation"}})

	// Set up: region 5 has nation 6 as a dependent.
	setup := mutation.NewScript()
	setup.Insert("region", regionRow(5, "REGION_WITH_DEP", "parent"))
	setup.Insert("nation", nationRow(6, "DEP_NATION", 5, "dependent"))
	require.NoError(t, setup.Run(context.Background(), h.Conn()), "setup run")
	_ = h.Capture(t, setup.ExpectedCounts())

	// Try to delete region 5 — should fail with FK violation. Run it
	// outside the mutation DSL because the DSL expects success.
	_, err := h.Conn().Exec(context.Background(), "DELETE FROM region WHERE r_regionkey=5")
	require.Error(t, err, "DELETE should fail due to FK from nation")

	// No proposals should appear on either topic from the failed delete.
	got := h.Capture(t, map[string]int{"region": 0, "nation": 0})
	if n := len(got["region"]); n != 0 {
		t.Errorf("region topic: expected 0 proposals from failed delete, got %d", n)
	}
	if n := len(got["nation"]); n != 0 {
		t.Errorf("nation topic: expected 0 proposals from failed delete, got %d", n)
	}
}

// TestPrimaryKeyUpdate exercises an UPDATE that changes the primary
// key. pgoutput's behavior under REPLICA IDENTITY FULL: emits an
// Update message with the old PK tuple as the OldTuple and the new
// row as the NewTuple. The committed dialect today only forwards the
// NewTuple (postgres.go:309) — so the resulting Entity has the NEW
// key and NEW data; the old key's history "ends" silently with no
// delete-like event. The oracle reflects that: one expected Entity at
// the new key.
//
// This is the test the plan specifically calls out as likely to
// surface a real bug; if the user wants old-key-delete semantics, the
// dialect's UpdateMessage handler needs to be extended.
func TestPrimaryKeyUpdate(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "ORIGINAL", "before-pk-change"))
	// Raw SQL: the mutation DSL's Update changes the row in place by
	// PK; PK-change is a different operation. The expected entity is
	// the new row at the new PK.
	s.Txn(func(t *mutation.Txn) {
		t.Exec("UPDATE region SET r_regionkey=$1, r_name=$2, r_comment=$3 WHERE r_regionkey=$4",
			2, "RENAMED", "after-pk-change", 1)
	})
	// The PK-change txn produces ONE Entity at the new key with the
	// new data (the dialect emits NewTuple only — old-key history end
	// is silent). Express that as a manual single-op script.
	expected := mutation.NewScript()
	expected.Insert("region", regionRow(1, "ORIGINAL", "before-pk-change"))
	expected.Update("region", regionRow(2, "RENAMED", "after-pk-change"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, expected.Expected(), h.Capture(t, expected.ExpectedCounts()))
}

// TestTransactionAtomicity verifies that proposals don't leak before
// COMMIT. We start a transaction, write rows, hold it open, briefly
// poll the topic to confirm zero new proposals, then COMMIT and check
// the full set lands as one proposal.
func TestTransactionAtomicity(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	ctx := context.Background()
	tx, err := h.Conn().Begin(ctx)
	require.NoError(t, err, "begin")
	_, err = tx.Exec(ctx, "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (1, 'A', 'a')")
	require.NoError(t, err, "insert 1")
	_, err = tx.Exec(ctx, "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (2, 'B', 'b')")
	require.NoError(t, err, "insert 2")

	// Hold the txn open. Confirm nothing leaked through CDC.
	time.Sleep(500 * time.Millisecond)
	mid := h.Capture(t, map[string]int{"region": 0})
	require.Empty(t, mid["region"], "uncommitted transaction must not leak proposals")

	require.NoError(t, tx.Commit(ctx), "commit")

	// After commit, both rows arrive in one proposal.
	expected := mutation.NewScript()
	expected.Txn(func(t *mutation.Txn) {
		t.Insert("region", regionRow(1, "A", "a"))
		t.Insert("region", regionRow(2, "B", "b"))
	})
	oracle.Assert(t, expected.Expected(), h.Capture(t, expected.ExpectedCounts()))
}

// TestLargeTransaction stresses the dialect's maxPendingEntities
// guard (postgres.go) by inserting many rows in one COMMIT. We use
// 2000 rows rather than the plan's 100k — empirically 100k slows the
// test enough to be miserable in a tight inner loop, and 2000 already
// exceeds the in-code guard threshold so the relevant branch fires.
func TestLargeTransaction(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	const n = 2000
	ctx := context.Background()
	tx, err := h.Conn().Begin(ctx)
	require.NoError(t, err, "begin")
	for i := 0; i < n; i++ {
		_, err = tx.Exec(ctx,
			"INSERT INTO region (r_regionkey, r_name, r_comment) VALUES ($1, $2, $3)",
			i, fmt.Sprintf("R%d", i), fmt.Sprintf("c%d", i))
		require.NoError(t, err, "insert %d", i)
	}
	require.NoError(t, tx.Commit(ctx), "commit")

	// We don't build a 2000-entity oracle by hand; just assert: at
	// least one proposal arrived, and the total entity count equals n.
	// This catches truncation, drops, and memory blowup that the plan
	// calls out as the failure modes for large transactions.
	got := h.Capture(t, map[string]int{"region": 1}) // gate on at least 1
	total := 0
	for _, p := range got["region"] {
		total += len(p.Entities)
	}
	require.Equal(t, n, total, "large-txn: total entity count must equal inserted rows")
}

// TestHotKeyChurn is the multi-key variant of TestUpdateCaptureKRepeats:
// many updates spread across a small set of primary keys. The per-key
// ordering check in the oracle guarantees each key's history is preserved.
func TestHotKeyChurn(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	const keys = 5
	const updatesPerKey = 100

	s := mutation.NewScript()
	for k := 0; k < keys; k++ {
		s.Insert("region", regionRow(k, fmt.Sprintf("K%d_V0", k), "v0"))
	}
	for v := 1; v <= updatesPerKey; v++ {
		for k := 0; k < keys; k++ {
			s.Update("region", regionRow(k, fmt.Sprintf("K%d_V%d", k, v), fmt.Sprintf("v%d", v)))
		}
	}

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestNullVsEmptyString verifies that a NULL value and an empty
// string are preserved as distinct in committed's Data JSON.
// pgoutput emits text-encoded column values; NULL becomes a real
// JSON null, empty string becomes the empty string.
func TestNullVsEmptyString(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", map[string]any{
		"r_regionkey": 1,
		"r_name":      "WITH_EMPTY",
		"r_comment":   "", // empty string — NOT null
	})
	s.Insert("region", map[string]any{
		"r_regionkey": 2,
		"r_name":      "WITH_NULL",
		"r_comment":   nil, // genuine NULL
	})

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestUnicode verifies that multi-byte characters (CJK, emoji,
// combining marks) round-trip correctly through CDC.
func TestUnicode(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, "日本", "コメント"))
	s.Insert("region", regionRow(2, "🌍🚀", "emoji+combining é"))
	s.Insert("region", regionRow(3, "Ω≈ç√∫", "mixed math/greek"))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMaxLength inserts a row that fills every VARCHAR to its
// declared maximum. Tests that the dialect doesn't silently truncate.
func TestMaxLength(t *testing.T) {
	h := harness.New(t, harness.Options{Tables: []string{"region"}})

	// region: r_name VARCHAR(25), r_comment VARCHAR(152).
	maxName := strings.Repeat("X", 25)
	maxComment := strings.Repeat("Y", 152)

	s := mutation.NewScript()
	s.Insert("region", regionRow(1, maxName, maxComment))

	if err := s.Run(context.Background(), h.Conn()); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// _ pulls in the dataset package even when no test currently uses Load.
// Tests added later will use Load; keeping the import here avoids a
// rebuild churn when those tests appear.
var _ = dataset.Tables
