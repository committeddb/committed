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

// These probes are a discovery batch: they assert the decode behavior I predict
// for each type, run on both engines where the type exists, and let CI confirm
// or refute each prediction in one pass. A red here is a measurement, not a
// regression — read the oracle diff: a divergence from the stated prediction is
// either a real canal decode finding (file it) or a wrong prediction (correct
// the expectation). Each test names its hypothesis so triage is mechanical.

// wideRow is the tmwide post-image. flag is the one column whose JSON type
// diverges by engine, so it is passed in: a Go bool for Postgres (BOOLEAN →
// JSON bool) and a json.Number("1") for MySQL (TINYINT(1) → JSON number). The
// remaining columns are identical across engines. tmw_note is nil to exercise
// SQL NULL → JSON null. tmw_date / tmw_ts are bound as strings and rely on the
// driver casting text to DATE/DATETIME — itself part of what this measures.
func wideRow(flag any) map[string]any {
	return map[string]any{
		"tmw_id":   1,
		"tmw_flag": flag,
		"tmw_date": "2024-01-15",
		"tmw_ts":   "2024-01-15 12:30:45",
		"tmw_note": nil,
	}
}

// runTypeMatrixWide drives the bool/date/datetime/NULL group. The expected
// stream is derived from the row, so the per-engine flag value also sets the
// per-engine expectation: Postgres asserts tmw_flag → true, MySQL → 1.
//
// Predictions: tmw_date → "2024-01-15" and tmw_ts → "2024-01-15 12:30:45" on
// both (pgoutput and canal both render the column as that text); tmw_note →
// null on both. Likely divergence points: MySQL DATETIME may carry fractional
// seconds, or canal may format either temporal differently — that surfaces here.
func runTypeMatrixWide(t *testing.T, engine harness.Engine, flag any) {
	t.Helper()
	h := harness.NewWith(t, engine, harness.Options{Tables: []string{"tmwide"}})

	s := mutation.NewScript()
	s.Insert("tmwide", wideRow(flag))

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestTypeMatrixWide (Postgres): tmw_flag is a BOOLEAN, expected JSON bool true.
func TestTypeMatrixWide(t *testing.T) {
	runTypeMatrixWide(t, harness.PostgresEngine(), true)
}

// TestMySQL_TypeMatrixWide: tmw_flag is a TINYINT(1), expected JSON number 1 —
// the canonical PG-vs-MySQL boolean divergence, here pinned as an explicit
// contract rather than left implicit.
func TestMySQL_TypeMatrixWide(t *testing.T) {
	runTypeMatrixWide(t, harness.MySQLEngine(), json.Number("1"))
}

// TestMySQL_TypeMatrixUnsigned probes BIGINT UNSIGNED at max uint64
// (18446744073709551615) — past int64, so the decode must carry it as uint64,
// not clamp to int64. CI-confirmed green: canal returns uint64 and the hardened
// oracle keeps every digit. (No Postgres analogue — Postgres has no unsigned.)
func TestMySQL_TypeMatrixUnsigned(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"tmunsigned"}})

	s := mutation.NewScript()
	s.Insert("tmunsigned", map[string]any{
		"tmu_id":  1,
		"tmu_big": json.Number("18446744073709551615"),
	})

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMySQL_TypeMatrixEnum asserts ENUM decodes to its label ("green"), matching
// the snapshot path. Regression lock for finding mysql-cdc-enum-set-index-decode:
// canal hands back the 1-based ordinal index, and the ingest now resolves it to
// the label rather than leaking the bare number. MySQL-only — Postgres has no
// native ENUM in this schema.
func TestMySQL_TypeMatrixEnum(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"tmenum"}})

	s := mutation.NewScript()
	s.Insert("tmenum", map[string]any{"tme_id": 1, "tme_color": "green"})

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestMySQL_TypeMatrixSet asserts SET decodes to its comma-joined labels in
// definition order ("a,c"), matching the snapshot path — the SET half of finding
// mysql-cdc-enum-set-index-decode: canal hands back a member bitmask, and the
// ingest now resolves it to labels rather than leaking the integer. MySQL-only —
// Postgres has no SET type.
func TestMySQL_TypeMatrixSet(t *testing.T) {
	h := harness.NewWith(t, harness.MySQLEngine(), harness.Options{Tables: []string{"tmset"}})

	s := mutation.NewScript()
	s.Insert("tmset", map[string]any{"tms_id": 1, "tms_tags": "a,c"})

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// runTypeMatrixComposite probes a composite primary key (tmcomp PK (tmc_a,
// tmc_b)). committed's ingestable takes a single primaryKey column, so the
// harness keys the entity by the leftmost column (tmc_a) and carries the full
// identity in the Data JSON. Prediction: one proposal keyed by tmc_a, Data
// {tmc_a, tmc_b, tmc_v}. Composite-PK ingestion is otherwise unexercised even on
// Postgres, so this is a real first probe of that path on both engines.
func runTypeMatrixComposite(t *testing.T, engine harness.Engine) {
	t.Helper()
	h := harness.NewWith(t, engine, harness.Options{Tables: []string{"tmcomp"}})

	s := mutation.NewScript()
	s.Insert("tmcomp", map[string]any{"tmc_a": 1, "tmc_b": 2, "tmc_v": "x"})

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestTypeMatrixComposite runs the composite-PK probe on Postgres.
func TestTypeMatrixComposite(t *testing.T) { runTypeMatrixComposite(t, harness.PostgresEngine()) }

// TestMySQL_TypeMatrixComposite runs the composite-PK probe on MySQL.
func TestMySQL_TypeMatrixComposite(t *testing.T) { runTypeMatrixComposite(t, harness.MySQLEngine()) }
