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

// typeMatrixRow is a fully-populated typematrix row. tm_big is a Go int64 (a
// plain integer, bound as-is), tm_dec a json.Number so an exact scale can be
// pinned, tm_txt an arbitrary string. All columns are NOT NULL.
func typeMatrixRow(id int, big int64, dec json.Number, txt string) map[string]any {
	return map[string]any{
		"tm_id":  id,
		"tm_big": big,
		"tm_dec": dec,
		"tm_txt": txt,
	}
}

// runTypeMatrixScalar drives the scalar decode-fidelity matrix against one
// engine. It is the same scenario for Postgres and MySQL — the point is that
// these types decode identically on both, so a single expected stream proves
// fidelity on each. The values are chosen to break a naive (float64) oracle:
//
//   - tm_big = max int64 (9223372036854775807): float64 can only hold integers
//     up to 2^53 exactly, so this rounds if a number ever passes through a
//     float. Both engines carry a signed BIGINT as int64, so the digits must
//     survive verbatim.
//   - tm_dec = a 20-significant-digit DECIMAL(20,4): beyond float64's ~16-digit
//     limit, with a trailing zero the scale must preserve. Extends the
//     DECIMAL(15,2) probe to a precision a float can't represent at all.
//   - tm_txt = multi-byte UTF-8 with quotes and an apostrophe: exercises the
//     string bind/decode path (and, on MySQL, the utf8mb4 round-trip) end to
//     end through the oracle.
//
// This is the payoff of the oracle's UseNumber hardening: under the old oracle
// tm_big and tm_dec would have collapsed to float64 and silently matched a
// mangled value. A divergence here (e.g. MySQL capping decimal precision) is a
// real decode finding, not a regression.
func runTypeMatrixScalar(t *testing.T, engine harness.Engine) {
	t.Helper()
	h := harness.NewWith(t, engine, harness.Options{Tables: []string{"typematrix"}})

	s := mutation.NewScript()
	s.Insert("typematrix", typeMatrixRow(
		1,
		9223372036854775807,
		json.Number("1234567890123456.7890"),
		`Ω≈ç√∫ 日本語 "quoted" O'Brien — ünïcödé`,
	))

	if err := h.RunScript(context.Background(), s); err != nil {
		t.Fatalf("script run: %v", err)
	}
	oracle.Assert(t, s.Expected(), h.Capture(t, s.ExpectedCounts()))
}

// TestTypeMatrixScalar runs the scalar fidelity matrix on Postgres.
func TestTypeMatrixScalar(t *testing.T) { runTypeMatrixScalar(t, harness.PostgresEngine()) }

// TestMySQL_TypeMatrixScalar runs the same matrix on MySQL. A failure here that
// Postgres passes points at a canal decode difference for big integers,
// high-precision decimals, or utf8mb4 strings.
func TestMySQL_TypeMatrixScalar(t *testing.T) { runTypeMatrixScalar(t, harness.MySQLEngine()) }
