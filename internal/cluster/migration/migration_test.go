package migration_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// stubResolver is a test Resolver. Version > 0 lookups are keyed as
// "id@version"; Version == 0 (LatestTypeRef) lookups are keyed by plain
// ID. Mirrors the production storage semantics closely enough to let
// migration and Syncable tests exercise both lookup shapes.
type stubResolver struct {
	types map[string]*cluster.Type
}

func (r *stubResolver) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	key := ref.ID
	if ref.Version > 0 {
		key = fmt.Sprintf("%s@%d", ref.ID, ref.Version)
	}
	t, ok := r.types[key]
	if !ok {
		return nil, fmt.Errorf("type %s not found", key)
	}
	return t, nil
}

func TestChain_NoUpgradeNeeded(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{}}
	data := []byte(`{"name":"alice"}`)

	got, err := migration.Chain(context.Background(), r, "person", 3, 3, data)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestChain_SingleStep(t *testing.T) {
	// v1 -> v2: add a default "email" field.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "unknown@example.com"}`)},
	}}

	got, err := migration.Chain(context.Background(), r, "person", 1, 2, []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","email":"unknown@example.com"}`, string(got))
}

func TestChain_MultipleSteps(t *testing.T) {
	// v1 -> v2: add email. v2 -> v3: rename email to contact.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "unknown@example.com"}`)},
		"person@3": {ID: "person", Version: 3, Migration: []byte(`{name, contact: .email}`)},
	}}

	got, err := migration.Chain(context.Background(), r, "person", 1, 3, []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","contact":"unknown@example.com"}`, string(got))
}

func TestChain_EmptyMigrationIsNoOp(t *testing.T) {
	// v2 has no migration (schema change was additive with nullable
	// field); v3 renames. The v1->v2 step passes through.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: nil},
		"person@3": {ID: "person", Version: 3, Migration: []byte(`{full_name: .name}`)},
	}}

	got, err := migration.Chain(context.Background(), r, "person", 1, 3, []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"full_name":"alice"}`, string(got))
}

func TestChain_BadJQProgram(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`this is not jq`)},
	}}

	_, err := migration.Chain(context.Background(), r, "person", 1, 2, []byte(`{"name":"alice"}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "v1->v2")
}

func TestChain_BadInputJSON(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`.`)},
	}}

	_, err := migration.Chain(context.Background(), r, "person", 1, 2, []byte(`not json`))
	require.Error(t, err)
}

func TestChain_ResolverError(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{}} // no types

	_, err := migration.Chain(context.Background(), r, "missing", 1, 2, []byte(`{}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch type")
}

func TestCompile_ValidProgram(t *testing.T) {
	require.NoError(t, migration.Compile([]byte(`. + {email: "x"}`)))
}

func TestCompile_InvalidProgram(t *testing.T) {
	require.Error(t, migration.Compile([]byte(`this is not jq`)))
}

// TestCompile_RejectsNondeterministicBuiltins is the determinism-sandbox
// regression: a migration runs at READ time (always-current sync, rebuild,
// dead-letter replay), so it must be a pure function of the entity. A program
// referencing a nondeterministic / unsandboxed builtin must be rejected at
// Compile (Type-registration) time, naming the offender, rather than silently
// drifting sink data or diverging per node later.
func TestCompile_RejectsNondeterministicBuiltins(t *testing.T) {
	for _, tc := range []struct {
		name, program, wantBuiltin string
	}{
		{"now assignment", `.ts = now`, "now"},
		{"now piped", `.ts = (now | floor)`, "now"},
		{"now nested in object", `{a: .a, b: {c: now}}`, "now"},
		{"localtime", `.t = (.epoch | localtime)`, "localtime"},
		{"gmtime", `.t = (.epoch | gmtime)`, "gmtime"},
		{"mktime", `.t = (.broken | mktime)`, "mktime"},
		{"strftime", `.t = (.epoch | strftime("%Y"))`, "strftime"},
		{"strflocaltime", `.t = (.epoch | strflocaltime("%Y"))`, "strflocaltime"},
		{"strptime", `.t = (.s | strptime("%Y"))`, "strptime"},
		{"date", `.t = (.epoch | date)`, "date"},
		{"loc", `.l = $__loc__`, "$__loc__"},
		{"env", `.e = env`, "env"},
		{"ENV var", `.e = $ENV`, "$ENV"},
		{"input", `. + input`, "input"},
		{"inputs", `[inputs]`, "inputs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := migration.Compile([]byte(tc.program))
			require.Error(t, err, "program must be rejected")
			require.Contains(t, err.Error(), tc.wantBuiltin, "error must name the offending builtin")
		})
	}
}

// TestCompile_AllowsDeterministicPrograms guards against the sandbox
// over-rejecting: pure programs — including ones with fields/strings that merely
// share a builtin's name, arithmetic, and big-int round-trips — must still compile.
func TestCompile_AllowsDeterministicPrograms(t *testing.T) {
	for _, program := range []string{
		`. + {email: "x"}`,
		`.a + .b`,
		`.now`,                      // field named "now", not the builtin
		`{ts: .now, note: "now"}`,   // field access + string literal
		`.id | tonumber + 1`,        // deterministic arithmetic
		`.big = 900719925474099267`, // big int
		`.items | map(select(.n > 0))`,
		`.t | fromdate`, // deterministic given input (not denied)
	} {
		require.NoError(t, migration.Compile([]byte(program)), program)
	}
}

// TestChain_RuntimeErrorIsStructured asserts a program failing at runtime
// surfaces as a *migration.Error naming the exact failing step — the
// identity the sync worker uses to dead-letter the failure against the
// type and count it in committed.type.migration.errors.
func TestChain_RuntimeErrorIsStructured(t *testing.T) {
	// v2 passes; v3's program errors at runtime on this payload.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "unknown@example.com"}`)},
		"person@3": {ID: "person", Version: 3, Migration: []byte(`error("cannot derive contact for " + .name)`)},
	}}

	_, err := migration.Chain(context.Background(), r, "person", 1, 3, []byte(`{"name":"alice"}`))
	require.Error(t, err)

	merr, ok := errors.AsType[*migration.Error](err)
	require.True(t, ok, "a runtime step failure must be a *migration.Error")
	require.Equal(t, "person", merr.TypeID)
	require.Equal(t, 2, merr.FromVersion, "the failing step starts at the version below the broken program")
	require.Equal(t, 3, merr.ToVersion, "the v3 program is the one that errored")
	require.Contains(t, merr.Error(), "v2->v3")
	require.Contains(t, merr.Error(), "cannot derive contact for alice")
}

// TestRun_SingleProgram covers the exported single-program entry point the
// ParseType pre-flight uses: a good sample transforms, a runtime failure
// reports the cause.
func TestRun_SingleProgram(t *testing.T) {
	out, err := migration.Run(context.Background(), []byte(`. + {email: "x"}`), []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","email":"x"}`, string(out))

	_, err = migration.Run(context.Background(), []byte(`error("boom")`), []byte(`{"name":"alice"}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
}

// TestRun_PreservesLargeIntegerPrecision is the migration-precision regression:
// Run decoded entity JSON through float64 (json.Unmarshal into any), silently
// rounding any integer above 2^53 — a >2^53 ID / money value / key corrupted by
// an ordinary migration, then persisted to the sink. The decode must preserve
// exact digits (json.Number) so gojq round-trips large integers losslessly.
//
// NOTE: assert on the raw digit string, NOT require.JSONEq — testify parses JSON
// numbers as float64, so JSONEq would treat ...807 and ...808 as equal and mask
// the very corruption under test.
func TestRun_PreservesLargeIntegerPrecision(t *testing.T) {
	// 9223372036854775807 = max int64 (a realistic BIGINT / snowflake id), far
	// above 2^53; float64 rounds it up to ...808.
	const bigID = "9223372036854775807"
	const rounded = "9223372036854775808"

	t.Run("identity passthrough", func(t *testing.T) {
		out, err := migration.Run(context.Background(), []byte(`.`), []byte(`{"id":`+bigID+`}`))
		require.NoError(t, err)
		require.Contains(t, string(out), bigID, "the >2^53 integer must survive byte-exact")
		require.NotContains(t, string(out), rounded, "float64 rounding must not occur")
	})

	t.Run("field restructure", func(t *testing.T) {
		out, err := migration.Run(context.Background(), []byte(`{account: .id}`), []byte(`{"id":`+bigID+`}`))
		require.NoError(t, err)
		require.Contains(t, string(out), bigID, "the >2^53 integer must survive a restructuring migration")
		require.NotContains(t, string(out), rounded)
	})
}

// TestRun_RespectsContextDeadline is the jq-migration-unbounded-execution
// regression: Run used gojq's non-context runner (q.Run), so an operator's
// non-terminating program ran unbounded on the caller's goroutine — wedging the
// sync worker (un-cancellable, defeating shutdown) or the propose handler. Run
// must honor ctx cancellation (and its own per-run timeout) so a pathological
// program returns an error instead of hanging.
func TestRun_RespectsContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		// `def f: f; f` recurses forever — gojq's non-context runner never returns.
		_, err := migration.Run(ctx, []byte(`def f: f; f`), []byte(`{}`))
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err, "a non-terminating program must return an error, not a value")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not honor the context deadline — a non-terminating program hung the goroutine")
	}
}
