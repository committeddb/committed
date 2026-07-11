package migration_test

import (
	"errors"
	"fmt"
	"testing"

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

	got, err := migration.Chain(r, "person", 3, 3, data)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestChain_SingleStep(t *testing.T) {
	// v1 -> v2: add a default "email" field.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "unknown@example.com"}`)},
	}}

	got, err := migration.Chain(r, "person", 1, 2, []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","email":"unknown@example.com"}`, string(got))
}

func TestChain_MultipleSteps(t *testing.T) {
	// v1 -> v2: add email. v2 -> v3: rename email to contact.
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`. + {email: "unknown@example.com"}`)},
		"person@3": {ID: "person", Version: 3, Migration: []byte(`{name, contact: .email}`)},
	}}

	got, err := migration.Chain(r, "person", 1, 3, []byte(`{"name":"alice"}`))
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

	got, err := migration.Chain(r, "person", 1, 3, []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"full_name":"alice"}`, string(got))
}

func TestChain_BadJQProgram(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`this is not jq`)},
	}}

	_, err := migration.Chain(r, "person", 1, 2, []byte(`{"name":"alice"}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "v1->v2")
}

func TestChain_BadInputJSON(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{
		"person@2": {ID: "person", Version: 2, Migration: []byte(`.`)},
	}}

	_, err := migration.Chain(r, "person", 1, 2, []byte(`not json`))
	require.Error(t, err)
}

func TestChain_ResolverError(t *testing.T) {
	r := &stubResolver{types: map[string]*cluster.Type{}} // no types

	_, err := migration.Chain(r, "missing", 1, 2, []byte(`{}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "fetch type")
}

func TestCompile_ValidProgram(t *testing.T) {
	require.NoError(t, migration.Compile([]byte(`. + {email: "x"}`)))
}

func TestCompile_InvalidProgram(t *testing.T) {
	require.Error(t, migration.Compile([]byte(`this is not jq`)))
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

	_, err := migration.Chain(r, "person", 1, 3, []byte(`{"name":"alice"}`))
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
	out, err := migration.Run([]byte(`. + {email: "x"}`), []byte(`{"name":"alice"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"alice","email":"x"}`, string(out))

	_, err = migration.Run([]byte(`error("boom")`), []byte(`{"name":"alice"}`))
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
		out, err := migration.Run([]byte(`.`), []byte(`{"id":`+bigID+`}`))
		require.NoError(t, err)
		require.Contains(t, string(out), bigID, "the >2^53 integer must survive byte-exact")
		require.NotContains(t, string(out), rounded, "float64 rounding must not occur")
	})

	t.Run("field restructure", func(t *testing.T) {
		out, err := migration.Run([]byte(`{account: .id}`), []byte(`{"id":`+bigID+`}`))
		require.NoError(t, err)
		require.Contains(t, string(out), bigID, "the >2^53 integer must survive a restructuring migration")
		require.NotContains(t, string(out), rounded)
	})
}
