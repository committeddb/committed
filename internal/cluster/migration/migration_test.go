package migration_test

import (
	"fmt"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/migration"
	"github.com/stretchr/testify/require"
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
