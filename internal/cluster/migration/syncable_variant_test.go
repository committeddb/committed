package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// mustNotResolve fails the test if the migration path consults the resolver —
// the pass-through paths must decide from the entity alone.
type mustNotResolve struct{ t *testing.T }

func (r *mustNotResolve) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	r.t.Errorf("ResolveType(%v) called for a non-row entity; non-rows must pass through untouched", ref)
	return nil, errors.New("must not be called")
}

// TestMigrateEntities_NonRowVariantsPassThroughUntouched: only row data
// migrates. A delete carries the sentinel and a refresh-boundary marker
// carries no Data at all, so either would corrupt into a permanent error in
// the migration chain — for the marker that meant a dead-letter on any topic
// whose type gained a version after the marker was committed (the chain ran
// jq over nil Data). Both must pass through byte-identical, without even a
// type resolution.
func TestMigrateEntities_NonRowVariantsPassThroughUntouched(t *testing.T) {
	tp := &cluster.Type{ID: "topic", Name: "Topic", Version: 1}
	marker := cluster.NewRefreshBoundaryEntity(tp, 3)
	del := cluster.NewDeleteEntity(tp, []byte("k"))

	out, err := migrateEntities(context.Background(), &mustNotResolve{t}, nil, []*cluster.Entity{marker, del})
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Same(t, marker, out[0], "the marker must pass through untouched")
	require.Same(t, del, out[1], "the delete must pass through untouched")
}
