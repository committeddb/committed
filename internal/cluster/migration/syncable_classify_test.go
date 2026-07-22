package migration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// failResolve is a Resolver whose ResolveType always fails — the type-unavailable
// case (a not-yet-replicated or a deleted topic type).
type failResolve struct{}

func (failResolve) ResolveType(cluster.TypeRef) (*cluster.Type, error) {
	return nil, errors.New("type not found")
}

// mustNotSync fails the test if the inner syncable is reached — a type-unavailable
// migration failure must short-circuit before inner.Sync.
type mustNotSync struct{ t *testing.T }

func (s mustNotSync) Sync(context.Context, *cluster.Actual) (cluster.ShouldSnapshot, error) {
	s.t.Errorf("inner.Sync must not be called when the migration fails to resolve the type")
	return false, nil
}
func (mustNotSync) Close() error { return nil }

// TestSyncable_TypeUnavailableIsTransient pins the #7 egress-classification fix:
// a migration failure caused by the topic's latest type not resolving fails
// EVERY entity of the topic identically (ResolveType's ref is keyed on the type
// ID, never the entity data) — schema/timing-shaped, not entry-specific. It must
// be TRANSIENT (wedge until the type is available), NOT cluster.Permanent
// (dead-letter). Before the fix migrateEntities' error was blanket-wrapped
// Permanent, so a not-yet-replicated or deleted type dead-lettered a whole
// topic's rows.
func TestSyncable_TypeUnavailableIsTransient(t *testing.T) {
	tp := &cluster.Type{ID: "person", Name: "Person", Version: 1}
	up := cluster.NewUpsertEntity(tp, []byte("k"), []byte(`{"name":"alice"}`))

	s := Wrap(mustNotSync{t}, failResolve{}, nil)
	_, err := s.Sync(context.Background(), &cluster.Actual{Index: 1, Entities: []*cluster.Entity{up}})

	require.Error(t, err)
	require.NotErrorIs(t, err, cluster.ErrPermanent,
		"a type-unavailable migration failure fails every entity of the topic → must be TRANSIENT (wedge), not dead-lettered")
}
