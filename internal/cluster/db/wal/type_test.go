package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/wal"
)

func TestType(t *testing.T) {
	tests := []struct {
		types []*cluster.Type
	}{
		{[]*cluster.Type{{ID: "foo", Name: "foo"}}},
		{[]*cluster.Type{{ID: "foo", Name: "foo"}, {ID: "bar", Name: "bar"}}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, index(3).terms(3, 4, 5))
			defer s.Cleanup()

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			insertTypes(t, s, tt.types, currentIndex, currentTerm)

			for _, expected := range tt.types {
				got, err := s.ResolveType(cluster.LatestTypeRef(expected.ID))
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			for _, expected := range tt.types {
				got, err := s.ResolveType(cluster.LatestTypeRef(expected.ID))
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}
		})
	}
}

func TestTypeDelete(t *testing.T) {
	tests := []struct {
		types []*cluster.Type
	}{
		{[]*cluster.Type{{ID: "foo", Name: "foo"}}},
		{[]*cluster.Type{{ID: "foo", Name: "foo"}, {ID: "bar", Name: "bar"}}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, index(3).terms(3, 4, 5))
			defer s.Cleanup()

			term := uint64(6)
			index := insertTypes(t, s, tt.types, term, uint64(6))

			for _, expected := range tt.types {
				got, err := s.ResolveType(cluster.LatestTypeRef(expected.ID))
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			for i, tipe := range tt.types {
				e := cluster.NewDeleteTypeEntity(tipe.ID)
				saveEntity(t, e, s, term, index+uint64(i))

				_, err := s.ResolveType(cluster.LatestTypeRef(tipe.ID))
				require.ErrorIs(t, err, wal.ErrTypeMissing)

				// Make sure other types are still available
				for j := i + 1; j < len(tt.types); j++ {
					got, err := s.ResolveType(cluster.LatestTypeRef(tt.types[j].ID))
					require.Equal(t, nil, err)
					require.Equal(t, tt.types[j], got)
				}
			}
		})
	}
}

func TestTypeError(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	_, err := s.ResolveType(cluster.LatestTypeRef("none"))
	require.ErrorIs(t, err, wal.ErrTypeMissing)
}

// TypeAtVersion fetches a specific historical version of a type with
// full schema metadata, even when newer versions exist. This is what the
// proposal apply path uses to pair an entity with the schema in force at
// propose time.
func TestTypeAtVersion(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	v1 := &cluster.Type{ID: "person", Name: "Person", Version: 1, SchemaType: "JSONSchema", Schema: []byte(`{"v":1}`)}
	v2 := &cluster.Type{ID: "person", Name: "Person", Version: 2, SchemaType: "JSONSchema", Schema: []byte(`{"v":2}`)}

	// Apply two upserts as separate Raft entries so each lands as its own
	// versioned bbolt entry.
	insertTypes(t, s, []*cluster.Type{v1}, 6, 6)
	insertTypes(t, s, []*cluster.Type{v2}, 6, 7)

	got1, err := s.ResolveType(cluster.TypeRefAt("person", int(1)))
	require.Nil(t, err)
	require.Equal(t, v1, got1)

	got2, err := s.ResolveType(cluster.TypeRefAt("person", int(2)))
	require.Nil(t, err)
	require.Equal(t, v2, got2)

	// Latest matches v2.
	latest, err := s.ResolveType(cluster.LatestTypeRef("person"))
	require.Nil(t, err)
	require.Equal(t, v2, latest)
}

func TestTypeAtVersion_NotFound(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	insertTypes(t, s, []*cluster.Type{{ID: "person", Name: "Person", Version: 1}}, 6, 6)

	_, err := s.ResolveType(cluster.TypeRefAt("person", int(99)))
	require.ErrorIs(t, err, cluster.ErrVersionNotFound)

	_, err = s.ResolveType(cluster.TypeRefAt("missing", int(1)))
	require.ErrorIs(t, err, cluster.ErrResourceNotFound)
}

func insertTypes(t *testing.T, s db.Storage, ts []*cluster.Type, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertTypeEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}
