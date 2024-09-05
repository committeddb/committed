package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"
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
				got, err := s.Type(expected.ID)
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			for _, expected := range tt.types {
				got, err := s.Type(expected.ID)
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
				got, err := s.Type(expected.ID)
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			for i, tipe := range tt.types {
				e := cluster.NewDeleteTypeEntity(tipe.ID)
				saveEntity(t, e, s, term, index+uint64(i))

				_, err := s.Type(tipe.ID)
				require.Equal(t, wal.ErrTypeMissing, err)

				// Make sure other types are still available
				for j := i + 1; j < len(tt.types); j++ {
					got, err := s.Type(tt.types[j].ID)
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

	_, err := s.Type("none")
	require.Equal(t, wal.ErrTypeMissing, err)
}

func insertTypes(t *testing.T, s db.Storage, ts []*cluster.Type, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertTypeEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}
