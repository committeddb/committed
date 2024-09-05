package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"
)

func TestDatabase(t *testing.T) {
	cfg1 := createDatabaseConfiguration("foo")
	cfg2 := createDatabaseConfiguration("bar")

	tests := []struct {
		cfgs []*cluster.Configuration
	}{
		{[]*cluster.Configuration{}},
		{[]*cluster.Configuration{cfg1}},
		{[]*cluster.Configuration{cfg1, cfg2}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			var p db.Parser = parser.New()
			s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
			defer s.Cleanup()

			dbs := make(map[string]*clusterfakes.FakeDatabase)
			dbs["foo"] = &clusterfakes.FakeDatabase{}
			dbs["bar"] = &clusterfakes.FakeDatabase{}

			fooDatabaseParser := &clusterfakes.FakeDatabaseParser{}
			fooDatabaseParser.ParseReturns(dbs["foo"], nil)
			p.AddDatabaseParser("foo", fooDatabaseParser)

			barDatabaseParser := &clusterfakes.FakeDatabaseParser{}
			barDatabaseParser.ParseReturns(dbs["bar"], nil)
			p.AddDatabaseParser("bar", barDatabaseParser)

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			insertDatabases(t, s, tt.cfgs, currentIndex, currentTerm)

			for _, expected := range tt.cfgs {
				got, err := s.Database(expected.ID)
				require.Equal(t, nil, err)
				require.Equal(t, dbs[expected.ID], got)
			}

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			for _, expected := range tt.cfgs {
				got, err := s.Database(expected.ID)
				require.Equal(t, nil, err)
				require.Equal(t, dbs[expected.ID], got)
			}
		})
	}
}

func TestDatabaseError(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	_, err := s.Database("none")
	require.Equal(t, wal.ErrDatabaseMissing, err)
}

func insertDatabases(t *testing.T, s db.Storage, ts []*cluster.Configuration, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertDatabaseEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}
