package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/stretchr/testify/require"
)

func TestIngestable(t *testing.T) {
	cfg1 := createIngestableConfiguration("foo")
	cfg2 := createIngestableConfiguration("bar")

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

			ingestables := make(map[string]*clusterfakes.FakeIngestable)
			ingestables["foo"] = &clusterfakes.FakeIngestable{}
			ingestables["bar"] = &clusterfakes.FakeIngestable{}

			fooIngestableParser := &clusterfakes.FakeIngestableParser{}
			fooIngestableParser.ParseReturns(ingestables["foo"], nil)
			p.AddIngestableParser("foo", fooIngestableParser)

			barIngestableParser := &clusterfakes.FakeIngestableParser{}
			barIngestableParser.ParseReturns(ingestables["bar"], nil)
			p.AddIngestableParser("bar", barIngestableParser)

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			insertIngestables(t, s, tt.cfgs, currentIndex, currentTerm)

			ctgs, err := s.Ingestables()
			require.Equal(t, nil, err)
			for _, expected := range tt.cfgs {
				require.Contains(t, ctgs, expected)
			}

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			ctgs, err = s.Ingestables()
			require.Equal(t, nil, err)
			for _, expected := range tt.cfgs {
				require.Contains(t, ctgs, expected)
			}
		})
	}
}

func TestEmptyIngestables(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	is, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 0, len(is))
}

func insertIngestables(t *testing.T, s db.Storage, ts []*cluster.Configuration, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertIngestableEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}

func createIngestableConfiguration(name string) *cluster.Configuration {
	d := &IngestableConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}
