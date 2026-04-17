package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
)

func TestSyncable(t *testing.T) {
	cfgd1 := createDatabaseConfiguration("foo")
	cfgd2 := createDatabaseConfiguration("bar")
	cfgs1 := createSyncableConfiguration("baz")
	cfgs2 := createSyncableConfiguration("qux")

	tests := []struct {
		cfgs []*cluster.Configuration
	}{
		{[]*cluster.Configuration{}},
		{[]*cluster.Configuration{cfgs1}},
		{[]*cluster.Configuration{cfgs1, cfgs2}},
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

			bazSyncable := &clusterfakes.FakeSyncable{}
			bazSyncableParser := &clusterfakes.FakeSyncableParser{}
			bazSyncableParser.ParseReturns(bazSyncable, nil)
			p.AddSyncableParser("baz", bazSyncableParser)

			quxSyncable := &clusterfakes.FakeSyncable{}
			quxSyncableParser := &clusterfakes.FakeSyncableParser{}
			quxSyncableParser.ParseReturns(quxSyncable, nil)
			p.AddSyncableParser("qux", quxSyncableParser)

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			// insertDatabases / insertSyncables have signature (term, index).
			// Pre-PR1 the call site swapped them and got away with it because
			// Save always re-applied: the syncable entries collided with the
			// database entries at indices 6,7 in the entry log, but the
			// database bucket already held foo/bar from the prior apply, and
			// the syncable bucket got populated by the syncable apply on the
			// same Save call. After PR1, apply is idempotent on entry index,
			// so a second apply at index 6/7 would skip — leaving the
			// syncable bucket empty. Fix: pass term and index in the right
			// slots, and start syncables AFTER the databases (index 8+).
			insertDatabases(t, s, []*cluster.Configuration{cfgd1, cfgd2}, currentTerm, currentIndex)
			insertSyncables(t, s, tt.cfgs, currentTerm, currentIndex+uint64(len(dbs)))

			cfgs, err := s.Syncables()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.cfgs), len(cfgs))
			require.ElementsMatch(t, tt.cfgs, cfgs)

			s = s.CloseAndReopenStorage(t)
			defer s.Cleanup()

			cfgs, err = s.Syncables()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.cfgs), len(cfgs))
			require.ElementsMatch(t, tt.cfgs, cfgs)
		})
	}
}

func createSyncableConfiguration(name string) *cluster.Configuration {
	d := &SyncableConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

func insertSyncables(t *testing.T, s db.Storage, ts []*cluster.Configuration, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertSyncableEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}
