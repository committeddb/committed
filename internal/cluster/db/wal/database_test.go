package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
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

			cfgs, err := s.Databases()
			require.Equal(t, nil, err)
			require.Equal(t, len(tt.cfgs), len(cfgs))
			for _, expected := range tt.cfgs {
				var got *cluster.Configuration
				for _, cfg := range cfgs {
					if expected.ID == cfg.ID {
						got = cfg
					}
				}
				require.NotNil(t, got)
				require.Equal(t, expected.Data, got.Data)
				require.Equal(t, expected.MimeType, got.MimeType)
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

// TestDatabaseRestore_ParserOrdering pins the ordering contract that
// cmd/node.go must follow: loadDatabases runs INSIDE wal.Open, so the
// database sub-parser must be registered on the Parser BEFORE Open. Register
// it afterward and a restarted node silently fails to rebuild its database
// handles — which in turn breaks RestoreSyncableWorkers, because a syncable's
// ParseSyncable resolves its sink via storage.Database.
//
// TestDatabase doesn't catch this: it reuses one Parser object across its
// close/reopen, so the parser is already populated when it reopens. A real
// restart builds a fresh parser.New(), so this test mirrors that — opening
// each incarnation with its own parser.
func TestDatabaseRestore_ParserOrdering(t *testing.T) {
	dir := t.TempDir()
	openOpts := []wal.Option{wal.WithoutFsync()}
	cfg := createDatabaseConfiguration("sink") // id "sink", dialect/type "sink"

	// Incarnation 1: parser registered, Open, apply the database config.
	p1 := parser.New()
	fp1 := &clusterfakes.FakeDatabaseParser{}
	fp1.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p1.AddDatabaseParser("sink", fp1)
	s1, err := wal.Open(dir, p1, nil, nil, openOpts...)
	require.NoError(t, err)
	insertDatabases(t, s1, []*cluster.Configuration{cfg}, 1, 1)
	_, err = s1.Database("sink")
	require.NoError(t, err, "database resolvable after the apply path builds it")
	require.NoError(t, s1.Close())

	// Incarnation 2a — the footgun: register the parser AFTER Open. Open's
	// loadDatabases already ran against an empty parser, so the database is
	// NOT rebuilt; the later registration is too late.
	pLate := parser.New()
	sLate, err := wal.Open(dir, pLate, nil, nil, openOpts...)
	require.NoError(t, err)
	fpLate := &clusterfakes.FakeDatabaseParser{}
	fpLate.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	pLate.AddDatabaseParser("sink", fpLate)
	_, err = sLate.Database("sink")
	require.ErrorIs(t, err, wal.ErrDatabaseMissing,
		"registering the db parser AFTER Open must miss the database on restart")
	require.NoError(t, sLate.Close())

	// Incarnation 2b — the contract: register the parser BEFORE Open and the
	// database is rebuilt on restart.
	pEarly := parser.New()
	fpEarly := &clusterfakes.FakeDatabaseParser{}
	fpEarly.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	pEarly.AddDatabaseParser("sink", fpEarly)
	sEarly, err := wal.Open(dir, pEarly, nil, nil, openOpts...)
	require.NoError(t, err)
	defer sEarly.Close()
	_, err = sEarly.Database("sink")
	require.NoError(t, err,
		"registering the db parser BEFORE Open rebuilds the database on restart")
}

func insertDatabases(t *testing.T, s db.Storage, ts []*cluster.Configuration, term, index uint64) uint64 {
	for i, tipe := range ts {
		e, err := cluster.NewUpsertDatabaseEntity(tipe)
		require.Equal(t, nil, err)

		saveEntity(t, e, s, term, index+uint64(i))
	}

	return index + uint64(len(ts))
}
