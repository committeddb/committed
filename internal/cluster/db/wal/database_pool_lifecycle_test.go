package wal_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestSaveDatabase_RePOSTPoolLifecycle pins the destination-pool lifecycle
// (ticket resource-teardown-lifecycle-close, A8a): a database re-POST must not
// leak the superseded *sql.DB connection pool. A syncable resolves
// storage.Database(id) once, at build time, and a database apply does not rebuild
// syncables — so a byte-identical re-POST must KEEP the existing pool (rebuilding
// would orphan it and leave the syncable pointing at a duplicate), and a changed
// re-POST must CLOSE the old pool before swapping in the new one. The propose-time
// guard (guardDatabaseConfigChange) ensures a changed connection has no live
// dependents, so closing here is safe.
//
// Both sub-tests fail on the pre-fix code, which always rebuilt the pool and
// never closed the superseded handle (a guaranteed leak on every re-POST).
func TestSaveDatabase_RePOSTPoolLifecycle(t *testing.T) {
	newFakeParser := func(handles *[]*clusterfakes.FakeDatabase) *clusterfakes.FakeDatabaseParser {
		fp := &clusterfakes.FakeDatabaseParser{}
		fp.ParseCalls(func(*cluster.ParsedConfig) (cluster.Database, error) {
			h := &clusterfakes.FakeDatabase{}
			*handles = append(*handles, h)
			return h, nil
		})
		return fp
	}

	t.Run("byte-identical re-POST keeps the existing pool (no leak, no rebuild)", func(t *testing.T) {
		var p db.Parser = parser.New()
		s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
		defer s.Cleanup()

		var handles []*clusterfakes.FakeDatabase
		p.AddDatabaseParser("sink", newFakeParser(&handles))

		cfg := createDatabaseConfiguration("sink")
		insertDatabases(t, s, []*cluster.Configuration{cfg}, 6, 6)
		insertDatabases(t, s, []*cluster.Configuration{cfg}, 6, 7) // identical re-POST

		require.Len(t, handles, 1, "identical re-POST must not build a second pool")
		require.Equal(t, 0, handles[0].CloseCallCount(), "the retained pool must not be closed")
		got, err := s.Database("sink")
		require.NoError(t, err)
		require.Same(t, handles[0], got.(*clusterfakes.FakeDatabase), "the original handle is retained")
	})

	t.Run("changed-config re-POST closes the superseded pool (no leak)", func(t *testing.T) {
		var p db.Parser = parser.New()
		s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
		defer s.Cleanup()

		var handles []*clusterfakes.FakeDatabase
		p.AddDatabaseParser("sink", newFakeParser(&handles))

		cfgA := createDatabaseConfiguration("sink")
		// Same id + type (so the same parser builds it), different bytes — a
		// changed config that forces a rebuild.
		cfgB := createConfiguration("sink", &DatabaseConfig{Details: &Details{Name: "sink-renamed", Type: "sink"}})
		insertDatabases(t, s, []*cluster.Configuration{cfgA}, 6, 6)
		insertDatabases(t, s, []*cluster.Configuration{cfgB}, 6, 7) // changed re-POST

		require.Len(t, handles, 2, "a changed re-POST rebuilds the pool")
		require.Equal(t, 1, handles[0].CloseCallCount(), "the superseded pool must be closed, not leaked")
		require.Equal(t, 0, handles[1].CloseCallCount(), "the new pool stays open")
		got, err := s.Database("sink")
		require.NoError(t, err)
		require.Same(t, handles[1], got.(*clusterfakes.FakeDatabase), "the new handle replaces the old")
	})
}
