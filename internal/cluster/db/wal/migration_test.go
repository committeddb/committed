package wal_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// TestMigration_FlatToVersioned writes flat key/value entries directly to
// bbolt (simulating the old layout), then reopens with wal.Open (which
// runs migration) and verifies the versioned layout is correct.
func TestMigration_FlatToVersioned(t *testing.T) {
	dir := t.TempDir()
	// Post-layout-migration, the bbolt file lives under metadata/.
	kvDir := filepath.Join(dir, "metadata")

	// We need to create the directory structure that wal.Open expects.
	// Instead of doing that manually, we'll open a storage, close it,
	// then write flat entries directly to the bbolt file.
	p := parser.New()
	fakeDBParser := &clusterfakes.FakeDatabaseParser{}
	fakeDBParser.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p.AddDatabaseParser("foo", fakeDBParser)

	fakeIngestParser := &clusterfakes.FakeIngestableParser{}
	fakeIngestParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeIngestParser)

	// Open and close to create directory structure
	s, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)
	require.Nil(t, s.Close())

	// Now write flat entries directly to bbolt (old layout)
	boltOpts := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(filepath.Join(kvDir, "bbolt.db"), 0600, boltOpts)
	require.Nil(t, err)

	cfg1 := &cluster.Configuration{ID: "ingest-1", MimeType: "application/json", Data: []byte(`{"ingestable":{"name":"i1","type":"foo"}}`)}
	cfg2 := &cluster.Configuration{ID: "ingest-2", MimeType: "application/json", Data: []byte(`{"ingestable":{"name":"i2","type":"foo"}}`)}
	dbCfg := &cluster.Configuration{ID: "db-1", MimeType: "application/json", Data: []byte(`{"database":{"name":"d1","type":"foo"}}`)}

	bs1, err := cfg1.Marshal()
	require.Nil(t, err)
	bs2, err := cfg2.Marshal()
	require.Nil(t, err)
	bsDB, err := dbCfg.Marshal()
	require.Nil(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		ib := tx.Bucket([]byte("ingestables"))
		if err := ib.Put([]byte("ingest-1"), bs1); err != nil {
			return err
		}
		if err := ib.Put([]byte("ingest-2"), bs2); err != nil {
			return err
		}

		dbb := tx.Bucket([]byte("databases"))
		return dbb.Put([]byte("db-1"), bsDB)
	})
	require.Nil(t, err)
	require.Nil(t, db.Close())

	// Reopen with wal.Open — migration should run
	s2, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)
	defer s2.Close()

	// Verify ingestables migrated to versioned layout
	cfgs, err := s2.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 2, len(cfgs))

	// Each should have exactly one version
	v1, err := s2.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 1, len(v1))
	require.Equal(t, uint64(1), v1[0].Version)
	require.True(t, v1[0].Current)

	v2, err := s2.IngestableVersions("ingest-2")
	require.Nil(t, err)
	require.Equal(t, 1, len(v2))

	// Version content matches original
	fetched, err := s2.IngestableVersion("ingest-1", 1)
	require.Nil(t, err)
	require.Equal(t, cfg1.ID, fetched.ID)
	require.Equal(t, cfg1.Data, fetched.Data)

	// Database also migrated
	dbs, err := s2.Databases()
	require.Nil(t, err)
	require.Equal(t, 1, len(dbs))

	dbVers, err := s2.DatabaseVersions("db-1")
	require.Nil(t, err)
	require.Equal(t, 1, len(dbVers))
}

// TestMigration_Idempotent verifies that running migration on already-migrated
// data is a no-op.
func TestMigration_Idempotent(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)

	cfg := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"i1","type":"foo"}}`),
	}
	e, err := cluster.NewUpsertIngestableEntity(cfg)
	require.Nil(t, err)
	saveEntity(t, e, s, 6, 6)

	// Close and reopen (migration runs again but should be no-op)
	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	versions, err := s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 1, len(versions))
	require.Equal(t, uint64(1), versions[0].Version)

	// Can still add new versions
	cfg2 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"i1-v2","type":"foo"}}`),
	}
	e2, err := cluster.NewUpsertIngestableEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	versions, err = s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))
}

// TestMigration_EmptyBuckets verifies that migration on fresh storage
// (no entries) works without error.
func TestMigration_EmptyBuckets(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	// Should be empty, no error
	cfgs, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 0, len(cfgs))

	cfgs, err = s.Syncables()
	require.Nil(t, err)
	require.Equal(t, 0, len(cfgs))

	cfgs, err = s.Databases()
	require.Nil(t, err)
	require.Equal(t, 0, len(cfgs))

	cfgs, err = s.Types()
	require.Nil(t, err)
	require.Equal(t, 0, len(cfgs))
}

// TestMigration_TypePreservesContent verifies that a Type written in flat
// layout (the old format) is correctly migrated and readable as a Type.
func TestMigration_TypePreservesContent(t *testing.T) {
	dir := t.TempDir()
	kvDir := filepath.Join(dir, "metadata")

	p := parser.New()
	s, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)
	require.Nil(t, s.Close())

	// Write a Type directly in flat layout
	boltOpts := &bolt.Options{Timeout: 1 * time.Second}
	db, err := bolt.Open(filepath.Join(kvDir, "bbolt.db"), 0600, boltOpts)
	require.Nil(t, err)

	origType := &cluster.Type{ID: "events", Name: "Events", Version: 1}
	bs, err := origType.Marshal()
	require.Nil(t, err)

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("types"))
		return b.Put([]byte("events"), bs)
	})
	require.Nil(t, err)
	require.Nil(t, db.Close())

	// Reopen — migration runs
	s2, err := wal.Open(dir, p, nil, nil, testOpenOptions...)
	require.Nil(t, err)
	defer s2.Close()

	// Type() should work via versioned lookup
	tp, err := s2.Type("events")
	require.Nil(t, err)
	require.Equal(t, "events", tp.ID)
	require.Equal(t, "Events", tp.Name)
	require.Equal(t, 1, tp.Version)

	// Types() listing should work
	types, err := s2.Types()
	require.Nil(t, err)
	require.Equal(t, 1, len(types))

	// Version listing works
	vers, err := s2.TypeVersions("events")
	require.Nil(t, err)
	require.Equal(t, 1, len(vers))
}
