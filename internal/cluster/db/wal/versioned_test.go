package wal_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/stretchr/testify/require"
)

func TestIngestableVersions_MultipleVersions(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	cfg1 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1","type":"foo"}}`),
	}
	cfg2 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1-v2","type":"foo"}}`),
	}

	e1, err := cluster.NewUpsertIngestableEntity(cfg1)
	require.Nil(t, err)
	saveEntity(t, e1, s, 6, 6)

	e2, err := cluster.NewUpsertIngestableEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	// List versions
	versions, err := s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))
	require.Equal(t, uint64(1), versions[0].Version)
	require.False(t, versions[0].Current)
	require.Equal(t, uint64(2), versions[1].Version)
	require.True(t, versions[1].Current)

	// Fetch specific versions
	v1, err := s.IngestableVersion("ingest-1", 1)
	require.Nil(t, err)
	require.Equal(t, cfg1.ID, v1.ID)
	require.Equal(t, cfg1.MimeType, v1.MimeType)
	require.Equal(t, cfg1.Data, v1.Data)

	v2, err := s.IngestableVersion("ingest-1", 2)
	require.Nil(t, err)
	require.Equal(t, cfg2.Data, v2.Data)

	// Current listing returns only the latest
	cfgs, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Equal(t, cfg2.Data, cfgs[0].Data)
}

func TestSyncableVersions_MultipleVersions(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeSyncableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	cfg1 := &cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable":{"name":"sync-1","type":"foo"}}`),
	}
	cfg2 := &cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable":{"name":"sync-1-v2","type":"foo"}}`),
	}

	e1, err := cluster.NewUpsertSyncableEntity(cfg1)
	require.Nil(t, err)
	saveEntity(t, e1, s, 6, 6)

	e2, err := cluster.NewUpsertSyncableEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	versions, err := s.SyncableVersions("sync-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))
	require.Equal(t, uint64(1), versions[0].Version)
	require.Equal(t, uint64(2), versions[1].Version)
	require.True(t, versions[1].Current)

	v1, err := s.SyncableVersion("sync-1", 1)
	require.Nil(t, err)
	require.Equal(t, cfg1.Data, v1.Data)
}

func TestDatabaseVersions_MultipleVersions(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeDatabaseParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeDatabase{}, nil)
	p.AddDatabaseParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	cfg1 := &cluster.Configuration{
		ID:       "db-1",
		MimeType: "application/json",
		Data:     []byte(`{"database":{"name":"db-1","type":"foo"}}`),
	}
	cfg2 := &cluster.Configuration{
		ID:       "db-1",
		MimeType: "application/json",
		Data:     []byte(`{"database":{"name":"db-1-v2","type":"foo"}}`),
	}

	e1, err := cluster.NewUpsertDatabaseEntity(cfg1)
	require.Nil(t, err)
	saveEntity(t, e1, s, 6, 6)

	e2, err := cluster.NewUpsertDatabaseEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	versions, err := s.DatabaseVersions("db-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))
	require.True(t, versions[1].Current)

	v1, err := s.DatabaseVersion("db-1", 1)
	require.Nil(t, err)
	require.Equal(t, cfg1.Data, v1.Data)
}

func TestTypeVersions_MultipleVersions(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "type-1", Name: "TypeV1", Version: 1})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "type-1", Name: "TypeV2", Version: 2})
	require.Nil(t, err)
	saveEntity(t, t2, s, 6, 7)

	versions, err := s.TypeVersions("type-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))
	require.True(t, versions[1].Current)

	v1, err := s.TypeVersion("type-1", 1)
	require.Nil(t, err)
	require.Contains(t, string(v1.Data), "TypeV1")

	v2, err := s.TypeVersion("type-1", 2)
	require.Nil(t, err)
	require.Contains(t, string(v2.Data), "TypeV2")
}

func TestVersions_NotFound(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	_, err := s.IngestableVersions("nonexistent")
	require.NotNil(t, err)

	_, err = s.IngestableVersion("nonexistent", 1)
	require.NotNil(t, err)

	_, err = s.SyncableVersions("nonexistent")
	require.NotNil(t, err)

	_, err = s.DatabaseVersions("nonexistent")
	require.NotNil(t, err)

	_, err = s.TypeVersions("nonexistent")
	require.NotNil(t, err)
}

func TestVersions_VersionNotFound(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "type-1", Name: "T1", Version: 1})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	_, err = s.TypeVersion("type-1", 99)
	require.NotNil(t, err)
}

func TestVersions_PersistAcrossRestart(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	cfg1 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1","type":"foo"}}`),
	}
	cfg2 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1-v2","type":"foo"}}`),
	}

	e1, err := cluster.NewUpsertIngestableEntity(cfg1)
	require.Nil(t, err)
	saveEntity(t, e1, s, 6, 6)

	e2, err := cluster.NewUpsertIngestableEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	// Close and reopen
	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	versions, err := s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))

	v1, err := s.IngestableVersion("ingest-1", 1)
	require.Nil(t, err)
	require.Equal(t, cfg1.Data, v1.Data)

	v2, err := s.IngestableVersion("ingest-1", 2)
	require.Nil(t, err)
	require.Equal(t, cfg2.Data, v2.Data)
}

func TestRollback_CreatesNewVersion(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeParser)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	cfg1 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1","type":"foo"}}`),
	}
	cfg2 := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1-v2","type":"foo"}}`),
	}

	e1, err := cluster.NewUpsertIngestableEntity(cfg1)
	require.Nil(t, err)
	saveEntity(t, e1, s, 6, 6)

	e2, err := cluster.NewUpsertIngestableEntity(cfg2)
	require.Nil(t, err)
	saveEntity(t, e2, s, 6, 7)

	// Simulate rollback: fetch v1, re-propose it
	v1, err := s.IngestableVersion("ingest-1", 1)
	require.Nil(t, err)

	rollbackEntity, err := cluster.NewUpsertIngestableEntity(v1)
	require.Nil(t, err)
	saveEntity(t, rollbackEntity, s, 6, 8)

	// Should now have 3 versions, v3 is current with v1's content
	versions, err := s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 3, len(versions))
	require.True(t, versions[2].Current)
	require.Equal(t, uint64(3), versions[2].Version)

	v3, err := s.IngestableVersion("ingest-1", 3)
	require.Nil(t, err)
	require.Equal(t, cfg1.Data, v3.Data)

	// Current listing should show v3 (which has v1's content)
	cfgs, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Equal(t, cfg1.Data, cfgs[0].Data)
}

func TestMultipleResources_IndependentVersioning(t *testing.T) {
	p := parser.New()
	fakeIngest := &clusterfakes.FakeIngestableParser{}
	fakeIngest.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeIngest)

	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	// Two different ingestables, each should have independent version counters
	cfgA := &cluster.Configuration{
		ID:       "ingest-a",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"a","type":"foo"}}`),
	}
	cfgB := &cluster.Configuration{
		ID:       "ingest-b",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"b","type":"foo"}}`),
	}

	idx := uint64(6)

	eA, err := cluster.NewUpsertIngestableEntity(cfgA)
	require.Nil(t, err)
	saveEntity(t, eA, s, 6, idx)
	idx++

	eB, err := cluster.NewUpsertIngestableEntity(cfgB)
	require.Nil(t, err)
	saveEntity(t, eB, s, 6, idx)
	idx++

	// Update A only
	cfgA2 := &cluster.Configuration{
		ID:       "ingest-a",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"a-v2","type":"foo"}}`),
	}
	eA2, err := cluster.NewUpsertIngestableEntity(cfgA2)
	require.Nil(t, err)
	saveEntity(t, eA2, s, 6, idx)

	versA, err := s.IngestableVersions("ingest-a")
	require.Nil(t, err)
	require.Equal(t, 2, len(versA))

	versB, err := s.IngestableVersions("ingest-b")
	require.Nil(t, err)
	require.Equal(t, 1, len(versB))

	cfgs, err := s.Ingestables()
	require.Nil(t, err)
	require.Equal(t, 2, len(cfgs))
}

func TestDeleteVersionedResource(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "type-1", Name: "T1", Version: 1})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "type-1", Name: "T1v2", Version: 2})
	require.Nil(t, err)
	saveEntity(t, t2, s, 6, 7)

	// Delete
	del := cluster.NewDeleteTypeEntity("type-1")
	saveEntity(t, del, s, 6, 8)

	// Versions should fail (resource gone)
	_, err = s.TypeVersions("type-1")
	require.NotNil(t, err)

	// List should be empty
	types, err := s.Types()
	require.Nil(t, err)
	require.Equal(t, 0, len(types))
}

func TestIngestableVersions_WithChannel(t *testing.T) {
	p := parser.New()
	fakeParser := &clusterfakes.FakeIngestableParser{}
	fakeParser.ParseReturns(&clusterfakes.FakeIngestable{}, nil)
	p.AddIngestableParser("foo", fakeParser)

	ingestCh := make(chan *db.IngestableWithID, 2)
	s := OpenStorage(t, t.TempDir(), p, nil, ingestCh)
	defer s.Cleanup()

	_ = s.Save(defaultHardState, index(3).terms(3, 4, 5), defaultSnap)

	cfg := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable":{"name":"ingest-1","type":"foo"}}`),
	}

	e, err := cluster.NewUpsertIngestableEntity(cfg)
	require.Nil(t, err)
	saveEntity(t, e, s, 6, 6)

	// Verify channel received notification
	received := <-ingestCh
	require.Equal(t, "ingest-1", received.ID)

	// Verify version was created
	versions, err := s.IngestableVersions("ingest-1")
	require.Nil(t, err)
	require.Equal(t, 1, len(versions))
}

// TestTypeImmutability_SameVersionRejected verifies that redefining a type
// with the same ID and Version field is silently skipped at the apply layer.
// The preflight check in ProposeType would reject this with an error before
// it reaches Raft, but the apply-side guard is a safety net.
func TestTypeImmutability_SameVersionRejected(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	// Save type v1
	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "Events", Version: 1,
	})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	// Attempt to save same ID+Version with different Name (mutation)
	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "EventsMutated", Version: 1,
	})
	require.Nil(t, err)
	saveEntity(t, t2, s, 6, 7)

	// The mutation should have been silently skipped — the original
	// definition should still be in place.
	got, err := s.ResolveType(cluster.LatestTypeRef("events"))
	require.Nil(t, err)
	require.Equal(t, "Events", got.Name)

	// Should still have only one config version (the skip doesn't create a new one)
	versions, err := s.TypeVersions("events")
	require.Nil(t, err)
	require.Equal(t, 1, len(versions))
}

// TestTypeImmutability_NewVersionAllowed verifies that defining a new schema
// version (different Version field) for the same type ID is allowed.
func TestTypeImmutability_NewVersionAllowed(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "Events", Version: 1,
	})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	// Version 2 with different schema — should be allowed
	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "EventsV2", Version: 2,
		SchemaType: "json-schema",
		Schema:     []byte(`{"type":"object"}`),
	})
	require.Nil(t, err)
	saveEntity(t, t2, s, 6, 7)

	// Current type should be v2
	got, err := s.ResolveType(cluster.LatestTypeRef("events"))
	require.Nil(t, err)
	require.Equal(t, "EventsV2", got.Name)
	require.Equal(t, 2, got.Version)

	// Should have two config versions
	versions, err := s.TypeVersions("events")
	require.Nil(t, err)
	require.Equal(t, 2, len(versions))

	// v1 is still retrievable
	v1cfg, err := s.TypeVersion("events", 1)
	require.Nil(t, err)
	require.Contains(t, string(v1cfg.Data), "Events")
}

// TestTypeImmutability_AfterDelete verifies that deleting a type and
// re-creating it with the same version is allowed (the immutability
// check only applies to existing types).
func TestTypeImmutability_AfterDelete(t *testing.T) {
	s := NewStorage(t, index(3).terms(3, 4, 5))
	defer s.Cleanup()

	t1, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "Events", Version: 1,
	})
	require.Nil(t, err)
	saveEntity(t, t1, s, 6, 6)

	// Delete
	del := cluster.NewDeleteTypeEntity("events")
	saveEntity(t, del, s, 6, 7)

	// Re-create with same version — should be allowed since the type was deleted
	t2, err := cluster.NewUpsertTypeEntity(&cluster.Type{
		ID: "events", Name: "EventsRedefined", Version: 1,
	})
	require.Nil(t, err)
	saveEntity(t, t2, s, 6, 8)

	got, err := s.ResolveType(cluster.LatestTypeRef("events"))
	require.Nil(t, err)
	require.Equal(t, "EventsRedefined", got.Name)
}
