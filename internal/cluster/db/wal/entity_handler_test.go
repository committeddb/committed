package wal_test

import (
	"context"
	"io"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/spf13/viper"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/stretchr/testify/require"
)

// makeEntry creates a raftpb.Entry containing a marshaled proposal at the given index.
func makeEntry(t *testing.T, idx uint64, entities ...*cluster.Entity) pb.Entry {
	p := &cluster.Proposal{Entities: entities}
	bs, err := p.Marshal()
	require.Nil(t, err)
	return pb.Entry{
		Term:  1,
		Index: idx,
		Type:  pb.EntryNormal,
		Data:  bs,
	}
}

func makeTypeEntity(t *testing.T, id string, name string) *cluster.Entity {
	typ := &cluster.Type{ID: id, Name: name, Version: 1}
	e, err := cluster.NewUpsertTypeEntity(typ)
	require.Nil(t, err)
	return e
}

func makeSyncableIndexEntity(t *testing.T, syncID string, idx uint64) *cluster.Entity {
	si := &cluster.SyncableIndex{ID: syncID, Index: idx}
	e, err := cluster.NewUpsertSyncableIndexEntity(si)
	require.Nil(t, err)
	return e
}

// makeUserEntity returns a user-data Entity. The Type it references must
// be registered in storage via s.RegisterType(t, "user-type-123") before
// the entity's proposal is applied, so that Unmarshal can resolve it.
// The entity's Type field is kept minimal (ID only) because production
// callers set Type directly from what they already have in memory; the
// registered storage entry is what the apply/read path will use.
func makeUserEntity() *cluster.Entity {
	userType := &cluster.Type{ID: "user-type-123", Name: "user-type-123", Version: 1}
	return cluster.NewUpsertEntity(userType, []byte("key1"), []byte(`{"value": "hello"}`))
}

// --- Type Entity Tests ---

func TestSave_TypeEntity_Upsert(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	entity := makeTypeEntity(t, "type-1", "MyType")
	entry := makeEntry(t, 1, entity)

	saveAndApply(t, s, []pb.Entry{entry})

	typ, err := s.ResolveType(cluster.LatestTypeRef("type-1"))
	require.Nil(t, err)
	require.Equal(t, "type-1", typ.ID)
	require.Equal(t, "MyType", typ.Name)
	require.Equal(t, 1, typ.Version)

	cfgs, err := s.Types()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Equal(t, "type-1", cfgs[0].ID)
}

func TestSave_TypeEntity_Delete(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	entity := makeTypeEntity(t, "type-del", "ToDelete")
	entry := makeEntry(t, 1, entity)
	saveAndApply(t, s, []pb.Entry{entry})

	_, err := s.ResolveType(cluster.LatestTypeRef("type-del"))
	require.Nil(t, err)

	delEntity := cluster.NewDeleteTypeEntity("type-del")
	delEntry := makeEntry(t, 2, delEntity)
	saveAndApply(t, s, []pb.Entry{delEntry})

	_, err = s.ResolveType(cluster.LatestTypeRef("type-del"))
	require.NotNil(t, err)
	require.ErrorContains(t, err, "type-del")
}

func TestSave_MultipleTypeEntities(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	e1 := makeTypeEntity(t, "t1", "Type1")
	e2 := makeTypeEntity(t, "t2", "Type2")
	entry := makeEntry(t, 1, e1, e2)

	saveAndApply(t, s, []pb.Entry{entry})

	cfgs, err := s.Types()
	require.Nil(t, err)
	require.Equal(t, 2, len(cfgs))
}

// --- SyncableIndex Entity Tests ---

func TestSave_SyncableIndexEntity(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	// Type at index 1; user entity at index 2; syncable index at 3.
	s.RegisterType(t, "user-type-123", 1, 1)

	userEntity := makeUserEntity()
	userEntry := makeEntry(t, 2, userEntity)
	saveAndApply(t, s, []pb.Entry{userEntry})

	siEntity := makeSyncableIndexEntity(t, "sync-1", 2)
	siEntry := makeEntry(t, 3, siEntity)
	saveAndApply(t, s, []pb.Entry{siEntry})

	// Reader with checkpoint at index 2 should skip the syncable index entry at 3
	reader := s.Reader("sync-1")
	_, _, err := reader.Read()
	require.Equal(t, io.EOF, err)
}

// --- User-Defined Entity Tests ---

func TestSave_UserDefinedEntity(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	// Type at index 1; user entity at index 2.
	s.RegisterType(t, "user-type-123", 1, 1)

	entity := makeUserEntity()
	entry := makeEntry(t, 2, entity)

	saveAndApply(t, s, []pb.Entry{entry})

	last, err := s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(2), last)
}

// --- Mixed Entities ---

func TestSave_MixedEntities(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	// user-type-123 is referenced by makeUserEntity and must be
	// resolvable by the time the mixed entry applies. Register it at
	// index 1; the mixed entry (which defines another type, type-mix,
	// and writes a user entity) goes at index 2.
	s.RegisterType(t, "user-type-123", 1, 1)

	typeEntity := makeTypeEntity(t, "type-mix", "MixType")
	userEntity := makeUserEntity()
	entry := makeEntry(t, 2, typeEntity, userEntity)

	saveAndApply(t, s, []pb.Entry{entry})

	typ, err := s.ResolveType(cluster.LatestTypeRef("type-mix"))
	require.Nil(t, err)
	require.Equal(t, "MixType", typ.Name)

	last, err := s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(2), last)
}

// --- ConfChange Entry ---

func TestSave_ConfChangeEntry_SkipsEntityHandlers(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	entry := pb.Entry{
		Term:  1,
		Index: 1,
		Type:  pb.EntryConfChange,
		Data:  []byte("some conf change data"),
	}

	saveAndApply(t, s, []pb.Entry{entry})

	cfgs, err := s.Types()
	require.Nil(t, err)
	require.Equal(t, 0, len(cfgs))
}

// --- Multiple Entries ---

func TestSave_MultipleEntries(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	e1 := makeTypeEntity(t, "t-first", "First")
	e2 := makeTypeEntity(t, "t-second", "Second")

	entries := []pb.Entry{
		makeEntry(t, 1, e1),
		makeEntry(t, 2, e2),
	}

	saveAndApply(t, s, entries)

	cfgs, err := s.Types()
	require.Nil(t, err)
	require.Equal(t, 2, len(cfgs))

	last, err := s.LastIndex()
	require.Nil(t, err)
	require.Equal(t, uint64(2), last)
}

// --- Database Entity (requires parser) ---

func TestSave_DatabaseEntity(t *testing.T) {
	p := parser.New()

	fakeDB := &clusterfakes.FakeDatabase{}
	fakeDBParser := &clusterfakes.FakeDatabaseParser{}
	fakeDBParser.ParseReturns(fakeDB, nil)
	p.AddDatabaseParser("sql", fakeDBParser)

	s := NewStorageWithParser(t, nil, p)
	defer s.Cleanup()

	cfg := &cluster.Configuration{
		ID:       "db-1",
		MimeType: "application/json",
		Data:     []byte(`{"database": {"type": "sql", "name": "testdb"}, "sql": {"dialect": "mysql"}}`),
	}
	entity, err := cluster.NewUpsertDatabaseEntity(cfg)
	require.Nil(t, err)

	entry := makeEntry(t, 1, entity)
	saveAndApply(t, s, []pb.Entry{entry})

	database, err := s.Database("db-1")
	require.Nil(t, err)
	require.Equal(t, fakeDB, database)

	cfgs, err := s.Databases()
	require.Nil(t, err)
	require.Equal(t, 1, len(cfgs))
	require.Equal(t, "db-1", cfgs[0].ID)
}

// --- Syncable Entity (requires parser + channel) ---

func TestSave_SyncableEntity_SignalsChannel(t *testing.T) {
	p := parser.New()

	fakeSyncable := &clusterfakes.FakeSyncable{}
	fakeSyncParser := &clusterfakes.FakeSyncableParser{}
	fakeSyncParser.ParseReturns(fakeSyncable, nil)
	p.AddSyncableParser("sql", fakeSyncParser)

	syncCh := make(chan *db.SyncableWithID, 1)
	s := OpenStorage(t, t.TempDir(), p, syncCh, nil)
	defer s.Cleanup()

	cfg := &cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable": {"type": "sql", "name": "testsync"}, "sql": {"topic": "t1"}}`),
	}
	entity, err := cluster.NewUpsertSyncableEntity(cfg)
	require.Nil(t, err)

	entry := makeEntry(t, 1, entity)
	saveAndApply(t, s, []pb.Entry{entry})

	received := <-syncCh
	require.Equal(t, "sync-1", received.ID)
	require.Equal(t, fakeSyncable, received.Syncable)
}

// --- Ingestable Entity (requires parser + channel) ---

func TestSave_IngestableEntity_SignalsChannel(t *testing.T) {
	p := parser.New()

	fakeIngestable := &clusterfakes.FakeIngestable{}
	fakeIngestParser := &clusterfakes.FakeIngestableParser{}
	fakeIngestParser.ParseReturns(fakeIngestable, nil)
	p.AddIngestableParser("kafka", fakeIngestParser)

	ingestCh := make(chan *db.IngestableWithID, 1)
	s := OpenStorage(t, t.TempDir(), p, nil, ingestCh)
	defer s.Cleanup()

	cfg := &cluster.Configuration{
		ID:       "ingest-1",
		MimeType: "application/json",
		Data:     []byte(`{"ingestable": {"type": "kafka", "name": "testingest"}}`),
	}
	entity, err := cluster.NewUpsertIngestableEntity(cfg)
	require.Nil(t, err)

	entry := makeEntry(t, 1, entity)
	saveAndApply(t, s, []pb.Entry{entry})

	received := <-ingestCh
	require.Equal(t, "ingest-1", received.ID)
	require.Equal(t, fakeIngestable, received.Ingestable)
}

// Ensure the fakes satisfy the interfaces at compile time
var _ cluster.DatabaseParser = (*clusterfakes.FakeDatabaseParser)(nil)
var _ cluster.SyncableParser = (*clusterfakes.FakeSyncableParser)(nil)
var _ cluster.IngestableParser = (*clusterfakes.FakeIngestableParser)(nil)

// Suppress unused import warnings
var _ *viper.Viper
var _ context.Context
