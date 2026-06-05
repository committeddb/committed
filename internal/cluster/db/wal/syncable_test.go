package wal_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/db"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/philborlin/committed/internal/cluster/db/wal"
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

// TestRestoreSyncableWorkers is the syncable twin of
// TestRestoreIngestableWorkers. It pins the restart-resume contract:
//
//  1. Open does NOT auto-restore workers — doing so would race the caller's
//     parser registration and, on a loaded machine, silently drop every
//     syncable ("cannot parse syncable of type: ..." → degraded → skipped),
//     so a restarted node would never resume syncing.
//  2. The explicit RestoreSyncableWorkers, called once parsers are wired,
//     re-sends each persisted syncable to the sync channel.
func TestRestoreSyncableWorkers(t *testing.T) {
	dir := t.TempDir()
	p := parser.New()
	sp := &clusterfakes.FakeSyncableParser{}
	sp.ParseReturns(&clusterfakes.FakeSyncable{}, nil)
	p.AddSyncableParser("test", sp)

	openOpts := []wal.Option{wal.WithoutFsync()}

	// First incarnation: persist a syncable. Buffered so the apply-path send
	// (saveSyncable) doesn't block; drain it so it can't be mistaken for a
	// restore send later.
	syncCh := make(chan *db.SyncableWithID, 4)
	s, err := wal.Open(dir, p, syncCh, nil, openOpts...)
	require.Nil(t, err)
	cfg := &cluster.Configuration{
		ID:       "sync-1",
		MimeType: "application/json",
		Data:     []byte(`{"syncable": {"name": "sync-1", "type": "test"}}`),
	}
	ent, err := cluster.NewUpsertSyncableEntity(cfg)
	require.Nil(t, err)
	saveEntity(t, ent, s, 1, 1)
	select {
	case got := <-syncCh:
		require.Equal(t, "sync-1", got.ID, "apply path should send the freshly-applied syncable")
	case <-time.After(2 * time.Second):
		t.Fatal("apply path did not send the syncable")
	}
	require.Nil(t, s.Close())

	// Reopen. Open must NOT auto-restore — restoration is now the caller's
	// explicit, post-parser-registration responsibility.
	syncCh2 := make(chan *db.SyncableWithID, 4)
	s2, err := wal.Open(dir, p, syncCh2, nil, openOpts...)
	require.Nil(t, err)
	defer s2.Close()
	select {
	case got := <-syncCh2:
		t.Fatalf("Open must not auto-restore workers; got unexpected send for %q", got.ID)
	case <-time.After(200 * time.Millisecond):
	}

	// The explicit restore re-sends the persisted syncable.
	s2.RestoreSyncableWorkers()
	select {
	case got := <-syncCh2:
		require.Equal(t, "sync-1", got.ID)
	case <-time.After(2 * time.Second):
		t.Fatal("RestoreSyncableWorkers did not re-send the persisted syncable")
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
