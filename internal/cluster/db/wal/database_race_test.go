package wal_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// TestDatabase_ConcurrentReadDuringApply pins the databases-map synchronization:
// Storage.Database(id) is called from HTTP handler goroutines (every syncable
// POST/rollback/replay parses its config, which resolves the database) and from
// the startup RestoreSyncableWorkers goroutine, while the raft apply path
// writes the same map in saveDatabase/deleteDatabase (and RestoreSnapshot swaps
// it wholesale). Unsynchronized, that pairing is a Go runtime
// "fatal error: concurrent map read and map write" — a node crash, not an
// error. This test drives both sides concurrently so `go test -race` fails on
// an unguarded map (it did, pre-fix) and stays green with the mutex.
func TestDatabase_ConcurrentReadDuringApply(t *testing.T) {
	var p db.Parser = parser.New()
	s := NewStorageWithParser(t, index(3).terms(3, 4, 5), p)
	defer s.Cleanup()

	fp := &clusterfakes.FakeDatabaseParser{}
	fp.ParseCalls(func(*cluster.ParsedConfig) (cluster.Database, error) {
		return &clusterfakes.FakeDatabase{}, nil
	})
	p.AddDatabaseParser("sink", fp)

	// Seed the database so the reader goroutine always has a hit to race with.
	idx := insertDatabases(t, s, []*cluster.Configuration{createDatabaseConfiguration("sink")}, 6, 6)

	// Reader: the HTTP-goroutine side of the race.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.Database("sink")
			}
		}
	}()

	// Writer: the apply side. Each config is byte-different (the name varies),
	// so every apply takes the rebuild path — closing the superseded handle and
	// writing the map — rather than the byte-identical fast path.
	for i := 0; i < 300; i++ {
		cfg := createConfiguration("sink", &DatabaseConfig{Details: &Details{Name: fmt.Sprintf("sink-%d", i), Type: "sink"}})
		e, err := cluster.NewUpsertDatabaseEntity(cfg)
		require.NoError(t, err)
		saveEntity(t, e, s, 6, idx+uint64(i))
	}

	close(stop)
	<-done
}
