package wal

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
)

func productionEntryTestBackends() map[string]func(string) (*eventLogBinding, error) {
	backends := map[string]func(string) (*eventLogBinding, error){
		"production-tidwall": func(path string) (*eventLogBinding, error) {
			return openEventLog(path, nil, tidwall.LegacyOptions{})
		},
	}
	for name, open := range eventLogTestBackends() {
		backends[name] = func(path string) (*eventLogBinding, error) {
			log, err := open(path)
			if err != nil {
				return nil, err
			}
			return &eventLogBinding{entries: bindEventEntries(log)}, nil
		}
	}
	return backends
}

func TestProductionEntryBindingBackends(t *testing.T) {
	for name, open := range productionEntryTestBackends() {
		t.Run(name, func(t *testing.T) {
			binding, err := open(t.TempDir())
			require.NoError(t, err)
			s := &Storage{eventLog: binding}
			t.Cleanup(func() { _ = s.eventLog.Close() })
			ref := cluster.TypeRef{ID: "items", Version: 1}
			typ, err := eventTestType(ref)
			require.NoError(t, err)
			s.typeCache.Store(ref, typeCacheEntry{t: typ})
			entry := func(id uint64) *pb.Entry {
				e := new(pb.Entry)
				require.NoError(t, proto.Unmarshal(experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "value")), e))
				return e
			}
			batch := []*pb.Entry{entry(10), entry(30)}
			require.NoError(t, s.appendEvents(batch))
			require.NoError(t, s.appendEvents(batch), "replay must not append duplicates")
			require.Equal(t, int64(1), s.eventLogWriteOps.Load())
			s.appliedIndex.Store(10)
			r := &Reader{s: s}
			defer func() { _ = r.Close() }()
			actual, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, uint64(10), actual.Index)
			_, err = r.Read()
			require.ErrorIs(t, err, io.EOF, "unapplied entry must remain pending")
			s.appliedIndex.Store(30)
			actual, err = r.Read()
			require.NoError(t, err)
			require.Equal(t, uint64(30), actual.Index)
			_, err = r.Read()
			require.ErrorIs(t, err, io.EOF)
			require.NoError(t, s.appendEvent(entry(50)))
			s.appliedIndex.Store(50)
			actual, err = r.Read()
			require.NoError(t, err)
			require.Equal(t, uint64(50), actual.Index, "EOF must remain temporary")
			actual, err = s.ActualAt(30)
			require.NoError(t, err)
			require.Equal(t, uint64(30), actual.Index)
			_, err = s.ActualAt(20)
			require.ErrorIs(t, err, ErrActualNotFound)
			var indexes []uint64
			require.NoError(t, s.scanEventEntries(30, func(e *pb.Entry) error {
				indexes = append(indexes, e.GetIndex())
				return nil
			}))
			require.Equal(t, []uint64{10, 30}, indexes)

			// Replace the complete binding without bumping the generation, as
			// peer adoption can do. The existing reader must rebind by identity.
			replacement, err := open(t.TempDir())
			require.NoError(t, err)
			s.eventMu.Lock()
			require.NoError(t, s.eventLog.Close())
			s.eventLog = replacement
			s.eventMu.Unlock()
			require.NoError(t, s.appendEvent(entry(70)))
			s.appliedIndex.Store(70)
			actual, err = r.Read()
			require.NoError(t, err)
			require.Equal(t, uint64(70), actual.Index)
		})
	}
}
