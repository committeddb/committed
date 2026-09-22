package wal

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Each factory is explicitly chosen and remembers whether it created this test
// directory. No on-disk format inference or production selector is involved.
func storageTestOpeners() map[string]eventLogOpener {
	result := map[string]eventLogOpener{"production-tidwall": openEventLog}
	for name, create := range eventLogTestBackends() {
		created := false
		result[name] = func(path string, _ *metrics.Metrics, _ tidwall.LegacyOptions) (*eventLogBinding, error) {
			var log eventlog.EventLog
			var err error
			if !created {
				log, err = create(path)
			} else if name == "tidwall" {
				log, err = tidwall.Open(path)
			} else {
				cache := segmentlog.CacheOptions{}
				if name == "segmented-cached" {
					cache = segmentlog.CacheOptions{RecentBytes: 1024, HistoricalBytes: 1024}
				}
				log, err = segmented.Open(path, segmentlog.Options{}, cache)
			}
			if err != nil {
				return nil, err
			}
			created = true
			return bindEventLog(log), nil
		}
	}
	return result
}

func TestStorageStartupApplyAndReopenBackends(t *testing.T) {
	for name, openEvents := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			open := func() *Storage {
				s, err := openStorage(path, nil, nil, nil, openEvents, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			s := open()
			typ := &cluster.Type{ID: "items", Name: "items", Version: 1}
			registration, err := cluster.NewUpsertTypeEntity(typ)
			require.NoError(t, err)
			data, err := (&cluster.Proposal{Entities: []*cluster.Entity{registration}}).Marshal()
			require.NoError(t, err)
			entries := make([]*pb.Entry, 0, 3)
			entries = append(entries, &pb.Entry{Index: proto.Uint64(1), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum(), Data: data})
			for _, id := range []uint64{2, 3} {
				entry := new(pb.Entry)
				require.NoError(t, proto.Unmarshal(experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "value")), entry))
				entries = append(entries, entry)
			}
			require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(3)}, entries, &pb.Snapshot{}))
			require.NoError(t, s.ApplyCommittedBatch(entries))
			require.Equal(t, uint64(3), s.EventIndex())
			require.Equal(t, uint64(3), s.DataEventIndex())
			require.NoError(t, s.Close())
			s = open()
			require.Equal(t, uint64(3), s.EventIndex())
			require.Equal(t, uint64(3), s.AppliedIndex())
			require.Equal(t, uint64(3), s.DataEventIndex())
			require.Equal(t, uint64(1), s.firstEventIndex.Load())
			require.NoError(t, s.ApplyCommittedBatch(entries), "replay must remain idempotent")
			require.Zero(t, s.eventLogWriteOps.Load())
			// Model the crash window after the event batch sync but before
			// application metadata/applied progress has been committed.
			pending := make([]*pb.Entry, 0, 2)
			for _, id := range []uint64{4, 5} {
				entry := new(pb.Entry)
				raw := experimentEntry(t, id, pb.EntryConfChange)
				if id == 4 {
					raw = experimentEntry(t, id, pb.EntryNormal, experimentRow("key", "later"))
				}
				require.NoError(t, proto.Unmarshal(raw, entry))
				pending = append(pending, entry)
			}
			require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(5)}, pending, &pb.Snapshot{}))
			require.NoError(t, s.appendEvents(pending))
			require.NoError(t, s.Close())
			s = open()
			require.Equal(t, uint64(5), s.EventIndex())
			require.Equal(t, uint64(3), s.AppliedIndex())
			require.NoError(t, s.ApplyCommittedBatch(pending))
			require.Equal(t, uint64(5), s.AppliedIndex())
			require.Equal(t, uint64(4), s.DataEventIndex())
			require.Zero(t, s.eventLogWriteOps.Load(), "replay must reuse the durable event batch")
			r := &Reader{s: s}
			defer func() { _ = r.Close() }()
			for _, id := range []uint64{2, 3, 4} {
				actual, err := r.Read()
				require.NoError(t, err)
				require.Equal(t, id, actual.Index)
			}
			_, err = r.Read()
			require.ErrorIs(t, err, io.EOF)
			actual, err := s.ActualAt(2)
			require.NoError(t, err)
			require.Equal(t, uint64(2), actual.Index)
		})
	}
}

func TestStorageFailedStartupClosesEventBackend(t *testing.T) {
	path := t.TempDir()
	var binding *eventLogBinding
	openEvents := func(dir string, _ *metrics.Metrics, _ tidwall.LegacyOptions) (*eventLogBinding, error) {
		log, err := segmented.Create(dir, 1, segmentlog.LogOptions{})
		require.NoError(t, err)
		require.NoError(t, log.Append([]eventlog.Record{{ID: 1, Payload: []byte{0xff}}}))
		binding = bindEventLog(log)
		return binding, nil
	}
	_, err := openStorage(path, nil, nil, nil, openEvents, WithSafeMode())
	require.ErrorIs(t, err, ErrCorruptEntry)
	_, _, err = binding.entries.LastAppended()
	require.ErrorIs(t, err, eventlog.ErrClosed)
	// Reopening the same backend proves the failed startup released its lock.
	log, err := segmented.Open(datadir.EventsDir(path), segmentlog.Options{})
	require.NoError(t, err)
	require.NoError(t, log.Close())
}
