package wal

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestDataHeadRecoveryBackends(t *testing.T) {
	for name, open := range productionEntryTestBackends() {
		t.Run(name, func(t *testing.T) {
			binding, err := open(t.TempDir())
			require.NoError(t, err)
			defer func() { _ = binding.Close() }()
			s := &Storage{eventLog: binding, logger: zap.NewNop()}
			s.recoverDataHead()
			require.Zero(t, s.DataEventIndex())
			records := make([]eventlog.Record, 0, 4096)
			records = append(records, eventlog.Record{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))})
			for id := uint64(20); id < 4115; id++ {
				records = append(records, eventlog.Record{ID: id, Payload: experimentEntry(t, id, pb.EntryConfChange)})
			}
			require.NoError(t, binding.entries.Append(records))
			s.recoverDataHead()
			require.Equal(t, uint64(10), s.DataEventIndex(), "the 4096th record is within the cap")
			s.dataEventIndex.Store(0)
			require.NoError(t, binding.entries.Append([]eventlog.Record{{ID: 5000, Payload: experimentEntry(t, 5000, pb.EntryConfChange)}}))
			s.recoverDataHead()
			require.Zero(t, s.DataEventIndex(), "the 4097th record is beyond the cap")
			require.NoError(t, binding.Close())
			s.dataEventIndex.Store(123)
			s.recoverDataHead()
			require.Equal(t, uint64(123), s.DataEventIndex(), "persisted head bypasses storage, even a closed handle")
		})
	}
}

func TestDataHeadRecoveryStopsAtCorruptEntry(t *testing.T) {
	for name, create := range eventLogTestBackends() {
		t.Run(name, func(t *testing.T) {
			log, err := create(t.TempDir())
			require.NoError(t, err)
			defer func() { _ = log.Close() }()
			require.NoError(t, log.Append([]eventlog.Record{
				{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal, experimentRow("key", "value"))},
				{ID: 90, Payload: []byte{0xff}},
			}))
			s := &Storage{eventLog: bindEventLog(log), logger: zap.NewNop()}
			count, err := s.eventLog.entries.ScanReverse(4096, func(*pb.Entry) (bool, error) {
				t.Fatal("must stop before delivering a corrupt entry or an older record")
				return false, nil
			})
			require.ErrorIs(t, err, ErrCorruptEntry)
			require.Equal(t, 1, count)
			s.recoverDataHead()
			require.Zero(t, s.DataEventIndex())
		})
	}
}
