package wal

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func TestProductionEventRecoveryBackends(t *testing.T) {
	for name, open := range productionEntryTestBackends() {
		t.Run(name, func(t *testing.T) {
			binding, err := open(t.TempDir())
			require.NoError(t, err)
			defer func() { _ = binding.Close() }()
			s := &Storage{eventLog: binding}
			require.NoError(t, s.deriveEventBoundsLocked())
			require.Zero(t, s.EventIndex())
			require.Zero(t, s.firstEventIndex.Load())
			require.NoError(t, binding.entries.Append([]eventlog.Record{
				{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal)},
				{ID: 90, Payload: experimentEntry(t, 90, pb.EntryNormal)},
			}))
			require.NoError(t, s.recoverEventIndex())
			require.Equal(t, uint64(90), s.EventIndex())
			require.Zero(t, s.firstEventIndex.Load(), "early recovery only publishes progress")
			require.NoError(t, s.deriveEventBoundsLocked())
			require.Equal(t, uint64(10), s.firstEventIndex.Load())
			require.NoError(t, binding.Close())
			require.Error(t, s.recoverEventIndex())
			require.Error(t, s.deriveEventBoundsLocked())
			require.Equal(t, uint64(90), s.EventIndex())
			require.Equal(t, uint64(10), s.firstEventIndex.Load())
		})
	}
}

func TestEventRecoveryRetainsErasedAppendProgress(t *testing.T) {
	for name, create := range eventLogTestBackends() {
		for _, eraseAll := range []bool{false, true} {
			label := "tail-erased"
			if eraseAll {
				label = "all-erased"
			}
			t.Run(name+"/"+label, func(t *testing.T) {
				path := t.TempDir()
				log, err := create(path)
				require.NoError(t, err)
				t.Cleanup(func() { _ = log.Close() })
				require.NoError(t, log.Append([]eventlog.Record{
					{ID: 10, Payload: experimentEntry(t, 10, pb.EntryNormal)},
					{ID: 90, Payload: experimentEntry(t, 90, pb.EntryNormal)},
				}))
				_, err = log.Rewrite(context.Background(), 1, func(r eventlog.Record) ([]byte, bool, error) {
					return r.Payload, !eraseAll && r.ID == 10, nil
				})
				require.NoError(t, err)
				require.NoError(t, log.Close())
				if name == "tidwall" {
					log, err = tidwall.Open(path)
				} else {
					log, err = segmented.Open(path, segmentlog.Options{})
				}
				require.NoError(t, err)
				s := &Storage{eventLog: &eventLogBinding{entries: bindEventEntries(log)}}
				require.NoError(t, s.recoverEventIndex())
				require.Equal(t, uint64(90), s.EventIndex())
				require.NoError(t, s.deriveEventBoundsLocked())
				require.Equal(t, uint64(90), s.EventIndex(), "erasure must not rewind replay progress")
				wantFirst := uint64(10)
				if eraseAll {
					wantFirst = 0
				}
				require.Equal(t, wantFirst, s.firstEventIndex.Load())
			})
		}
	}
}
