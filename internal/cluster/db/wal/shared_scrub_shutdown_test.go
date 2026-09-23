package wal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

type interruptedScrubLog struct {
	eventlog.EventLog
	phase   string
	entered chan struct{}
	once    sync.Once
}

func (l *interruptedScrubLog) RewriteWithPublicationLock(ctx context.Context, generation uint64, transform eventlog.Transform, lock sync.Locker) (eventlog.RewriteResult, error) {
	if l.phase != "rewrite" {
		return l.EventLog.RewriteWithPublicationLock(ctx, generation, transform, lock)
	}
	return l.EventLog.RewriteWithPublicationLock(ctx, generation, func(record eventlog.Record) ([]byte, bool, error) {
		l.once.Do(func() { close(l.entered) })
		<-ctx.Done()
		return nil, false, ctx.Err()
	}, lock)
}

func (l *interruptedScrubLog) Reclaim(ctx context.Context) (eventlog.ReclaimResult, error) {
	if l.phase == "reclaim" {
		l.once.Do(func() { close(l.entered) })
		<-ctx.Done()
	}
	return l.EventLog.Reclaim(ctx)
}

func TestSharedScrubShutdownDuringStorageWork(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, phase := range []string{"rewrite", "reclaim"} {
			t.Run(name+"/"+phase, func(t *testing.T) {
				path := t.TempDir()
				opener := storageTestOpeners()[name]
				entered := make(chan struct{})
				wrapped := func(path string, m *metrics.Metrics, options tidwall.LegacyOptions) (*eventLogBinding, error) {
					binding, err := opener(path, m, options)
					if err == nil {
						binding.managed = &interruptedScrubLog{EventLog: binding.managed, phase: phase, entered: entered}
					}
					return binding, err
				}
				s, err := openStorage(path, nil, nil, nil, wrapped)
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				stageSharedScrubCommand(t, s)
				require.NoError(t, s.SetAppliedIndexForTest(2))
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("worker did not reach storage work")
				}
				closed := make(chan error, 1)
				go func() { closed <- s.Close() }()
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(5 * time.Second):
					t.Fatal("shutdown did not cancel active storage work")
				}
				require.Zero(t, s.lastScrubbedBound.Load(), "interrupted work is not completion")
				resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
				require.NoError(t, err)
				t.Cleanup(func() { _ = resumed.Close() })
				generation, err := resumed.eventLog.managed.Generation()
				require.NoError(t, err)
				expected := uint64(0)
				if phase == "reclaim" {
					expected = 1
				}
				require.Equal(t, expected, generation)
				require.NoError(t, resumed.runPendingScrub())
				require.Equal(t, uint64(1), resumed.lastScrubbedBound.Load())
			})
		}
	}
}

func TestStoragePolicyScanStopsBetweenRecords(t *testing.T) {
	for name, opener := range storageTestOpeners() {
		t.Run(name, func(t *testing.T) {
			binding, err := opener(t.TempDir(), nil, tidwall.LegacyOptions{})
			require.NoError(t, err)
			t.Cleanup(func() { _ = binding.Close() })
			require.NoError(t, binding.entries.Append([]eventlog.Record{
				{ID: 1, Payload: experimentEntry(t, 1, pb.EntryNormal)},
				{ID: 2, Payload: experimentEntry(t, 2, pb.EntryNormal)},
			}))
			s := &Storage{eventLog: binding, scrubStop: make(chan struct{})}
			visits := 0
			err = s.scanEventEntries(2, func(*pb.Entry) error {
				visits++
				close(s.scrubStop)
				return nil
			})
			require.ErrorIs(t, err, errScrubStopped)
			require.Equal(t, 1, visits)
			require.ErrorIs(t, s.scanEventEntries(2, func(*pb.Entry) error {
				t.Fatal("stopped scan must not start again")
				return nil
			}), errScrubStopped)
		})
	}
}
