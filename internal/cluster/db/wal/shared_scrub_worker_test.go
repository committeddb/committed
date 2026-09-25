package wal

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

type observedScrubGeneration struct {
	eventlog.EventLog
	queries atomic.Int64
	failure error
}

func (l *observedScrubGeneration) Generation() (uint64, error) {
	l.queries.Add(1)
	if l.failure != nil {
		return 0, l.failure
	}
	return l.EventLog.Generation()
}

// Stage the exact apply window: event bytes and the pending command are durable,
// but the enclosing apply has not yet advanced its watermark or returned.
func stageSharedScrubCommand(t *testing.T, s *Storage) {
	t.Helper()
	command, err := cluster.NewScrubEntity(1, false)
	require.NoError(t, err)
	data, err := (&cluster.Proposal{Entities: []*cluster.Entity{command}}).Marshal()
	require.NoError(t, err)
	entries := []*pb.Entry{
		{Index: proto.Uint64(1), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum()},
		{Index: proto.Uint64(2), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum(), Data: data},
	}
	require.NoError(t, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: proto.Uint64(2)}, entries, &pb.Snapshot{}))
	require.NoError(t, s.appendEvents(entries))
	require.NoError(t, s.SetAppliedIndexForTest(1))
	require.NoError(t, s.handleScrub(command, 2))
}

func TestSharedScrubWorkerRetriesAdmission(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		for _, condition := range []string{"protected-reader", "layout-freeze", "apply-watermark", "shutdown"} {
			t.Run(name+"/"+condition, func(t *testing.T) {
				opener := storageTestOpeners()[name]
				var observed *observedScrubGeneration
				wrapped := func(path string, m *metrics.Metrics, options tidwall.LegacyOptions) (*eventLogBinding, error) {
					binding, err := opener(path, m, options)
					if err == nil {
						observed = &observedScrubGeneration{EventLog: binding.managed}
						binding.managed = observed
					}
					return binding, err
				}
				s, err := openStorage(t.TempDir(), nil, nil, nil, wrapped)
				require.NoError(t, err)
				t.Cleanup(func() { _ = s.Close() })
				release := func() {}
				switch condition {
				case "protected-reader", "shutdown":
					release = s.BeginFromZeroRead()
				case "layout-freeze":
					release = s.FreezeEventLayout()
				}
				defer release()
				stageSharedScrubCommand(t, s)
				if condition != "apply-watermark" {
					require.NoError(t, s.SetAppliedIndexForTest(2))
				}
				require.Eventually(t, func() bool { return observed.queries.Load() >= 4 }, 5*time.Second, 5*time.Millisecond,
					"worker must retry temporary admission failure without another command")
				require.Zero(t, s.lastScrubbedBound.Load())
				if condition == "shutdown" {
					closed := make(chan error, 1)
					go func() { closed <- s.Close() }()
					select {
					case err := <-closed:
						require.NoError(t, err)
					case <-time.After(5 * time.Second):
						t.Fatal("shutdown waited for a protected reader instead of canceling retry")
					}
					return
				}
				release()
				if condition == "apply-watermark" {
					require.NoError(t, s.SetAppliedIndexForTest(2))
				}
				require.Eventually(t, func() bool { return s.lastScrubbedBound.Load() == 1 }, 5*time.Second, 5*time.Millisecond,
					"clearing the blocker must suffice; no new command or explicit signal")
			})
		}
	}
}

func TestSharedScrubWorkerResumesOnOpen(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			s, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			stageSharedScrubCommand(t, s)
			require.NoError(t, s.SetAppliedIndexForTest(2))
			require.NoError(t, s.Close())
			resumed, err := openStorage(path, nil, nil, nil, opener)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resumed.Close() })
			require.Eventually(t, func() bool { return resumed.lastScrubbedBound.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
		})
	}
}

func TestSharedScrubWorkerDoesNotRetryBackendFailure(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			opener := storageTestOpeners()[name]
			var observed *observedScrubGeneration
			wrapped := func(path string, m *metrics.Metrics, options tidwall.LegacyOptions) (*eventLogBinding, error) {
				binding, err := opener(path, m, options)
				if err == nil {
					observed = &observedScrubGeneration{EventLog: binding.managed, failure: eventlog.ErrPoisoned}
					binding.managed = observed
				}
				return binding, err
			}
			s, err := openStorage(t.TempDir(), nil, nil, nil, wrapped)
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			require.Eventually(t, func() bool { return observed.queries.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
			require.Never(t, func() bool { return observed.queries.Load() > 1 }, 200*time.Millisecond, 5*time.Millisecond,
				"a backend requiring reopen must not enter the admission retry loop")
		})
	}
}
