//go:build adversarial

package db_test

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/wal"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Every node, joiner, and restart in a scenario uses the same options.
func forLifecycleBackends(t *testing.T, scenario func(*testing.T, []wal.Option)) {
	t.Helper()
	t.Run("tidwall", func(t *testing.T) { scenario(t, nil) })
	t.Run("segmented", func(t *testing.T) {
		scenario(t, []wal.Option{wal.WithSegmentedEventLog(segmentlog.LogOptions{
			SegmentBytes: 2048,
			Encoding:     segmentlog.Options{Compression: segmentlog.ZstdDefault},
			Cache:        segmentlog.CacheOptions{RecentBytes: 8192, HistoricalBytes: 8192},
		})})
	})
}

// Compare logical IDs and original protobuf bytes. Native physical sequences
// are not a valid way to inspect the segmented engine after a scrub.
func compareSegmentedEventPrefix(t *testing.T, dirs []string) bool {
	t.Helper()
	recognized, err := segmentlog.RecognizeDirectory(filepath.Join(dirs[0], "events"))
	require.NoError(t, err)
	if !recognized {
		return false
	}
	logs := make([]*segmented.Log, 0, len(dirs))
	through := ^uint64(0)
	for _, dir := range dirs {
		log, err := segmented.Open(filepath.Join(dir, "events"), segmentlog.Options{})
		require.NoError(t, err)
		defer func() { _ = log.Close() }()
		logs = append(logs, log)
		last, exists, err := log.LastAppended()
		require.NoError(t, err)
		require.True(t, exists)
		through = min(through, last)
	}
	var expected []eventlog.Record
	for i, log := range logs {
		var records []eventlog.Record
		require.NoError(t, log.Scan(t.Context(), eventlog.Coverage{Start: 1, End: through + 1}, func(r eventlog.Record) error {
			records = append(records, eventlog.Record{ID: r.ID, Payload: bytes.Clone(r.Payload)})
			return nil
		}))
		require.NotEmpty(t, records)
		if i == 0 {
			expected = records
		} else {
			require.Equal(t, expected, records)
		}
	}
	return true
}

func TestAdversarial_StorageLeaderRestart(t *testing.T) {
	forLifecycleBackends(t, func(t *testing.T, options []wal.Option) {
		nodes, _, dirs, fatalC := newSevereLagCluster(t, 3, nil, options...)
		t.Cleanup(func() {
			for _, node := range nodes {
				_ = node.Close()
			}
			for _, node := range nodes {
				_ = node.storage.Close()
			}
		})
		nodes.WaitForLeader(t)
		var seq uint64
		proposeBurst(t, nodes, &seq, 5)
		leader := nodes.LeaderRaft()
		index := 0
		var survivors Rafts
		for i, node := range nodes {
			if node == leader {
				index = i
			} else {
				survivors = append(survivors, node)
			}
		}
		require.NoError(t, leader.Close())
		require.NoError(t, leader.storage.Close())
		survivors.WaitForLeader(t)
		require.NotEqual(t, leader.id, survivors.LeaderRaft().id)
		proposeBurst(t, survivors, &seq, 5)
		rebootWalNode(t, leader, dirs[index], nil, fatalC, options...)
		waitForLeaderExtended(t, nodes, 15*time.Second)
		proposeBurst(t, nodes, &seq, 3)
		waitForSurvivorConvergence(t, nodes, 20*time.Second)
		requireNoFatal(t, fatalC)
	})
}
