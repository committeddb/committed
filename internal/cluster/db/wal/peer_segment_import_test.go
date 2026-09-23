package wal

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Interrupt after one segment has been imported. The next exchange must use
// the receiver's durable prefix even though the first stream never reached End.
type interruptedSegmentSink struct {
	*receiverSink
	segments int
	failure  error
}

func (s *interruptedSegmentSink) Segment(name string, size int64, r io.Reader) error {
	s.segments++
	if s.segments == 2 {
		return s.failure
	}
	return s.receiverSink.Segment(name, size, r)
}

func TestNativePeerServesSharedReceivers(t *testing.T) {
	peer := openFetchPeer(t, t.TempDir(), time.Hour)
	seedEventLog(t, peer, 1, 40)
	compressOldestSealed(t, peer)
	release := peer.FreezeEventLayout()
	layout, err := peer.EventLayout()
	require.NoError(t, err)
	require.Greater(t, len(layout.Sealed), 1)
	require.True(t, layout.Sealed[0].Compressed)
	release()
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			opener := storageTestOpeners()[name]
			target, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = target.Close() })
			release := target.BeginCatchUp()
			defer release()
			failure := errors.New("peer stream interrupted")
			sink := &interruptedSegmentSink{receiverSink: &receiverSink{recv: target}, failure: failure}
			_, err = peer.ServeEvents(t.Context(), 0, peer.EventIndex(), sink)
			require.ErrorIs(t, err, failure)
			prefix := target.EventIndex()
			require.Positive(t, prefix)
			require.Less(t, prefix, peer.EventIndex())
			release()
			require.NoError(t, target.Close())
			target, err = openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			require.Equal(t, prefix, target.EventIndex(), "failed exchange still leaves a durable prefix")
			release = target.BeginCatchUp()
			defer release()
			catchUp(t, peer, target, peer.EventIndex())
			// Re-delivery of complete files overlaps all existing records.
			require.NoError(t, target.AdoptEventSegments(stage(t, layout.Sealed)))
			staged, err := os.ReadDir(target.EventFetchDir())
			require.NoError(t, err)
			require.Empty(t, staged)
			release()
			require.NoError(t, target.Close())
			resumed, err := openStorage(path, nil, nil, nil, opener, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = resumed.Close() })
			require.Equal(t, peer.EventIndex(), resumed.EventIndex())
			want, got := peer.eventLog.records(), resumed.eventLog.records()
			defer func() { _ = want.Close() }()
			defer func() { _ = got.Close() }()
			for index := uint64(1); index <= peer.EventIndex(); index++ {
				expected, err := want.Seek(index)
				require.NoError(t, err)
				actual, err := got.Seek(index)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
			}
		})
	}
}

func TestSharedSegmentImportValidatesBeforeAppendAndRetries(t *testing.T) {
	for _, name := range []string{"tidwall", "segmented", "segmented-cached"} {
		t.Run(name, func(t *testing.T) {
			target, err := openStorage(t.TempDir(), nil, nil, nil, storageTestOpeners()[name], WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = target.Close() })
			dir := t.TempDir()
			first := filepath.Join(dir, "00000000000000000001")
			second := filepath.Join(dir, "00000000000000000002")
			ten := experimentEntry(t, 10, pb.EntryNormal)
			twenty := experimentEntry(t, 20, pb.EntryNormal)
			require.NoError(t, os.WriteFile(first, fetchedStream(frame(ten)), 0o600))
			// A complete valid record followed by a corrupt record must not publish
			// any part of this file. Earlier complete files remain retryable progress.
			require.NoError(t, os.WriteFile(second, fetchedStream(frame(twenty), []byte("bad frame")), 0o600))
			release := target.FreezeEventLayout()
			require.ErrorIs(t, target.AdoptEventSegments([]string{first}), ErrLayoutFrozen)
			require.Zero(t, target.EventIndex())
			release()
			require.Error(t, target.AdoptEventSegments([]string{first, second}))
			require.Equal(t, uint64(10), target.EventIndex())
			require.NoFileExists(t, first)
			require.FileExists(t, second)
			require.NoError(t, os.WriteFile(first, fetchedStream(frame(ten)), 0o600))
			require.NoError(t, os.WriteFile(second, fetchedStream(frame(twenty)), 0o600))
			require.NoError(t, target.AdoptEventSegments([]string{first, second}))
			require.Equal(t, uint64(20), target.EventIndex())
			require.NoFileExists(t, first)
			require.NoFileExists(t, second)
		})
	}
}
