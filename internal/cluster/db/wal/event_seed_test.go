package wal

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/pkg/segmentlog"
)

// compressionTestEntry is a compressible, committed-shaped event at index i.
func compressionTestEntry(i int) *pb.Entry {
	data := []byte(fmt.Sprintf(`{"entity_id":%d,"entity_state":"active","padding":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i))
	return &pb.Entry{
		Term:  proto.Uint64(1),
		Index: proto.Uint64(uint64(i)),
		Type:  pb.EntryNormal.Enum(),
		Data:  data,
	}
}

func seedEventLog(t *testing.T, s *Storage, from, n int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		require.NoError(t, s.appendEvents([]*pb.Entry{compressionTestEntry(i)}))
	}
}

// openSmallSegments opens a node whose event log seals every 2 KiB, so a few
// hundred events span many segments; sealer is the background sealer's idle
// interval.
func openSmallSegments(t *testing.T, dir string, sealer time.Duration) *Storage {
	t.Helper()
	s, err := Open(dir, nil, nil, nil, WithoutFsync(), WithSealerIdleInterval(sealer), WithSegmentedEventLog(segmentlog.LogOptions{
		SegmentBytes: 2048,
		Encoding:     segmentlog.Options{Compression: segmentlog.ZstdDefault},
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// compressOneSealed takes one sealer step by hand.
func compressOneSealed(t *testing.T, s *Storage) {
	t.Helper()
	s.eventMu.RLock()
	compressor := s.eventLog.compressor
	s.eventMu.RUnlock()
	did, err := compressor.CompressNextSealed()
	require.NoError(t, err)
	require.True(t, did, "the seeded log must hold a sealed range to compress")
}

// countCompressedSegments counts the event log's compressed segment files.
func countCompressedSegments(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "segment-") {
			n++
		}
	}
	return n
}
