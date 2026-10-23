package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	twal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// A tidwall log holding an entry over the segmented record limit (committed
// under a raised cap before 0.8.3) is not converted: the error names its raft
// index, and the original log is left in place.
func TestConvertEventLogNamesAnOversizedEntry(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil, nil, nil, WithoutFsync())
	require.NoError(t, err)
	require.NoError(t, s.Close())
	events := datadir.EventsDir(dir)
	require.NoError(t, os.RemoveAll(events))
	log, err := twal.Open(events, &twal.Options{NoSync: true})
	require.NoError(t, err)
	for i, size := range []int{8, segmentlog.MaxRecordBytes} {
		raw, err := proto.Marshal(&pb.Entry{Term: proto.Uint64(1), Index: proto.Uint64(uint64(i + 1)), Type: pb.EntryNormal.Enum(), Data: make([]byte, size)})
		require.NoError(t, err)
		require.NoError(t, log.Write(uint64(i+1), frame(raw)))
	}
	require.NoError(t, log.Close())

	_, err = ConvertEventLog(dir)
	require.ErrorIs(t, err, ErrEntryTooLarge)
	require.ErrorContains(t, err, "raft index 2")
	tidwall, err := isTidwallEventLog(dir)
	require.NoError(t, err)
	require.True(t, tidwall, "the original log stays in place")
}
