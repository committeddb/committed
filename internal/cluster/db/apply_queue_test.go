package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func entries(from, to uint64) []*raftpb.Entry {
	var out []*raftpb.Entry
	for i := from; i <= to; i++ {
		out = append(out, &raftpb.Entry{Index: proto.Uint64(i)})
	}
	return out
}

func indexes(es []*raftpb.Entry) []uint64 {
	out := make([]uint64, len(es))
	for i, e := range es {
		out[i] = e.GetIndex()
	}
	return out
}

// TestApplyQueue_MergesQueuedBatchesInOrder: a backlog of small Readies
// applies as one batch, in order, so it shares one set of fsyncs.
func TestApplyQueue_MergesQueuedBatchesInOrder(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, 2))
	q.push(entries(3, 3))
	q.push(entries(4, 6))
	b, ok := q.pop()
	require.True(t, ok)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, indexes(b))
	q.done()
	require.True(t, q.drain())
}

// TestApplyQueue_MergeIsCapped: a merge stops before passing maxApplyMerge,
// leaving the rest for the next pop.
func TestApplyQueue_MergeIsCapped(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, maxApplyMerge-1))
	q.push(entries(maxApplyMerge, maxApplyMerge+1))
	b, ok := q.pop()
	require.True(t, ok)
	require.Len(t, b, maxApplyMerge-1)
	q.done()
	b, ok = q.pop()
	require.True(t, ok)
	require.Equal(t, []uint64{maxApplyMerge, maxApplyMerge + 1}, indexes(b))
}

// TestApplyQueue_StopReleasesWaiters: stop ends pop and drain.
func TestApplyQueue_StopReleasesWaiters(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, 1))
	_, ok := q.pop()
	require.True(t, ok)
	drained := make(chan bool)
	go func() { drained <- q.drain() }()
	q.stop()
	require.False(t, <-drained)
	_, ok = q.pop()
	require.False(t, ok)
}
