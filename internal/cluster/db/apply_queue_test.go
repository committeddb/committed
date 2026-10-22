package db

import (
	"testing"
	"time"

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

func applyResp(last uint64) []*raftpb.Message {
	return []*raftpb.Message{{Type: raftpb.MsgStorageApplyResp.Enum(), Index: proto.Uint64(last)}}
}

// TestApplyQueue_MergesQueuedBatchesInOrder: a backlog of small Readies
// applies as one batch, in order, so it shares one set of fsyncs; every
// batch's responses come along, in order.
func TestApplyQueue_MergesQueuedBatchesInOrder(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, 2), applyResp(2))
	q.push(entries(3, 3), applyResp(3))
	q.push(entries(4, 6), applyResp(6))
	b, ok := q.pop()
	require.True(t, ok)
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, indexes(b.entries))
	require.Len(t, b.responses, 3)
	for i, want := range []uint64{2, 3, 6} {
		require.Equal(t, want, b.responses[i].GetIndex())
	}
	q.done()
	require.True(t, q.exclusive(func() {}))
}

// TestApplyQueue_MergeIsCapped: a merge stops before passing maxApplyMerge,
// leaving the rest for the next pop.
func TestApplyQueue_MergeIsCapped(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, maxApplyMerge-1), nil)
	q.push(entries(maxApplyMerge, maxApplyMerge+1), nil)
	b, ok := q.pop()
	require.True(t, ok)
	require.Len(t, b.entries, maxApplyMerge-1)
	q.done()
	b, ok = q.pop()
	require.True(t, ok)
	require.Equal(t, []uint64{maxApplyMerge, maxApplyMerge + 1}, indexes(b.entries))
}

// TestApplyQueue_ExclusiveWaitsForApplyAndHoldsItOff: exclusive runs only
// once the batch being applied is done, and pop takes nothing while it runs.
func TestApplyQueue_ExclusiveWaitsForApplyAndHoldsItOff(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, 1), nil)
	_, ok := q.pop()
	require.True(t, ok)

	ran := make(chan struct{})
	release := make(chan struct{})
	go q.exclusive(func() {
		close(ran)
		<-release
	})
	select {
	case <-ran:
		t.Fatal("exclusive ran while a batch was being applied")
	case <-time.After(50 * time.Millisecond):
	}
	q.done()
	<-ran

	q.push(entries(2, 2), nil)
	popped := make(chan struct{})
	go func() {
		_, _ = q.pop()
		close(popped)
	}()
	select {
	case <-popped:
		t.Fatal("pop took a batch while exclusive ran")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-popped
}

// TestApplyQueue_StopReleasesWaiters: stop ends pop and exclusive.
func TestApplyQueue_StopReleasesWaiters(t *testing.T) {
	q := newApplyQueue()
	q.push(entries(1, 1), nil)
	_, ok := q.pop()
	require.True(t, ok)
	ran := false
	result := make(chan bool)
	go func() { result <- q.exclusive(func() { ran = true }) }()
	q.stop()
	require.False(t, <-result)
	require.False(t, ran)
	_, ok = q.pop()
	require.False(t, ok)
}

func appendMsg(term uint64, ents ...*raftpb.Entry) *raftpb.Message {
	m := &raftpb.Message{Type: raftpb.MsgStorageAppend.Enum(), Entries: ents}
	if term > 0 {
		m.Term, m.Vote, m.Commit = proto.Uint64(term), proto.Uint64(1), proto.Uint64(0)
	}
	return m
}

func snapshotMsg(index uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:     raftpb.MsgStorageAppend.Enum(),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(index)}},
	}
}

// TestAppendQueue_TakesTheQueueUpToASnapshot: pop takes every queued message
// as one write, takes a snapshot message alone, and ends a run before one.
func TestAppendQueue_TakesTheQueueUpToASnapshot(t *testing.T) {
	q := newAppendQueue(1)
	q.push(appendMsg(0, entries(1, 1)...))
	q.push(appendMsg(0, entries(2, 2)...))
	q.push(snapshotMsg(10))
	q.push(appendMsg(0, entries(11, 11)...))

	msgs, ok := q.pop()
	require.True(t, ok)
	require.Len(t, msgs, 2)
	msgs, ok = q.pop()
	require.True(t, ok)
	require.Len(t, msgs, 1)
	require.True(t, hasSnapshot(msgs[0]))
	msgs, ok = q.pop()
	require.True(t, ok)
	require.Len(t, msgs, 1)
	require.Equal(t, uint64(11), msgs[0].GetEntries()[0].GetIndex())
}

// TestAppendQueue_NudgeAndStop: a nudge wakes pop with nothing to write, and
// stop ends it.
func TestAppendQueue_NudgeAndStop(t *testing.T) {
	q := newAppendQueue(1)
	q.nudge()
	msgs, ok := q.pop()
	require.True(t, ok)
	require.Empty(t, msgs)
	q.push(appendMsg(0, entries(1, 1)...))
	q.stop()
	_, ok = q.pop()
	require.False(t, ok)
}

// TestAppendQueue_OnlyOwnVoteHoldsTheElectionClock: a write carrying this
// node's vote (or pre-vote) for itself holds the election clock until it is
// finished; ordinary writes — a follower's entries and acknowledgements, a
// vote granted to another node — do not.
func TestAppendQueue_OnlyOwnVoteHoldsTheElectionClock(t *testing.T) {
	withResponse := func(typ raftpb.MessageType, to uint64) *raftpb.Message {
		m := appendMsg(0, entries(1, 1)...)
		m.Responses = []*raftpb.Message{{Type: typ.Enum(), To: proto.Uint64(to)}}
		return m
	}
	q := newAppendQueue(1)
	q.push(withResponse(raftpb.MessageType_MsgAppResp, 2))
	q.push(withResponse(raftpb.MessageType_MsgVoteResp, 3))
	require.False(t, q.ownVotePending(), "entries and a vote for another node hold nothing")

	for _, typ := range []raftpb.MessageType{raftpb.MessageType_MsgVoteResp, raftpb.MessageType_MsgPreVoteResp} {
		q.push(withResponse(typ, 1))
		require.True(t, q.ownVotePending(), typ.String())
		msgs, ok := q.pop()
		require.True(t, ok)
		require.True(t, q.ownVotePending(), "taken but not yet written")
		q.finished(msgs)
		require.False(t, q.ownVotePending(), typ.String())
	}
}

// TestMergeAppends_KeepsTheLatestHardStateAndRaftsLog: merged appends carry
// the latest HardState, and a later append starting at or below the earlier
// tail replaces it, as raft's log does.
func TestMergeAppends_KeepsTheLatestHardStateAndRaftsLog(t *testing.T) {
	hs, ents := mergeAppends([]*raftpb.Message{
		appendMsg(2, entries(1, 5)...),
		appendMsg(0, entries(6, 7)...),
		appendMsg(3, entries(4, 4)...),
		appendMsg(0),
	})
	require.Equal(t, uint64(3), hs.GetTerm())
	require.Equal(t, []uint64{1, 2, 3, 4}, indexes(ents))

	hs, ents = mergeAppends([]*raftpb.Message{appendMsg(0, entries(1, 2)...)})
	require.Nil(t, hs.Term)
	require.Equal(t, []uint64{1, 2}, indexes(ents))
}

// TestDurableCommit_WaitsForTheCommit: wait returns once the commit is
// durable, and false if stopped first.
func TestDurableCommit_WaitsForTheCommit(t *testing.T) {
	d := newDurableCommit(5)
	require.True(t, d.wait(5))

	done := make(chan bool)
	go func() { done <- d.wait(7) }()
	d.advance(6)
	select {
	case <-done:
		t.Fatal("wait returned before the commit was durable")
	case <-time.After(50 * time.Millisecond):
	}
	d.advance(7)
	require.True(t, <-done)

	go func() { done <- d.wait(9) }()
	d.stop()
	require.False(t, <-done)
}
