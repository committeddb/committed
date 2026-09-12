package db

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// fakeReceiver is an EventReceiver over an in-memory event index and
// generation: it records what the loop asked of it. The embedded (nil)
// raftStorage satisfies the Raft's storage field; the loop touches only
// the receiver surface.
type fakeReceiver struct {
	raftStorage

	mu            sync.Mutex
	eventIndex    uint64
	gen           uint64
	snapCompleted uint64
	resets        int
	genSets       []uint64
	adopted       [][]string
	records       int
}

func (f *fakeReceiver) EventIndex() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.eventIndex
}

func (f *fakeReceiver) EventLogGeneration() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gen
}

func (f *fakeReceiver) SnapshotScrubCompleted(*raftpb.Snapshot) (uint64, error) {
	return f.snapCompleted, nil
}

func (f *fakeReceiver) SetEventLogGeneration(gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen = gen
	f.genSets = append(f.genSets, gen)
	return nil
}

func (f *fakeReceiver) ResetEventLog() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.eventIndex, f.gen = 0, 0
	f.resets++
	return nil
}

func (f *fakeReceiver) AppendFetchedRecords([]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records++
	return nil
}

func (f *fakeReceiver) AdoptEventSegments(paths []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adopted = append(f.adopted, paths)
	return nil
}
func (f *fakeReceiver) EventFetchDir() string { return "" }
func (f *fakeReceiver) advance(to uint64)     { f.mu.Lock(); f.eventIndex = to; f.mu.Unlock() }

// fakePeers is a Transport whose FetchEvents answers from a script: each
// call sees the request and returns what a peer would, moving the
// receiver's event index as a real stream would have.
type fakePeers struct {
	Transport
	recv     *fakeReceiver
	mu       sync.Mutex
	requests []EventFetchRequest
	answer   func(req EventFetchRequest, sink EventSink) (EventFetchResult, error)
}

func (p *fakePeers) FetchEvents(_ context.Context, req EventFetchRequest, sink EventSink) (EventFetchResult, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	return p.answer(req, sink)
}

func (p *fakePeers) seen() []EventFetchRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]EventFetchRequest(nil), p.requests...)
}

func newCatchUpRaft(t *testing.T, recv *fakeReceiver, peers *fakePeers) *Raft {
	t.Helper()
	n := &Raft{storage: recv, transport: peers, logger: zap.NewNop(), closeC: make(chan struct{})}
	return n
}

func snapshotAt(index uint64) *raftpb.Snapshot {
	return &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(index)}}
}

// The loop asks for what is missing, pinned to the log's generation once it
// has content and to the snapshot's minimum always, and returns once the
// event log reaches the snapshot's index; a peer that serves nothing new is
// retried after a pause.
func TestCatchUp_FillsToTheSnapshotAndPinsGenerations(t *testing.T) {
	recv := &fakeReceiver{snapCompleted: 3}
	peers := &fakePeers{recv: recv}
	calls := 0
	peers.answer = func(req EventFetchRequest, sink EventSink) (EventFetchResult, error) {
		calls++
		switch calls {
		case 1:
			return EventFetchResult{}, ErrNoPeerToFetchFrom // nobody yet
		case 2:
			require.NoError(t, sink.Begin(5, 100)) // an empty log adopts the peer's generation
			recv.advance(40)
			return EventFetchResult{Peer: 2, EventServeResult: EventServeResult{Generation: 5, EventIndex: 100, LastIndex: 40, More: true}}, nil
		default:
			require.NoError(t, sink.Begin(5, 100))
			recv.advance(100)
			return EventFetchResult{Peer: 3, EventServeResult: EventServeResult{Generation: 5, EventIndex: 100, LastIndex: 100}}, nil
		}
	}
	n := newCatchUpRaft(t, recv, peers)

	needIndex, needGen, needed := n.snapshotNeedsCatchUp(snapshotAt(100))
	require.True(t, needed)
	require.Equal(t, uint64(100), needIndex)
	require.Equal(t, uint64(3), needGen)
	require.True(t, n.catchUpEventLog(snapshotAt(100), needIndex, needGen))

	seen := peers.seen()
	require.Len(t, seen, 3)
	require.Equal(t, EventFetchRequest{After: 0, To: 100, MinGeneration: 3}, seen[0], "an empty log pins nothing but the minimum")
	require.Equal(t, EventFetchRequest{After: 40, To: 100, Generation: 5, MinGeneration: 3}, seen[2], "with content, the generation is pinned")
	require.Equal(t, []uint64{5}, recv.genSets, "adopted once, into the empty log; pinned thereafter")
	require.Zero(t, recv.resets)
	_, active := n.CatchUp()
	require.False(t, active, "the status clears when the catch-up ends")
}

// A log whose generation predates the snapshot's completed scrub is
// discarded before anything is fetched; a peer at a newer generation than
// the log's makes the loop discard the log and fetch it whole; a peer at
// an older one is skipped.
func TestCatchUp_DiscardsAStaleGenerationLog(t *testing.T) {
	recv := &fakeReceiver{eventIndex: 30, gen: 2, snapCompleted: 4}
	peers := &fakePeers{recv: recv}
	calls := 0
	peers.answer = func(req EventFetchRequest, sink EventSink) (EventFetchResult, error) {
		calls++
		require.NoError(t, sink.Begin(6, 100))
		recv.advance(100)
		return EventFetchResult{Peer: 2, EventServeResult: EventServeResult{Generation: 6, EventIndex: 100, LastIndex: 100}}, nil
	}
	n := newCatchUpRaft(t, recv, peers)
	_, needGen, needed := n.snapshotNeedsCatchUp(snapshotAt(100))
	require.True(t, needed)
	require.True(t, n.catchUpEventLog(snapshotAt(100), 100, needGen))
	require.Equal(t, 1, recv.resets, "content at generation 2 cannot be brought to the snapshot's 4: fetched whole")
	require.Equal(t, EventFetchRequest{After: 0, To: 100, MinGeneration: 4}, peers.seen()[0])
	require.Equal(t, uint64(6), recv.EventLogGeneration())

	// Content at the snapshot's generation, but every peer has moved on.
	recv = &fakeReceiver{eventIndex: 30, gen: 4, snapCompleted: 4}
	peers = &fakePeers{recv: recv}
	calls = 0
	peers.answer = func(req EventFetchRequest, sink EventSink) (EventFetchResult, error) {
		calls++
		if calls == 1 {
			return EventFetchResult{Peer: 2, EventServeResult: EventServeResult{Generation: 7}}, &EventGenerationMismatch{Peer: 2, Have: 7, Want: 4}
		}
		require.NoError(t, sink.Begin(7, 100))
		recv.advance(100)
		return EventFetchResult{Peer: 2, EventServeResult: EventServeResult{Generation: 7, EventIndex: 100, LastIndex: 100}}, nil
	}
	n = newCatchUpRaft(t, recv, peers)
	require.True(t, n.catchUpEventLog(snapshotAt(100), 100, 4))
	require.Equal(t, 1, recv.resets, "a newer peer generation: the log is fetched whole at it")
	require.Equal(t, uint64(7), recv.EventLogGeneration())

	// A peer behind this node's generation is skipped, not adopted.
	recv = &fakeReceiver{eventIndex: 30, gen: 4, snapCompleted: 4}
	peers = &fakePeers{recv: recv}
	calls = 0
	peers.answer = func(req EventFetchRequest, sink EventSink) (EventFetchResult, error) {
		calls++
		if calls == 1 {
			return EventFetchResult{Peer: 3}, &EventGenerationMismatch{Peer: 3, Have: 2, Want: 4}
		}
		require.NoError(t, sink.Begin(4, 100))
		recv.advance(100)
		return EventFetchResult{Peer: 2, EventServeResult: EventServeResult{Generation: 4, EventIndex: 100, LastIndex: 100}}, nil
	}
	n = newCatchUpRaft(t, recv, peers)
	require.True(t, n.catchUpEventLog(snapshotAt(100), 100, 4))
	require.Zero(t, recv.resets)
	require.Equal(t, uint64(4), recv.EventLogGeneration())
}

// Closing the node ends a catch-up that is waiting on peers; the status
// reports progress while it runs.
func TestCatchUp_StopsOnCloseAndReportsProgress(t *testing.T) {
	recv := &fakeReceiver{}
	peers := &fakePeers{recv: recv}
	started := make(chan struct{})
	var once sync.Once
	peers.answer = func(req EventFetchRequest, _ EventSink) (EventFetchResult, error) {
		once.Do(func() { close(started) })
		return EventFetchResult{}, errors.New("unreachable")
	}
	n := newCatchUpRaft(t, recv, peers)

	done := make(chan bool, 1)
	go func() { done <- n.catchUpEventLog(snapshotAt(100), 100, 0) }()
	<-started
	st, active := n.CatchUp()
	require.True(t, active)
	require.Equal(t, uint64(100), st.Need)
	require.False(t, st.Since.IsZero())

	close(n.closeC)
	select {
	case ok := <-done:
		require.False(t, ok, "a closing node does not claim to have caught up")
	case <-time.After(5 * time.Second):
		t.Fatal("catch-up did not stop on close")
	}
}

// The receiving sink pins the stream's generation: an empty log adopts it
// durably before any content, a log with content refuses another.
func TestFetchSink_Begin(t *testing.T) {
	recv := &fakeReceiver{}
	sink := &fetchSink{recv: recv, dir: t.TempDir(), logger: zap.NewNop()}
	require.NoError(t, sink.Begin(9, 100))
	require.Equal(t, []uint64{9}, recv.genSets)

	recv.advance(10)
	var mismatch *EventGenerationMismatch
	require.ErrorAs(t, sink.Begin(11, 100), &mismatch)
	require.Equal(t, &EventGenerationMismatch{Have: 11, Want: 9}, mismatch)
	require.NoError(t, sink.Begin(9, 100))
}

// eventIndexOnly is a storage with an event index but no receiver surface —
// the shape of the in-memory doubles.
type eventIndexOnly struct{ raftStorage }

func (eventIndexOnly) EventIndex() uint64 { return 0 }

// The receiving sink stages segments and adopts them in one batch — at the
// end of the stream, or before the records that must follow them — so a
// stream of many segments costs one reopen, not one per file.
func TestFetchSink_AdoptsSegmentsInBatches(t *testing.T) {
	recv := &fakeReceiver{}
	dir := t.TempDir()
	sink := &fetchSink{recv: recv, dir: dir, logger: zap.NewNop()}
	require.NoError(t, sink.Begin(1, 100))
	seg := func(name string) {
		require.NoError(t, sink.Segment(name, 3, strings.NewReader("abc")))
	}
	seg("00000000000000000001")
	seg("00000000000000000010")
	require.Empty(t, recv.adopted, "nothing adopted until the batch closes")
	require.NoError(t, sink.Records([]byte("run")))
	require.Len(t, recv.adopted, 1)
	require.Equal(t, []string{filepath.Join(dir, "00000000000000000001"), filepath.Join(dir, "00000000000000000010")}, recv.adopted[0],
		"the segments before a run of records go in first, together")
	require.Equal(t, 1, recv.records)
	seg("00000000000000000020")
	require.NoError(t, sink.End(EventServeResult{}))
	require.Len(t, recv.adopted, 2)
	require.Equal(t, []string{filepath.Join(dir, "00000000000000000020")}, recv.adopted[1], "the rest at the end")
	require.NoError(t, sink.End(EventServeResult{}))
	require.Len(t, recv.adopted, 2, "an empty batch is nothing")

	// A stream cut after staging never reaches End; the next stream owns
	// its own files and the cut one's are gone.
	seg("00000000000000000030")
	stale := filepath.Join(dir, "00000000000000000030")
	require.FileExists(t, stale)
	require.NoError(t, sink.Begin(1, 100))
	require.NoFileExists(t, stale, "a cut stream's staged file is discarded")
	seg("00000000000000000040")
	require.NoError(t, sink.End(EventServeResult{}))
	require.Equal(t, []string{filepath.Join(dir, "00000000000000000040")}, recv.adopted[2], "only this stream's segment is adopted")
}

// A storage without an event log cannot catch up: the loop declines and the
// caller keeps the fail-fast path.
func TestCatchUp_UnavailableWithoutAnEventLog(t *testing.T) {
	n := &Raft{storage: eventIndexOnly{}, logger: zap.NewNop(), closeC: make(chan struct{})}
	_, _, needed := n.snapshotNeedsCatchUp(snapshotAt(100))
	require.True(t, needed, "an in-memory storage's event index is 0")
	require.False(t, n.catchUpEventLog(snapshotAt(100), 100, 0))
}
