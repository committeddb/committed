package db

import (
	"sync"
	"sync/atomic"
)

// LeaderTransition captures an observed change in the raft leader ID.
// Old is the leader this node previously believed in (0 = none known);
// New is the leader it now observes (0 = none known). SetLeaderID only
// dispatches a transition when New differs from Old.
type LeaderTransition struct {
	Old uint64
	New uint64
}

// LeaderState tracks what this node believes about the current raft
// leader. It exposes two orthogonal views of the same fact because they
// have different callers:
//
//   - IsLeader / SetLeader answer "is THIS node the leader right now?"
//     Used by the HTTP layer (whether to accept writes vs. forward) and
//     by the metrics gauge.
//   - Leader / SetLeaderID answer "what ID is this node's raft client
//     currently pointing at?" Used by the propose fail-fast path, which
//     stamps in-flight waiters with the leader at submission time and
//     signals them after a later transition.
//
// The two are updated independently by the raft Ready loop (both come
// from n.node.Status() but encode different bits of the snapshot). A
// zero-value LeaderState is usable — callers can Store without explicit
// construction.
type LeaderState struct {
	leader   atomic.Bool
	leaderID atomic.Uint64

	subsMu      sync.Mutex
	subscribers []chan LeaderTransition
}

func NewLeaderState(leader bool) *LeaderState {
	l := &LeaderState{}

	l.leader.Store(leader)

	return l
}

func (s *LeaderState) SetLeader(leader bool) {
	s.leader.Store(leader)
}

func (s *LeaderState) IsLeader() bool {
	return s.leader.Load()
}

// SetLeaderID records id as the currently-observed raft leader. A call
// with id equal to the previously-stored value is a no-op (no subscriber
// dispatch). When id differs, every subscriber receives a
// LeaderTransition on a best-effort, non-blocking basis: the channel is
// buffered, and a full buffer drops the event rather than blocking the
// caller (the raft Ready loop). id == 0 is a valid "no leader known"
// observation and is recorded and dispatched like any other change.
func (s *LeaderState) SetLeaderID(id uint64) {
	old := s.leaderID.Swap(id)
	if old == id {
		return
	}

	s.subsMu.Lock()
	subs := s.subscribers
	s.subsMu.Unlock()

	t := LeaderTransition{Old: old, New: id}
	for _, c := range subs {
		select {
		case c <- t:
		default:
		}
	}
}

// Leader returns the raft node ID this LeaderState most recently
// observed as leader, or 0 if no leader has been observed since
// construction.
func (s *LeaderState) Leader() uint64 {
	return s.leaderID.Load()
}

// Subscribe registers a buffered channel to receive LeaderTransition
// events when SetLeaderID records an actual change. Subscribers remain
// registered for the lifetime of the LeaderState — there is no
// explicit unsubscribe, but a slow consumer only loses transitions
// (sends are non-blocking with a default branch), never blocks the
// publisher. buffer is the channel capacity; callers that just need
// "did something change at all" since the last read can pass 1, while
// callers that care about seeing every transition should pass a
// larger value.
func (s *LeaderState) Subscribe(buffer int) <-chan LeaderTransition {
	ch := make(chan LeaderTransition, buffer)

	s.subsMu.Lock()
	s.subscribers = append(s.subscribers, ch)
	s.subsMu.Unlock()

	return ch
}
