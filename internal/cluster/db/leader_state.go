package db

import (
	"sync/atomic"
)

type LeaderState struct {
	leader atomic.Bool
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
