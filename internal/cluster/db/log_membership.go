package db

import (
	"fmt"
	"maps"

	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// LogSource is what LogMembership reads from a stopped node's storage.
type LogSource interface {
	InitialState() (*raftpb.HardState, *raftpb.ConfState, error)
	AppliedIndex() uint64
	LastIndex() (uint64, error)
	Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error)
	RemovedMembers() map[uint64]uint64
	MembershipGuardIndex() (uint64, bool)
}

// LogMembership is the membership a node's log holds through its last index,
// committed or not: the configuration as of the applied index with every
// membership change after it applied the way the node would apply it (the
// apply-time guard included). Restore and recover read it offline — the
// metadata db only reflects the applied index, and the tail past it can hold
// an add, a remove or a joint configuration.
type LogMembership struct {
	ConfState *raftpb.ConfState
	// LastIndex is the log's last index.
	LastIndex uint64
	// Removed maps every removed id to the index that removed it.
	Removed map[uint64]uint64
}

// Joint reports whether the configuration is joint (an outgoing voter set
// remains).
func (m *LogMembership) Joint() bool { return len(m.ConfState.GetVotersOutgoing()) > 0 }

// IsVoter reports whether id votes in the incoming configuration.
func (m *LogMembership) IsVoter(id uint64) bool {
	for _, v := range m.ConfState.GetVoters() {
		if v == id {
			return true
		}
	}
	return false
}

// IsMember reports whether id is a voter or learner in either half of the
// configuration.
func (m *LogMembership) IsMember(id uint64) bool {
	cs := m.ConfState
	for _, set := range [][]uint64{cs.GetVoters(), cs.GetVotersOutgoing(), cs.GetLearners(), cs.GetLearnersNext()} {
		for _, v := range set {
			if v == id {
				return true
			}
		}
	}
	return false
}

// ReadLogMembership replays s's membership changes past its applied index.
func ReadLogMembership(s LogSource) (*LogMembership, error) {
	_, cs, err := s.InitialState()
	if err != nil {
		return nil, fmt.Errorf("read the configuration: %w", err)
	}
	last, err := s.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("read the log's last index: %w", err)
	}
	trk := tracker.MakeProgressTracker(1, 0)
	chg := confchange.Changer{Tracker: trk, LastIndex: last}
	cfg, prs, err := confchange.Restore(chg, cs)
	if err != nil {
		return nil, fmt.Errorf("restore the configuration %v: %w", cs, err)
	}
	trk.Config, trk.Progress = cfg, prs

	removed := maps.Clone(s.RemovedMembers())
	if removed == nil {
		removed = map[uint64]uint64{}
	}
	since, on := s.MembershipGuardIndex()

	for lo := s.AppliedIndex() + 1; lo <= last; {
		ents, err := s.Entries(lo, last+1, 64<<20)
		if err != nil {
			return nil, fmt.Errorf("read the log from %d: %w", lo, err)
		}
		if len(ents) == 0 {
			return nil, fmt.Errorf("read the log from %d: no entries before %d", lo, last)
		}
		for _, e := range ents {
			cc, ok, err := decodeConfChange(e)
			if err != nil {
				return nil, err
			}
			if ok {
				index := e.GetIndex()
				if on && index > since && len(cc.Changes) > 0 {
					refuseInvalidChanges(cc, map[uint64]struct{}(trk.Config.Voters[0]), removed, since)
				}
				chg := confchange.Changer{Tracker: trk, LastIndex: last}
				var cfg tracker.Config
				var prs tracker.ProgressMap
				if cc.LeaveJoint() {
					cfg, prs, err = chg.LeaveJoint()
				} else if autoLeave, joint := cc.EnterJoint(); joint {
					cfg, prs, err = chg.EnterJoint(autoLeave, cc.Changes...)
				} else {
					cfg, prs, err = chg.Simple(cc.Changes...)
				}
				if err != nil {
					return nil, fmt.Errorf("the membership change at index %d does not apply: %w", index, err)
				}
				trk.Config, trk.Progress = cfg, prs
				for _, ch := range cc.Changes {
					switch {
					case ch.GetNodeId() == 0:
					case ch.GetType() == raftpb.ConfChangeRemoveNode:
						if _, ok := removed[ch.GetNodeId()]; !ok {
							removed[ch.GetNodeId()] = index
						}
					case ch.GetType() == raftpb.ConfChangeUpdateNode && string(cc.Context) == membershipGuardMarker && !on:
						since, on = index, true
					}
				}
			}
			lo = e.GetIndex() + 1
		}
	}
	return &LogMembership{ConfState: trk.ConfState(), LastIndex: last, Removed: removed}, nil
}

// decodeConfChange returns e's membership change as v2, when it is one.
func decodeConfChange(e *raftpb.Entry) (*raftpb.ConfChangeV2, bool, error) {
	switch e.GetType() {
	case raftpb.EntryConfChangeV2:
		var cc raftpb.ConfChangeV2
		if err := proto.Unmarshal(e.Data, &cc); err != nil {
			return nil, false, fmt.Errorf("decode the membership change at index %d: %w", e.GetIndex(), err)
		}
		return &cc, true, nil
	case raftpb.EntryConfChange:
		var cc raftpb.ConfChange
		if err := proto.Unmarshal(e.Data, &cc); err != nil {
			return nil, false, fmt.Errorf("decode the membership change at index %d: %w", e.GetIndex(), err)
		}
		return cc.AsV2(), true, nil
	}
	return nil, false, nil
}
