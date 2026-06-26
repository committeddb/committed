package db_test

import (
	"fmt"
	"strings"
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db"
)

// TestStartRaft_FailsLoudOnInconsistentStorage is the regression guard for the
// "term should be set when sending MsgPreVoteResp" panic
// (raft37-prevote-term0-panic-flake). A Storage holding a log entry at term 1 but
// a HardState still at term 0 violates raft's implicit precondition that
// HardState.Term >= lastLogTerm.
//
// Before the boundary check, committed routed that term-0 view to
// raft.StartNode, raft's setupNode silently swallowed Bootstrap's "can't
// bootstrap a nonempty Storage" error, and the node came up beneath its own log —
// then panicked deep in raft's pre-vote rejection (which sends the rejecter's
// term, down to 0), with a stack far from the cause. Now NewRaft fails loud at
// startup with an attributable message, the same posture as the missing-transport
// check.
func TestStartRaft_FailsLoudOnInconsistentStorage(t *testing.T) {
	s := NewMemoryStorage()
	// Seed a non-empty log at term 1 while leaving the HardState at term 0 —
	// the invariant-violating state behind the panic.
	if err := s.Append([]*raftpb.Entry{{Term: proto.Uint64(1), Index: proto.Uint64(1)}}); err != nil {
		t.Fatalf("seed log: %v", err)
	}

	peers := []raft.Peer{{ID: 1, Context: []byte("")}}
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected NewRaft to fail loud on an inconsistent (term-below-log) storage")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "storage invariant violated") {
			t.Fatalf("panic %q does not name the storage invariant", msg)
		}
	}()
	db.NewRaft(1, peers, s, proposeC, confChangeC)
}

// TestStartRaft_AcceptsConsistentStorage guards against a false positive: a
// storage whose HardState.Term is at or above its last log term is consistent and
// must start normally. A false positive would panic with the invariant message
// here and fail the test.
func TestStartRaft_AcceptsConsistentStorage(t *testing.T) {
	s := NewMemoryStorage()
	if err := s.Save(&raftpb.HardState{Term: proto.Uint64(1)}, []*raftpb.Entry{{Term: proto.Uint64(1), Index: proto.Uint64(1)}}, nil); err != nil {
		t.Fatalf("seed storage: %v", err)
	}

	peers := []raft.Peer{{ID: 1, Context: []byte("")}}
	proposeC := make(chan []byte)
	confChangeC := make(chan *raftpb.ConfChangeV2)

	_, rr := db.NewRaft(1, peers, s, proposeC, confChangeC)
	rr.Close()
}
