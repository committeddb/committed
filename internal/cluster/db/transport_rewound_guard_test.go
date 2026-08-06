package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"
)

// stubStepNode records messages stepped into raft; every other raft.Node
// method panics via the embedded nil interface — the guard tests only ever
// reach Step.
type stubStepNode struct {
	raft.Node
	stepped []*raftpb.Message
}

func (s *stubStepNode) Step(_ context.Context, m *raftpb.Message) error {
	s.stepped = append(s.stepped, m)
	return nil
}

type stubLastIndex struct {
	li  uint64
	err error
}

func (s stubLastIndex) LastIndex() (uint64, error) { return s.li, s.err }

func newGuardedTransport(li stubLastIndex) (*httpTransportRaft, *stubStepNode, *observer.ObservedLogs) {
	core, logs := observer.New(zap.InfoLevel)
	node := &stubStepNode{}
	tr := &httpTransportRaft{
		node:      node,
		lastIndex: li,
		// WriteThenPanic turns the guard's Fatal into a recoverable panic so
		// the test can observe it instead of the process exiting.
		logger: zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic)),
	}
	return tr, node, logs
}

func heartbeat(commit uint64) *raftpb.Message {
	return &raftpb.Message{
		Type:   raftpb.MsgHeartbeat.Enum(),
		From:   proto.Uint64(1),
		Commit: proto.Uint64(commit),
	}
}

// The rewound-member state: a heartbeat commits beyond this node's log —
// impossible unless the node un-acknowledged entries (a member restored from
// a backup). The guard must fatal with the operational diagnosis BEFORE the
// message reaches raft, whose own commitTo panic misdiagnoses the state as
// disk corruption.
func TestProcess_RewoundMemberFatalsWithDiagnosis(t *testing.T) {
	tr, node, logs := newGuardedTransport(stubLastIndex{li: 100})

	require.Panics(t, func() {
		_ = tr.Process(context.Background(), heartbeat(150))
	}, "the guard must fatal (WriteThenPanic) on a rewound-state heartbeat")

	require.Empty(t, node.stepped, "the poisoned heartbeat must never reach raft")

	entries := logs.FilterMessageSnippet("raft state rewound").All()
	require.Len(t, entries, 1)
	require.Contains(t, entries[0].Message, "rebuild",
		"the fatal must point at the rebuild runbook")
	require.NotContains(t, entries[0].Message, "corrupt",
		"the diagnosis must not suggest corruption — that is the library panic's misdiagnosis")
	ctx := entries[0].ContextMap()
	require.EqualValues(t, 150, ctx["heartbeatCommit"])
	require.EqualValues(t, 100, ctx["lastIndex"])
}

// Normal operation: a heartbeat commit at or below the log passes straight
// through to raft. This is every healthy and merely-BEHIND node — behind is
// raft's own catch-up job, never the guard's.
func TestProcess_HealthyHeartbeatPassesThrough(t *testing.T) {
	tr, node, _ := newGuardedTransport(stubLastIndex{li: 100})

	require.NoError(t, tr.Process(context.Background(), heartbeat(100)))
	require.NoError(t, tr.Process(context.Background(), heartbeat(40)))
	require.Len(t, node.stepped, 2)
}

// MsgApp legitimately carries a commit covering entries the SAME message
// appends, and the library min-caps its commitTo — the guard must not
// second-guess appends.
func TestProcess_AppendAboveLastIndexPassesThrough(t *testing.T) {
	tr, node, _ := newGuardedTransport(stubLastIndex{li: 100})

	m := &raftpb.Message{
		Type:   raftpb.MsgApp.Enum(),
		From:   proto.Uint64(1),
		Commit: proto.Uint64(150),
	}
	require.NoError(t, tr.Process(context.Background(), m))
	require.Len(t, node.stepped, 1)
}

// A LastIndex read error skips the guard rather than fatal-ing on a read —
// the library panic remains the backstop for the true rewound state.
func TestProcess_LastIndexErrorSkipsGuard(t *testing.T) {
	tr, node, _ := newGuardedTransport(stubLastIndex{li: 0, err: context.DeadlineExceeded})

	require.NoError(t, tr.Process(context.Background(), heartbeat(150)))
	require.Len(t, node.stepped, 1)
}
