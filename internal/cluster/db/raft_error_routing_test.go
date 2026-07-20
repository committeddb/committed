package db

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

// newErrorRoutingRaft builds the minimal Raft needed to exercise the ErrorC
// routing seams (forwardProposeErr / sendRaftError) directly: the production
// channel shapes — unbuffered error channel, closeC — with no raft node.
func newErrorRoutingRaft() (*Raft, chan error) {
	errC := make(chan error) // unbuffered, like production
	return &Raft{raftErrorC: errC, closeC: make(chan struct{}), logger: zap.NewNop()}, errC
}

// TestForwardProposeErr_DroppedIsAbsorbed pins the P1 fix: ErrProposalDropped
// is a per-proposal outcome (raft drops proposals during a leadership transfer
// — a routine event under the disk-pressure transfer — or after removal, or on
// a full uncommitted buffer), NOT a node fault. Pre-fix it was forwarded to
// ErrorC, whose one-shot consumer killed the whole node.
func TestForwardProposeErr_DroppedIsAbsorbed(t *testing.T) {
	n, errC := newErrorRoutingRaft()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.forwardProposeErr(raft.ErrProposalDropped, false)
	}()

	select {
	case <-done:
		// returned without forwarding — correct
	case err := <-errC:
		t.Fatalf("a dropped proposal must not reach the fatal ErrorC (got %v) — this is what self-killed the node on a routine leadership transfer", err)
	case <-time.After(2 * time.Second):
		t.Fatal("forwardProposeErr blocked — a dropped proposal must be absorbed, not sent")
	}
	select {
	case err := <-errC:
		t.Fatalf("unexpected error on ErrorC after return: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestForwardProposeErr_RealFaultForwarded: anything that is not a shutdown
// cancellation or a per-proposal outcome is a genuine node fault and must
// still reach ErrorC.
func TestForwardProposeErr_RealFaultForwarded(t *testing.T) {
	n, errC := newErrorRoutingRaft()

	fault := errors.New("storage exploded")
	go n.forwardProposeErr(fault, false)

	select {
	case err := <-errC:
		require.ErrorIs(t, err, fault)
	case <-time.After(2 * time.Second):
		t.Fatal("a real node fault must be forwarded to ErrorC")
	}
}

// TestForwardProposeErr_ShutdownSuppressed: an error surfaced while shutdown
// is in progress is not a node fault (it is the cancellation unwinding).
func TestForwardProposeErr_ShutdownSuppressed(t *testing.T) {
	n, errC := newErrorRoutingRaft()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.forwardProposeErr(errors.New("context canceled"), true)
	}()

	select {
	case <-done:
	case err := <-errC:
		t.Fatalf("shutdown-window errors must be suppressed (got %v)", err)
	case <-time.After(2 * time.Second):
		t.Fatal("forwardProposeErr blocked during shutdown suppression")
	}
}

// TestSendRaftError_ShutdownDoesNotBlock pins the P2 fix: ErrorC has a
// one-shot consumer (zero reads on the SIGTERM path), so a raw send with no
// reader parks the sending goroutine — the Ready loop — forever, deadlocking
// Close into SIGKILL. The closeC guard must let the send abort at shutdown.
func TestSendRaftError_ShutdownDoesNotBlock(t *testing.T) {
	n, _ := newErrorRoutingRaft()
	close(n.closeC) // shutdown in progress; nobody will ever read ErrorC

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.sendRaftError(errors.New("conf change unmarshal"))
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendRaftError blocked past shutdown — this is the un-exitable-process deadlock (SIGKILL required)")
	}
}

// TestSendRaftError_DeliversWhenRead: with a live consumer the error still
// arrives — the guard must not turn real faults into silent drops.
func TestSendRaftError_DeliversWhenRead(t *testing.T) {
	n, errC := newErrorRoutingRaft()

	fault := errors.New("transport fatal")
	go n.sendRaftError(fault)

	select {
	case err := <-errC:
		require.ErrorIs(t, err, fault)
	case <-time.After(2 * time.Second):
		t.Fatal("sendRaftError must deliver to a live ErrorC consumer")
	}
}
