package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestStopTransport_IdempotentNoDoubleClosePanic pins the shutdown-time crash:
// stopTransport is reachable from BOTH the transport-error path (writeError) and
// Close, and it calls close(transportStopC). Without a guard the second call
// panics on a double close — crashing the node on shutdown when a transport
// error and Close race (the most routine op a customer runs). It must be
// idempotent.
func TestStopTransport_IdempotentNoDoubleClosePanic(t *testing.T) {
	done := make(chan struct{})
	close(done) // serveRaft already exited → transportDoneC closed, so <-transportDoneC returns
	n := &Raft{
		transportStopC: make(chan struct{}),
		transportDoneC: done,
		logger:         zap.NewNop(),
	}

	n.stopTransport()                                  // first call (e.g. a transport error)
	require.NotPanics(t, func() { n.stopTransport() }, // second call (Close)
		"stopTransport called twice (transport error + Close) must not panic on a double close(transportStopC)")
}
