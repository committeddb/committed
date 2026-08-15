//go:build docker

package harness

import (
	"net"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A stolen port must not poison the run. The shared port pair means a
// single collision would otherwise kill EVERY remaining test in the
// binary — the 2026-08-15 CI incident, where a postgres container's
// docker-proxy host mapping landed exactly on the reserved raft peer port
// and thirty tests bind-failed in sequence. Pre-binding the allocated
// peer port reproduces that class; startCommitted must notice the dead
// child, reallocate a fresh pair, and come healthy.
func TestStartCommitted_RetriesOnStolenPort(t *testing.T) {
	_, peerURL := committedAddrs(t)
	stolen, err := net.Listen("tcp", strings.TrimPrefix(peerURL, "http://"))
	require.NoError(t, err, "steal the allocated peer port")
	defer func() { _ = stolen.Close() }()

	p := startCommitted(t)
	require.NotNil(t, p, "startCommitted must survive a stolen port via reallocation")

	_, newPeer := committedAddrs(t)
	require.NotEqual(t, peerURL, newPeer, "the poisoned pair must have been discarded")
}
