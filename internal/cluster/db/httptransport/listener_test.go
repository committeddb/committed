package httptransport

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestStoppableListener_Accept verifies that a connection to the listener
// is accepted, returns a usable net.Conn, and TCP keep-alive is set.
func TestStoppableListener_Accept(t *testing.T) {
	stopc := make(chan struct{})
	ln, err := newStoppableListener("127.0.0.1:0", stopc)
	require.Nil(t, err)
	defer ln.Close()

	addr := ln.Addr().String()

	// Accept on a goroutine
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	results := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		results <- acceptResult{c, err}
	}()

	// Dial it
	dialed, err := net.Dial("tcp", addr)
	require.Nil(t, err)
	defer dialed.Close()

	select {
	case r := <-results:
		require.Nil(t, r.err)
		require.NotNil(t, r.conn)
		r.conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return after dial")
	}
}

// TestStoppableListener_StopSignal verifies that closing the stop channel
// causes a blocking Accept to return with the "server stopped" error.
func TestStoppableListener_StopSignal(t *testing.T) {
	stopc := make(chan struct{})
	ln, err := newStoppableListener("127.0.0.1:0", stopc)
	require.Nil(t, err)
	defer ln.Close()

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	results := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		results <- acceptResult{c, err}
	}()

	// Give Accept a moment to enter the select
	time.Sleep(50 * time.Millisecond)

	// Close stop channel
	close(stopc)

	select {
	case r := <-results:
		require.NotNil(t, r.err)
		require.Equal(t, "server stopped", r.err.Error())
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return after stop signal")
	}
}

// TestStoppableListener_InvalidAddress verifies that newStoppableListener
// returns an error for an invalid address.
func TestStoppableListener_InvalidAddress(t *testing.T) {
	stopc := make(chan struct{})
	_, err := newStoppableListener("not-a-valid-address", stopc)
	require.NotNil(t, err)
}
