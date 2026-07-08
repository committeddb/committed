package httptransport

import (
	"errors"
	"net"
	"time"
)

// errServerStopped is returned by Accept once the stop channel is closed, so
// the raft peer server's accept loop exits cleanly on shutdown.
var errServerStopped = errors.New("server stopped")

// stoppableListener is a net.Listener whose Accept unblocks when its stop
// channel is closed. It embeds the *net.TCPListener, so Close and Addr come for
// free, and enables TCP keep-alive on every accepted connection so a dead peer's
// socket is eventually reaped rather than lingering.
type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

// newStoppableListener begins listening for TCP connections on addr. It returns
// an error if addr is not a listenable TCP address.
func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcpLn, ok := ln.(*net.TCPListener)
	if !ok {
		_ = ln.Close()
		return nil, errors.New("httptransport: listener is not a *net.TCPListener")
	}
	return &stoppableListener{TCPListener: tcpLn, stopc: stopc}, nil
}

// Accept returns the next connection, or errServerStopped once the stop channel
// is closed. The blocking accept runs on its own goroutine so the stop signal
// can win the race; that goroutine is unblocked (and reaped) when the listener
// is later Closed. Accepted connections have TCP keep-alive enabled.
func (ln *stoppableListener) Accept() (net.Conn, error) {
	type accepted struct {
		conn *net.TCPConn
		err  error
	}
	next := make(chan accepted, 1) // buffered so the goroutine never leaks on send
	go func() {
		c, err := ln.TCPListener.AcceptTCP()
		next <- accepted{conn: c, err: err}
	}()

	select {
	case <-ln.stopc:
		return nil, errServerStopped
	case a := <-next:
		if a.err != nil {
			return nil, a.err
		}
		_ = a.conn.SetKeepAlive(true)
		_ = a.conn.SetKeepAlivePeriod(3 * time.Minute)
		return a.conn, nil
	}
}
