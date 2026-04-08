package httptransport

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
)

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

type HttpTransport struct {
	id        uint64
	peers     []raft.Peer
	transport *rafthttp.Transport
}

// New constructs an HttpTransport and fully initialises the underlying
// rafthttp.Transport (calling its Start, which is non-blocking and just sets
// up round-trippers, probers and the peer/remote maps). Initialising
// synchronously here — rather than deferring it to (HttpTransport).Start —
// guarantees that callers can safely call Stop() at any time after New
// returns, even if the listener goroutine hasn't started serving yet.
func New(id uint64, ps []raft.Peer, l *zap.Logger, r Raft) *HttpTransport {
	t := &rafthttp.Transport{
		Logger:      l,
		ID:          types.ID(id),
		ClusterID:   0x1000,
		Raft:        r,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), fmt.Sprint(id)),
		ErrorC:      make(chan error),
	}

	if err := t.Start(); err != nil {
		// rafthttp.Transport.Start only fails if it can't construct round
		// trippers, which only happens with a malformed TLS config (we don't
		// pass one). Treat this as fatal — there is no recovery path.
		log.Fatalf("rafthttp transport start: %v", err)
	}

	return &HttpTransport{id: id, peers: ps, transport: t}
}

func (t *HttpTransport) GetErrorC() chan error {
	return t.transport.ErrorC
}

func (t *HttpTransport) Start(stopC <-chan struct{}) error {
	rawURL := ""
	for _, p := range t.peers {
		if p.ID != t.id {
			t.transport.AddPeer(types.ID(p.ID), []string{string(p.Context)})
		} else {
			rawURL = string(p.Context)
		}
	}

	// An empty local-peer URL means "do not bind a TCP listener". This is
	// the right shape for single-node tests: there are no peers to accept
	// connections from, so taking a port (and the cross-package collision
	// risk that comes with it) buys us nothing. The transport can still
	// send messages to AddPeer'd peers via t.transport.Send, but it cannot
	// accept incoming ones — which is fine when there are no peers.
	//
	// We still block until stopC is closed so callers (Raft.serveRaft) can
	// distinguish "asked to stop" from "the listener exploded" using their
	// existing shutdown logic.
	if rawURL == "" {
		<-stopC
		return nil
	}

	url, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, stopC)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	return (&http.Server{Handler: t.transport.Handler()}).Serve(ln)
}

func (t *HttpTransport) AddPeer(peer raft.Peer) error {
	_, err := url.Parse(string(peer.Context))
	if err != nil {
		return err
	}
	t.transport.AddPeer(types.ID(peer.ID), []string{string(peer.Context)})

	return nil
}
func (t *HttpTransport) RemovePeer(id uint64) {
	t.transport.RemovePeer(types.ID(id))
}
func (t *HttpTransport) Send(msgs []raftpb.Message) {
	t.transport.Send(msgs)
}
func (t *HttpTransport) Stop() {
	t.transport.Stop()
}
