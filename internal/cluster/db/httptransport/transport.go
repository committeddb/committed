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
	peers     Peers
	transport *rafthttp.Transport
}

type Peers map[uint64]string

func New(id uint64, peers Peers, l *zap.Logger, r Raft) *HttpTransport {
	t := &rafthttp.Transport{
		Logger:      l,
		ID:          types.ID(id),
		ClusterID:   0x1000,
		Raft:        r,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), fmt.Sprint(id)),
		ErrorC:      make(chan error),
	}

	return &HttpTransport{id: id, peers: peers, transport: t}
}

func (t *HttpTransport) GetErrorC() chan error {
	return t.transport.ErrorC
}

func (t *HttpTransport) Start(stopC <-chan struct{}) error {
	t.transport.Start()
	for k, v := range t.peers {
		if k != t.id {
			t.transport.AddPeer(types.ID(k), []string{v})
		}
	}

	url, err := url.Parse(t.peers[t.id])
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
