package db

import (
	"context"

	tlstransport "go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Transport is the raft peer transport: it ships this node's outgoing messages
// to peers and feeds incoming ones back into raft (via TransportRaft). db owns
// this abstraction; a concrete implementation (internal/cluster/db/httptransport)
// is injected by the composition root through a TransportFactory, so db itself
// does not depend on any transport implementation.
type Transport interface {
	GetErrorC() chan error
	Start(stopC <-chan struct{}) error
	AddPeer(peer raft.Peer) error
	RemovePeer(id uint64)
	Send(msgs []raftpb.Message)
	Stop()
}

// TransportRaft is the raft-node surface a Transport drives: deliver an incoming
// message into the node, and report peer reachability / snapshot delivery back.
// (etcd's rafthttp calls this "Raft"; renamed here to avoid colliding with
// db.Raft.) startRaft passes the node's implementation to the factory.
type TransportRaft interface {
	Process(ctx context.Context, m raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

// TransportFactory builds the peer Transport for a raft node. It is the
// inversion-of-control seam: the composition root (cmd) supplies one via
// WithTransportFactory so this package depends only on the Transport abstraction
// above, never on a concrete transport. The factory gets everything the node can
// provide — its id, the seed peer set, the logger, the node's TransportRaft
// callbacks, and the optional mTLS config — and returns a ready Transport.
type TransportFactory func(id uint64, peers []raft.Peer, logger *zap.Logger, r TransportRaft, tlsInfo *tlstransport.TLSInfo) Transport
