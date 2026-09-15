package db

import (
	"context"
	"errors"
	"fmt"
	"io"

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
	Start(stopC <-chan struct{}) error
	AddPeer(peer raft.Peer) error
	RemovePeer(id uint64)
	Send(msgs []*raftpb.Message)
	Stop()
	// The catch-up client: a node whose event log is behind the snapshot
	// raft wants to install fetches the missing events from a peer over
	// this same transport (see catchup.go).
	EventFetcher
}

// EventSink receives one peer's event-log content as it streams in, in log
// order: Begin with the serving log's identity, then whole sealed segment
// files and runs of records, then End with what the stream covered. A
// stream that stops without End was cut short. The serving side (an
// EventServer) drives one; the transport carries each call across the
// wire; the receiving side (catch-up) implements one that stages and
// adopts — segments in batches, at End or before the records that follow
// them, because taking files into the log reopens it, and a reopen costs
// a directory listing of every segment the log already has.
type EventSink interface {
	// Begin opens the stream: gen is the serving log's generation (see
	// EventServer.EventLogGeneration) and eventIndex the serving node's
	// event index. A receiver that cannot take content at that generation
	// returns *EventGenerationMismatch, and nothing else is sent.
	Begin(gen, eventIndex uint64) error
	// Segment delivers one sealed segment file whole, under its own name
	// (its first sequence, .zst for a compressed one). r yields exactly
	// size bytes.
	Segment(name string, size int64, r io.Reader) error
	// Records delivers a run of records in the event log's on-disk encoding
	// (each a uvarint length prefix and a framed record), in order.
	Records(data []byte) error
	// End closes a complete stream with what it covered.
	End(res EventServeResult) error
}

// EventServeResult reports what one ServeEvents call streamed.
type EventServeResult struct {
	Generation uint64 // the serving log's generation
	EventIndex uint64 // the serving node's event index when the stream began
	LastIndex  uint64 // raft index of the last event served (0 when none)
	More       bool   // events in the requested range remain on the serving node
}

// EventServer is the storage surface the peer transport serves catch-up
// fetches from — wal.Storage in production; the in-memory test doubles
// have no event log and serve nothing.
type EventServer interface {
	// ServeEvents streams to sink every event with raft index in (after,
	// to] that this node holds, a bounded amount per call, under one
	// layout freeze so the files it lists stay on disk while it reads
	// them. ctx aborts a stream whose sink has stalled.
	ServeEvents(ctx context.Context, after, to uint64, sink EventSink) (EventServeResult, error)
	// EventLogGeneration identifies the CONTENT of this node's event log:
	// the scrub bound its bytes reflect. Two logs at one generation are
	// byte-identical over their shared prefix, so one can extend the
	// other; a fetch never mixes generations.
	EventLogGeneration() uint64
}

// EventFetchRequest asks a peer for the events this node is missing: those
// with raft index in (After, To].
type EventFetchRequest struct {
	After, To uint64
	// Generation pins the serving log's generation; 0 accepts any (an
	// empty log adopts what it is given). MinGeneration is the least the
	// receiver can use — the completed scrub bound of the snapshot it
	// will install, whose bbolt has pruned the tombstones of every scrub
	// through it, so content at an older generation could never be brought
	// forward. A peer at another generation than the pin, or below the
	// minimum, answers *EventGenerationMismatch instead of content.
	Generation, MinGeneration uint64
}

// EventFetchResult is what one FetchEvents call obtained, and from whom.
type EventFetchResult struct {
	Peer uint64
	EventServeResult
}

// EventGenerationMismatch is the fetch outcome when the serving peer's log
// is at another generation than the request pinned: Have is the peer's,
// Want the request's.
type EventGenerationMismatch struct {
	Peer, Have, Want uint64
}

func (e *EventGenerationMismatch) Error() string {
	return fmt.Sprintf("peer %d serves event-log generation %d, this node's log is at %d", e.Peer, e.Have, e.Want)
}

// ErrNoPeerToFetchFrom is the fetch outcome when no registered peer could be
// asked, or none answered with content — the caller retries later.
var ErrNoPeerToFetchFrom = errors.New("no peer served the events")

// EventFetcher is the transport's catch-up client. One call asks one peer
// (the transport picks it) for the requested range and drives sink with
// what comes back; a peer that has nothing new, or cannot be reached, is
// skipped for another. It returns when the peer's stream ends (result.More
// says whether to ask again), on a sink error, or with
// ErrNoPeerToFetchFrom.
type EventFetcher interface {
	FetchEvents(ctx context.Context, req EventFetchRequest, sink EventSink) (EventFetchResult, error)
}

// TransportRaft is the raft-node surface a Transport drives: deliver an incoming
// message into the node, and report peer reachability / snapshot delivery back.
// (etcd's rafthttp calls this "Raft"; renamed here to avoid colliding with
// db.Raft.) startRaft passes the node's implementation to the factory.
type TransportRaft interface {
	Process(ctx context.Context, m *raftpb.Message) error
	IsIDRemoved(id uint64) bool
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
}

// TransportFactory builds the peer Transport for a raft node. It is the
// inversion-of-control seam: the composition root (cmd) supplies one via
// WithTransportFactory so this package depends only on the Transport abstraction
// above, never on a concrete transport. The factory gets everything the node can
// provide — its id, the seed peer set, the logger, the node's TransportRaft
// callbacks, the event log it serves peers' catch-ups from (nil when the
// storage has none), the optional mTLS config, and the cluster API bearer
// token (empty when auth is off) — and returns a ready Transport.
type TransportFactory func(id uint64, peers []raft.Peer, logger *zap.Logger, r TransportRaft, events EventServer, tlsInfo *tlstransport.TLSInfo, token string) Transport
