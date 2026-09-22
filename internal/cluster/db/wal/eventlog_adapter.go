package wal

import (
	"sync"
	"sync/atomic"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// eventLogAdapter is Committed's shared application layer over eventlog.EventLog.
// It depends on the storage contract, never either concrete backend. Encoding
// and stable identity checks live in eventlog_entries.go; append/replay and
// rewrite coordination live in eventlog_append.go and eventlog_rewrite.go.
// Actual decoding, applied visibility, selection, and protected lifetimes are
// application policy and remain here beside the production WAL policy helpers.
//
// This adapter is experimental and is not used by production Storage.
// The caller owns the supplied log and its Close.
// Payloads are unframed raftpb.Entry bytes; each backend supplies integrity framing.
// IDs are the entries' Raft indexes, never tidwall sequence numbers.
// The adapter must not be copied after use. Mutations must go through it while
// readers are live; its lock protects each complete Read from rewrite publication.
type eventLogAdapter struct {
	// mutationMu serializes appends, rewrites, and protected-reader registration.
	// mu excludes complete reads only when a rewrite publishes (or earlier when
	// required by a backend). Lock order is mutationMu then mu.
	mutationMu     sync.Mutex
	mu             sync.RWMutex
	log            eventlog.EventLog
	protectedReads atomic.Int64
	readEpoch      uint64 // guarded by mu; invalidates retained decoded entries
}
