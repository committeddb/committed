package wal

import (
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/metrics"
)

// Option configures behaviour of Open. Some are test-only (WithoutFsync,
// WithLostCallback); others are production wiring (WithLogger, WithMetrics,
// WithEventCacheSegments) passed by cmd/node.
type Option func(*options)

type options struct {
	fsyncDisabled      bool
	logger             *zap.Logger
	metrics            *metrics.Metrics
	lostCallback       func([]uint64)
	eventCacheSegments int
	safeMode           bool
}

// WithoutFsync disables fsync on the underlying key-value store, trading
// crash durability for speed. Intended for tests; do not use in production.
func WithoutFsync() Option {
	return func(o *options) { o.fsyncDisabled = true }
}

// WithLogger overrides the logger used by Storage. Defaults to zap.NewNop()
// so tests run silently.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithLostCallback registers a callback fired from appendEntries when a
// higher-term leader's AppendEntries truncates uncommitted tail entries
// this node physically held. It receives the set of non-zero
// cluster.Proposal RequestIDs carried by those truncated entries — the
// proposals definitively removed from this node's log before they
// committed. Production wires db.notifyLost via SetLostNotifier (the
// callback can't exist at Open time, before the DB does); this option is
// for tests that construct a Storage directly with a known callback. A
// nil callback (the default) disables truncation detection entirely, so
// the happy path pays nothing.
func WithLostCallback(fn func([]uint64)) Option {
	return func(o *options) { o.lostCallback = fn }
}

// WithMetrics wires an OTel Metrics instance into Storage so the
// committed.wal.corrupt_entries counter is emitted when a per-entry
// checksum verification fails on read. A nil *Metrics (the default) is
// safe — corruption is still detected and returned as ErrCorruptEntry, just
// not counted.
func WithMetrics(m *metrics.Metrics) Option {
	return func(o *options) { o.metrics = m }
}

// WithEventCacheSegments sets how many EVENT-LOG segments may be held parsed
// in memory at once (the tidwall segment cache). Each resident segment costs
// ~21MB (a ~20MB entry buffer plus its position table), and capacity is not
// preallocation — an unused slot costs nothing — so the steady-state working
// set is min(this, the number of concurrent readers spread across distinct
// segments). Syncables replaying history are concurrent readers, one segment
// each; a cache smaller than the reader count thrashes (every read by one
// evicts another's segment, forcing a ~20MB re-parse). Wired from
// COMMITTED_EVENT_CACHE_SEGMENTS by cmd/node; <= 0 (the default) resolves to
// DefaultEventCacheSegments. Sizing rule of thumb: at least concurrent
// syncables + 2. The raft entry log deliberately keeps the library default —
// its reader is the single sequential Ready loop, which cannot thrash.
func WithEventCacheSegments(n int) Option {
	return func(o *options) { o.eventCacheSegments = n }
}

// WithSafeMode holds the background scrub worker: Open does not resume a
// pending scrub and later Scrub signals are left queued (the durable bound
// stays recorded in bbolt and resumes on the next normal open). Part of the
// operator escape hatch wired from COMMITTED_SAFE_MODE by cmd/node — a
// diagnosis window must not have the event log being rewritten and swapped
// underneath it, and a scrub that itself crashes the node must not re-fire
// on every boot. The DB layer holds sync/ingest workers under the same flag
// (db.WithSafeMode); raft, apply, and the API run normally.
func WithSafeMode() Option {
	return func(o *options) { o.safeMode = true }
}
