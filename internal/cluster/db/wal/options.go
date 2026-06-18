package wal

import (
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/metrics"
)

// Option configures behaviour of Open. Options are intended primarily for
// tests; production callers should pass none.
type Option func(*options)

type options struct {
	fsyncDisabled bool
	logger        *zap.Logger
	metrics       *metrics.Metrics
	lostCallback  func([]uint64)
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
