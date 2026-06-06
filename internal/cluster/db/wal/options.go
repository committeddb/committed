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

// WithMetrics wires an OTel Metrics instance into Storage so the
// committed.wal.corrupt_entries counter is emitted when a per-entry
// checksum verification fails on read. A nil *Metrics (the default) is
// safe — corruption is still detected and returned as ErrCorruptEntry, just
// not counted.
func WithMetrics(m *metrics.Metrics) Option {
	return func(o *options) { o.metrics = m }
}
