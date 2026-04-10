package wal

import "go.uber.org/zap"

// Option configures behaviour of Open. Options are intended primarily for
// tests; production callers should pass none.
type Option func(*options)

type options struct {
	fsyncDisabled      bool
	inMemoryTimeSeries bool
	logger             *zap.Logger
}

// WithoutFsync disables fsync on the underlying key-value store, trading
// crash durability for speed. Intended for tests; do not use in production.
func WithoutFsync() Option {
	return func(o *options) { o.fsyncDisabled = true }
}

// WithInMemoryTimeSeries keeps the time-series store entirely in memory
// instead of persisting to disk. Intended for tests; data does not survive
// process exit.
func WithInMemoryTimeSeries() Option {
	return func(o *options) { o.inMemoryTimeSeries = true }
}

// WithLogger overrides the logger used by Storage. Defaults to zap.NewNop()
// so tests run silently.
func WithLogger(l *zap.Logger) Option {
	return func(o *options) { o.logger = l }
}
