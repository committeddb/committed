package wal

// Option configures behaviour of Open. Options are intended primarily for
// tests; production callers should pass none.
type Option func(*options)

type options struct {
	fsyncDisabled      bool
	inMemoryTimeSeries bool
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
