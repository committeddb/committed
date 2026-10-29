package wal

import (
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// Option configures behaviour of Open. Some are test-only (WithoutFsync,
// WithLostCallback); others are production wiring (WithLogger, WithMetrics,
// WithEventCacheSegments) passed by cmd/node.
type Option func(*options)

type options struct {
	eventLogOpener     eventLogOpener
	fsyncDisabled      bool
	logger             *zap.Logger
	metrics            *metrics.Metrics
	lostCallback       func([]uint64)
	safeMode           bool
	sealerIdleInterval time.Duration
	// offline opens a stopped node's directory for maintenance (restore,
	// recover): no parser, so persisted configs are neither validated nor
	// built — no database connection from a command-line tool.
	offline bool
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

// WithSealerIdleInterval overrides how often the background sealer re-checks
// for newly sealed segments once everything is compressed (default:
// sealerIdleInterval, 30s). A test knob, like WithEventSegmentSize: the
// compression tests seed a log and wait for .zst segments to appear, and at
// the production cadence each one sleeps a full idle tick doing nothing —
// three tests, ninety seconds of pure sleep. Production keeps the default;
// the cadence is a bytes-at-rest concern with no correctness stake.
func WithSealerIdleInterval(d time.Duration) Option {
	return func(o *options) { o.sealerIdleInterval = d }
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

// WithSegmentedEventLog configures the segmented permanent event log, the
// default without it being zstd-compressed with no segment cache. Raft logs
// remain tidwall-backed. An empty events directory is initialized; an existing
// directory must already contain this format, and a tidwall one is refused
// naming `wal convert`. SegmentBytes applies at creation; Encoding and Cache
// also apply on reopen.
func WithSegmentedEventLog(opts segmentlog.LogOptions) Option {
	return func(o *options) { o.eventLogOpener = segmentedEventLogOpener(opts) }
}
