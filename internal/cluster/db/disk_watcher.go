package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// diskState is the pressure level the watcher classifies the data directory's
// filesystem into, from its free-space percent. The levels are ordered (ok <
// warn < critical < full) so the propose gate and the compaction-pressure hint
// can use simple comparisons (e.g. "s >= diskCritical").
type diskState int32

const (
	diskOK diskState = iota
	diskWarn
	diskCritical
	diskFull
)

func (s diskState) String() string {
	switch s {
	case diskWarn:
		return "warn"
	case diskCritical:
		return "critical"
	case diskFull:
		return "full"
	default:
		return "ok"
	}
}

// Disk-watcher production defaults. The percent thresholds form descending
// bands (warn > critical > full); cmd/node.go wires the COMMITTED_DISK_*_PERCENT
// env vars to override them, and 0 in any DiskWatcherConfig field resolves back
// to the matching default.
const (
	DefaultDiskWatchInterval   = 30 * time.Second
	DefaultDiskWarnPercent     = 20.0
	DefaultDiskCriticalPercent = 10.0
	DefaultDiskFullPercent     = 3.0
)

// DiskWatcherConfig configures the background disk-usage watcher. An empty Path
// (the default) disables the watcher entirely; the node behaves exactly as it
// did before disk limits existed. Wired in via WithDiskWatcher.
type DiskWatcherConfig struct {
	// Path is the directory to statfs — the node's data directory. Empty
	// disables the watcher.
	Path string
	// Interval is the poll cadence. 0 resolves to DefaultDiskWatchInterval.
	Interval time.Duration
	// WarnPercent / CriticalPercent / FullPercent are the free-space percent
	// thresholds for the warn / critical / full states. 0 in any field
	// resolves to the matching Default*Percent.
	WarnPercent     float64
	CriticalPercent float64
	FullPercent     float64
}

// errDiskWatchUnsupported is returned by diskUsage on platforms with no statfs
// syscall (Windows). The watcher logs it once and disables itself rather than
// failing a probe — and spamming the log — every interval.
var errDiskWatchUnsupported = errors.New("disk watcher: free-space probe not supported on this platform")

// diskWatcher periodically samples the free space on the filesystem backing
// its path, publishes gauges, classifies the result into a diskState, and — on
// every change — logs the transition and pushes the new state to onState
// (which updates the propose gate and the compaction-pressure hint). One
// watcher runs per node, started from db.New when a data dir is configured.
type diskWatcher struct {
	path        string
	interval    time.Duration
	warnPct     float64
	criticalPct float64
	fullPct     float64

	// usage probes free/total bytes for path. Defaults to diskUsage (the
	// platform statfs); tests inject a fake to drive state transitions
	// without a real low-disk filesystem.
	usage   func(path string) (free, total uint64, err error)
	onState func(diskState)
	logger  *zap.Logger
	metrics *metrics.Metrics

	// last is the most recently published state and started records whether
	// any sample has been taken. Both are touched only by the single watcher
	// goroutine (run), so they need no synchronization.
	last    diskState
	started bool
}

// newDiskWatcher builds a watcher from cfg, resolving zero-valued fields to
// their production defaults and wiring the real platform probe.
func newDiskWatcher(cfg DiskWatcherConfig, onState func(diskState), logger *zap.Logger, m *metrics.Metrics) *diskWatcher {
	interval := cfg.Interval
	if interval <= 0 {
		interval = DefaultDiskWatchInterval
	}
	warn := cfg.WarnPercent
	if warn <= 0 {
		warn = DefaultDiskWarnPercent
	}
	critical := cfg.CriticalPercent
	if critical <= 0 {
		critical = DefaultDiskCriticalPercent
	}
	full := cfg.FullPercent
	if full <= 0 {
		full = DefaultDiskFullPercent
	}
	// The bands must be strictly descending (warn > critical > full) or a band
	// is unreachable — e.g. warn <= critical makes classify() reach the critical
	// band first, so the warn state never fires. Per-value validation (the
	// <=0/>=100 fallback above, and parsePercentEnv upstream) can't catch a bad
	// *combination* — a set like warn=5, critical=10 is each individually valid —
	// so guard the ordering here. On a non-descending set, warn and fall back to
	// the (descending-by-construction) defaults, mirroring the per-value fallback
	// policy rather than silently accepting an unreachable band.
	if !(warn > critical && critical > full) {
		logger.Warn("disk watcher thresholds must be descending (warn > critical > full); using defaults",
			zap.Float64("warn", warn),
			zap.Float64("critical", critical),
			zap.Float64("full", full),
		)
		warn = DefaultDiskWarnPercent
		critical = DefaultDiskCriticalPercent
		full = DefaultDiskFullPercent
	}
	return &diskWatcher{
		path:        cfg.Path,
		interval:    interval,
		warnPct:     warn,
		criticalPct: critical,
		fullPct:     full,
		usage:       diskUsage,
		onState:     onState,
		logger:      logger,
		metrics:     m,
	}
}

// classify maps a free-space percent to a diskState against the configured
// thresholds. The bands are inclusive at the low end (freePct <= threshold) so
// that hitting a threshold exactly counts as entering that level.
func (w *diskWatcher) classify(freePct float64) diskState {
	switch {
	case freePct <= w.fullPct:
		return diskFull
	case freePct <= w.criticalPct:
		return diskCritical
	case freePct <= w.warnPct:
		return diskWarn
	default:
		return diskOK
	}
}

// sample takes one reading: probe, publish gauges, classify, and on the first
// sample or any state change, log the transition and propagate the new state.
// A probe error is returned to the caller (run) to decide; gauges and state
// are left untouched so a transient stat failure never spuriously gates
// writes.
func (w *diskWatcher) sample() error {
	free, total, err := w.usage(w.path)
	if err != nil {
		return err
	}

	var freePct float64
	if total > 0 {
		freePct = 100 * float64(free) / float64(total)
	}
	if w.metrics != nil {
		w.metrics.SetDiskFree(free, freePct)
	}

	state := w.classify(freePct)
	if w.metrics != nil {
		w.metrics.SetDiskState(state.String())
	}

	if !w.started || state != w.last {
		w.logTransition(w.last, state, free, freePct)
		w.onState(state)
		w.last = state
		w.started = true
	}
	return nil
}

// logTransition logs a state change at a severity that scales with the
// pressure: the initial sample and recoveries log at Info, a step up to warn
// at Warn, and a step up into critical/full at Error ("log loudly").
func (w *diskWatcher) logTransition(from, to diskState, free uint64, freePct float64) {
	fields := []zap.Field{
		zap.String("path", w.path),
		zap.Uint64("free_bytes", free),
		zap.Float64("free_percent", freePct),
		zap.String("state", to.String()),
	}
	switch {
	case !w.started:
		w.logger.Info("disk watcher started", fields...)
	case to > from:
		fields = append(fields, zap.String("from", from.String()))
		if to >= diskCritical {
			w.logger.Error("disk pressure increased; writes restricted", fields...)
		} else {
			w.logger.Warn("disk pressure increased", fields...)
		}
	default:
		fields = append(fields, zap.String("from", from.String()))
		w.logger.Info("disk pressure recovered; writes re-enabled", fields...)
	}
}

// run samples once immediately (so the gate and gauges reflect reality before
// the first interval elapses), then on every tick until ctx is canceled
// (db.Close). An unsupported-platform probe error disables the watcher; any
// other probe error is logged and retried on the next tick, leaving the
// last-known state in place.
func (w *diskWatcher) run(ctx context.Context) {
	if !w.handleSample(w.sample()) {
		return
	}

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.handleSample(w.sample()) {
				return
			}
		}
	}
}

// handleSample reports whether the watcher should keep running after a sample.
// It returns false (stop) only on the unrecoverable unsupported-platform
// error; transient probe errors are logged and tolerated.
func (w *diskWatcher) handleSample(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, errDiskWatchUnsupported) {
		w.logger.Warn("disk watcher disabled: unsupported platform", zap.Error(err))
		return false
	}
	w.logger.Warn("disk watcher: free-space probe failed; retaining last state",
		zap.String("path", w.path), zap.Error(err))
	return true
}

// diskRejection returns the typed error a proposal of the given kind should be
// rejected with under state, or nil if it may proceed. kind is the value
// returned by proposalKind ("user", "config", "index", "position").
//
// The policy stops the writes that grow the log while keeping the bookkeeping
// that keeps the cluster correct and the main feature (sync) alive:
//
//   - critical: reject only user-data proposals (API writes + ingest data).
//     Config changes and checkpoints (sync index / ingest position bumps) still
//     flow, and compaction is nudged, so the node can keep draining.
//   - full: also freeze config — operator discretionary changes wait for
//     recovery — but STILL allow checkpoints. Index/position bumps are tiny and
//     blocking them would (a) make the sync worker re-deliver the same Actual in
//     a loop for the whole full period and (b) regress sync's "≤1 duplicate on
//     crash" guarantee into a duplicate storm. The 3% full headroom is reserved
//     precisely for these essential small writes plus compaction's snapshot.
//
// The same policy applies at two scopes: the propose gate normally evaluates
// it against the CLUSTER-effective state (the leader-computed admission
// verdict — see disk_cluster.go) and falls back to this node's own state when
// no fresh verdict is held. Either way, this one kind-aware policy decides.
func diskRejection(state diskState, kind string) error {
	switch state {
	case diskFull:
		if kind == "user" || kind == "config" {
			return fmt.Errorf("%w: data disk is full; only checkpoints and compaction continue", cluster.ErrInsufficientStorage)
		}
	case diskCritical:
		if kind == "user" {
			return fmt.Errorf("%w: data disk is critically low; user-data writes are rejected", cluster.ErrInsufficientStorage)
		}
	}
	return nil
}
