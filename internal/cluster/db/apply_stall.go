package db

import (
	"sync"
	"time"
)

// ApplyStallThreshold is how long committed-but-unapplied work may sit with
// ZERO apply progress before /ready reports the node not ready. It only
// governs the wedged case — while apply is advancing (even slowly, e.g. a
// boot replay of a long backlog) the clock resets on every advance and the
// node stays ready. 30s is far beyond any healthy fsync or batch-apply
// latency, so the only way to trip it is a genuine apply wedge (a hung
// bbolt/disk write, an apply-handler bug) — the "down while looking up"
// condition from the field incident, where appliedIndex froze one entry
// behind commitIndex and /ready stayed green while every proposal in the
// cluster timed out.
var ApplyStallThreshold = 30 * time.Second

// applyStallDetector decides, probe-driven, whether apply has stalled:
// committed work is pending AND appliedIndex has made no progress since the
// pending gap was first observed threshold ago. State lives here (not a
// watcher goroutine) because the readiness probe is the only consumer —
// detection latency is threshold + probe interval, which is fine for a
// load-balancer signal.
//
// The gap timestamp records when THIS pending gap was first seen — not the
// last apply time. An idle cluster's first commit after a quiet hour must
// start the clock at observation, or the probe would read the idle time as
// an instant stall.
type applyStallDetector struct {
	mu          sync.Mutex
	threshold   time.Duration
	gapSince    time.Time
	lastApplied uint64
}

// check reports whether apply is stalled given the node's current raft
// commit index and applied index. Safe for concurrent probes.
func (d *applyStallDetector) check(commit, applied uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if commit <= applied {
		// No pending work — clear any observed gap.
		d.gapSince = time.Time{}
		d.lastApplied = applied
		return false
	}
	if applied != d.lastApplied || d.gapSince.IsZero() {
		// Progress since the last probe (or a freshly observed gap):
		// restart the clock. A slow-but-moving replay never trips.
		d.gapSince = time.Now()
		d.lastApplied = applied
		return false
	}
	return time.Since(d.gapSince) > d.threshold
}

// ApplyStalled reports whether this node's raft apply loop has committed
// work pending with no progress for ApplyStallThreshold. The /ready probe
// uses it to take a node whose apply path is wedged out of rotation: such
// a node cannot confirm proposals (Propose waits for local apply) and
// serves stale reads, so "ready" would be a lie — the field incident's
// silent-while-green half.
func (db *DB) ApplyStalled() bool {
	if db.raft == nil {
		return false
	}
	return db.applyStall.check(db.raft.CommitIndex(), db.AppliedIndex())
}
