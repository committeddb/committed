package db

import (
	"time"

	"go.uber.org/zap"
)

// Sync circuit-breaker tuning. This many permanent errors in a row — each within
// syncBreakerHealthyWindow of the previous — on one syncable is a systematic
// fault: a config typo that every event of a variant violates, not a few
// individually-bad records. Past it the worker parks for operator intervention
// (fix the config, then replace the syncable) instead of dead-lettering the
// whole topic one blocking raft round-trip at a time. A gap longer than the
// window means the syncable ran healthy long enough that the next error starts a
// fresh run — so sporadic bad records never trip it, and (unlike a
// success-reset) interleaved foreign-topic Actuals don't defeat it either. The
// window mirrors the ingest supervisor's healthy-window design.
const (
	syncBreakerMaxConsecutivePermanent = 100
	syncBreakerHealthyWindow           = 60 * time.Second
)

// syncBreakerState tracks the consecutive permanent-error run for one syncable.
type syncBreakerState struct {
	lastPermanentAt time.Time
	consecutive     int
	// lastIndex is the raft index of the last counted permanent. Retries of
	// the SAME entry (the worker holds position and re-runs decide+record
	// when the dead-letter record itself could not persist — e.g. its
	// propose 507'd at disk-full) must not inflate the run: the breaker's
	// premise is "N DISTINCT entries failing consecutively means a
	// systematic fault", and one poison entry retried under disk pressure
	// once tripped it in ~a minute.
	lastIndex uint64
}

// recordSyncPermanent counts one permanent sync error for id and reports the run
// length and whether it has crossed the breaker threshold. A gap longer than
// syncBreakerHealthyWindow since the previous permanent starts a fresh run.
func (db *DB) recordSyncPermanent(id string, index uint64) (consecutive int, tripped bool) {
	db.syncBreakerMu.Lock()
	defer db.syncBreakerMu.Unlock()
	if db.syncBreakerStates == nil {
		db.syncBreakerStates = make(map[string]*syncBreakerState)
	}
	now := time.Now()
	st := db.syncBreakerStates[id]
	if st == nil || now.Sub(st.lastPermanentAt) > syncBreakerHealthyWindow {
		st = &syncBreakerState{}
		db.syncBreakerStates[id] = st
	}
	// Count DISTINCT entries only: a retry of the same index (record-persist
	// failure, not a new sink verdict) refreshes the window but not the run.
	if st.lastIndex != index || st.consecutive == 0 {
		st.consecutive++
		st.lastIndex = index
	}
	st.lastPermanentAt = now
	return st.consecutive, st.consecutive >= syncBreakerMaxConsecutivePermanent
}

// resetSyncBreaker clears id's run — called when a worker (re)starts so a
// replacement (e.g. after the operator fixes the config) begins fresh.
func (db *DB) resetSyncBreaker(id string) {
	db.syncBreakerMu.Lock()
	defer db.syncBreakerMu.Unlock()
	delete(db.syncBreakerStates, id)
}

// syncBreakerTripped reports whether id's run has already crossed the threshold,
// without counting a new error — the batch worker checks it after its fallback
// to decide whether to park.
func (db *DB) syncBreakerTripped(id string) bool {
	db.syncBreakerMu.Lock()
	defer db.syncBreakerMu.Unlock()
	st := db.syncBreakerStates[id]
	return st != nil && st.consecutive >= syncBreakerMaxConsecutivePermanent
}

// tripSyncBreaker emits the high-severity signal when the breaker trips — a
// distinct ERROR log and the SyncBreakerTripped metric. The caller parks the
// worker (returns) after calling this.
func (db *DB) tripSyncBreaker(id string, consecutive int, cause error) {
	db.logger.Error("sync circuit breaker tripped: consecutive permanent errors hit the cap; parking this syncable's worker without further dead-lettering — fix the config and replace the syncable",
		zap.String("id", id),
		zap.Int("consecutive_permanent", consecutive),
		zap.Int("cap", syncBreakerMaxConsecutivePermanent),
		zap.Error(cause))
	if db.metrics != nil {
		db.metrics.SyncBreakerTripped(id)
	}
}
