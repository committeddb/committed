package wal

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// rewriteSharedPlan is the shared-backend publication step for prepared scrub
// plans. It does not reclaim retired files, mark scrub completion, or reconcile
// erasure bookkeeping. The caller supplies a fresh backend generation and must
// reopen after a backend failure before retrying. Production runScrub still uses
// native execution; callers of this experimental step exclude Close/replacement.
//
// Appends wait for the rewrite. Existing protected readers or layout freezes
// defer it; registration of new protected readers waits until this step returns.
func (s *Storage) rewriteSharedPlan(ctx context.Context, generation uint64, plan *scrubPlan) (eventlog.RewriteResult, error) {
	if ctx == nil || plan == nil {
		return eventlog.RewriteResult{}, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return eventlog.RewriteResult{}, err
	}
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	// A catch-up may begin after the worker prepared its plan. Check again
	// under append exclusion before publishing over an incoming history.
	if s.catchingUp.Load() {
		return eventlog.RewriteResult{}, errEventRewriteDeferred
	}
	s.fromZeroMu.Lock()
	defer s.fromZeroMu.Unlock()
	if s.fromZeroReads != 0 {
		return eventlog.RewriteResult{}, errEventRewriteDeferred
	}
	release, ok := s.eventLayout.move()
	if !ok {
		return eventlog.RewriteResult{}, ErrLayoutFrozen
	}
	defer release()
	s.eventMu.RLock()
	log := s.eventLog.managed
	frontier, applied := s.EventIndex(), s.AppliedIndex()
	s.eventMu.RUnlock()
	if log == nil {
		return eventlog.RewriteResult{}, eventlog.ErrUnsupported
	}
	if plan.bound > applied || applied > frontier {
		return eventlog.RewriteResult{}, eventlog.ErrInvalid
	}
	result, err := log.RewriteWithPublicationLock(ctx, generation, checkedEntryTransform(plan.transform), storageEntryPublicationLock{s})
	if err != nil {
		// Preparation can poison storage before it ever acquires publication.
		// Drop pending decoded entries so readers consult that storage state.
		s.eventMu.Lock()
		s.scrubGen.Add(1)
		s.eventMu.Unlock()
	}
	return result, err
}

// Invalidate decoded cursors before readers can observe a published generation,
// including an uncertain publication outcome. Do not call storage from Unlock:
// a backend may still hold its own lock at this point.
type storageEntryPublicationLock struct{ storage *Storage }

func (l storageEntryPublicationLock) Lock() { l.storage.eventMu.Lock() }
func (l storageEntryPublicationLock) Unlock() {
	l.storage.scrubGen.Add(1)
	l.storage.eventMu.Unlock()
}
