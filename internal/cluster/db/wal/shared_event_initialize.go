package wal

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// initializeFetchedGeneration assigns the source generation to an empty shared
// receiver. Same-generation retries are no-ops, including after partial receive.
// No history (even fully erased history) may be relabeled. The caller holds a
// catch-up fence and excludes Close/replacement. This internal experiment does
// not install snapshots or mark scrubs complete; interrupted transfers must be
// resumed in safe mode until application catch-up recovery is integrated.
func (s *Storage) initializeFetchedGeneration(ctx context.Context, generation uint64) error {
	if ctx == nil {
		return eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	if !s.catchingUp.Load() {
		return eventlog.ErrInvalid
	}
	s.fromZeroMu.Lock()
	defer s.fromZeroMu.Unlock()
	if s.fromZeroReads != 0 {
		return errEventRewriteDeferred
	}
	release, ok := s.eventLayout.move()
	if !ok {
		return ErrLayoutFrozen
	}
	defer release()
	s.eventMu.RLock()
	log := s.eventLog.managed
	s.eventMu.RUnlock()
	if log == nil {
		return eventlog.ErrUnsupported
	}
	selected, err := log.Generation()
	if err != nil {
		return err
	}
	if selected == generation {
		return nil
	}
	if generation < selected {
		return eventlog.ErrInvalid
	}
	_, hasHistory, err := log.LastAppended()
	if err != nil {
		return err
	}
	if hasHistory || s.EventIndex() != 0 || s.AppliedIndex() != 0 {
		return eventlog.ErrInvalid
	}
	// An empty rewrite publishes only storage generation metadata. Encountering
	// any record contradicts the empty-history check and must abort publication.
	_, err = log.RewriteWithPublicationLock(ctx, generation, func(eventlog.Record) ([]byte, bool, error) {
		return nil, false, eventlog.ErrInvalid
	}, storageEntryPublicationLock{s})
	if err != nil {
		s.eventMu.Lock()
		s.scrubGen.Add(1)
		s.eventMu.Unlock()
	}
	return err
}
