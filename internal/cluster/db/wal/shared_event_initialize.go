package wal

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// initializeFetchedGeneration assigns the source generation to an empty shared
// receiver. Same-generation retries are no-ops, including after partial receive.
// No history (even fully erased history) may be relabeled. The caller holds a
// catch-up fence and excludes Close/replacement. This storage-only helper does
// not install snapshots or record application completion; its tests reopen in
// safe mode. SetEventLogGeneration supplies the application persistence step.
func (s *Storage) initializeFetchedGeneration(ctx context.Context, generation uint64) error {
	return s.initializeFetchedGenerationWith(ctx, generation, nil)
}

// beforePublish persists the existing application generation after eligibility
// checks but before storage publication. It also runs on same-generation retry.
func (s *Storage) initializeFetchedGenerationWith(ctx context.Context, generation uint64, beforePublish func() error) error {
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
		if beforePublish != nil {
			return beforePublish()
		}
		return nil
	}
	if generation < selected {
		return eventlog.ErrInvalid
	}
	_, hasHistory, err := log.LastAppended()
	if err != nil {
		return err
	}
	if hasHistory || s.EventIndex() != 0 {
		return eventlog.ErrInvalid
	}
	if beforePublish != nil {
		if err := beforePublish(); err != nil {
			return err
		}
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
