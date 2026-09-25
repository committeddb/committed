package wal

import (
	"context"
	"fmt"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// reclaimSharedGeneration removes retired managed files only after confirming
// the selected generation. The caller must establish which plan that generation
// represents. Equality alone does not prove authorization or scrub completion.
// This step never updates completion or erasure bookkeeping. Backend failures
// require reopen before retry; reclamation itself is idempotent after reopen.
// As with shared publication, callers exclude Close and binding replacement.
func (s *Storage) reclaimSharedGeneration(ctx context.Context, generation uint64) (eventlog.ReclaimResult, error) {
	if ctx == nil {
		return eventlog.ReclaimResult{}, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return eventlog.ReclaimResult{}, err
	}
	s.eventAppendMu.Lock()
	defer s.eventAppendMu.Unlock()
	release, ok := s.eventLayout.move()
	if !ok {
		return eventlog.ReclaimResult{}, ErrLayoutFrozen
	}
	defer release()
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	log := s.eventLog.managed
	if log == nil {
		return eventlog.ReclaimResult{}, eventlog.ErrUnsupported
	}
	selected, err := log.Generation()
	if err != nil {
		s.scrubGen.Add(1)
		return eventlog.ReclaimResult{}, err
	}
	if selected != generation {
		return eventlog.ReclaimResult{}, fmt.Errorf("event reclamation expected generation %d, selected %d: %w", generation, selected, eventlog.ErrInvalid)
	}
	result, err := log.Reclaim(ctx)
	if err != nil {
		// A failure can poison the backend. Retained decoded entries must not
		// let readers bypass its recovery-required state.
		s.scrubGen.Add(1)
	}
	return result, err
}
