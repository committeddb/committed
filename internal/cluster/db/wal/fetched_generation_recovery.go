package wal

import (
	"context"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// recoverFetchedGeneration runs before background maintenance starts. Catch-up
// persists the source generation in application metadata before initializing
// the empty backend. Complete that publication after interruption; a log with
// append history cannot be relabeled. Native generation lives only in metadata.
func (s *Storage) recoverFetchedGeneration(completed uint64) error {
	log := s.eventLog.managed
	if log == nil || completed == 0 {
		return nil
	}
	selected, err := log.Generation()
	if err != nil {
		return err
	}
	if selected >= completed {
		return nil
	}
	_, hasHistory, err := log.LastAppended()
	if err != nil {
		return err
	}
	if hasHistory || s.EventIndex() != 0 {
		return eventlog.ErrInvalid
	}
	_, err = log.Rewrite(context.Background(), completed, func(eventlog.Record) ([]byte, bool, error) {
		return nil, false, eventlog.ErrInvalid
	})
	return err
}
