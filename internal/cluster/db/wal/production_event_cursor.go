package wal

// productionEventCursor binds a logical cursor to one storage binding/generation.
// The caller holds the reader lock and eventMu throughout binding and seeking.
type productionEventCursor struct {
	entryCursor
	source     *eventLogBinding
	generation uint64
}

func (r *Reader) eventCursorLocked() entryCursor {
	generation := r.s.scrubGen.Load()
	if r.cursor.entryCursor == nil || r.cursor.source != r.s.eventLog || r.cursor.generation != generation {
		if r.cursor.entryCursor != nil {
			_ = r.cursor.Close()
		}
		r.cursor = productionEventCursor{
			entryCursor: r.s.eventLog.entries.NewEntryCursor(r.raftIndex + 1),
			source:      r.s.eventLog,
			generation:  generation,
		}
	}
	return r.cursor.entryCursor
}
