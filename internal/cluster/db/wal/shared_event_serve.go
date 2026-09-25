package wal

import (
	"context"
	"errors"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// checkedRecordCursor validates logical identity while preserving original bytes.
// Native positioning does this in its codec, without a second decode here.
type checkedRecordCursor struct{ eventlog.Cursor }

func (c checkedRecordCursor) Seek(index uint64) (eventlog.Record, error) {
	record, err := c.Cursor.Seek(index)
	if _, err := decodeEventEntry(record, err); err != nil {
		return eventlog.Record{}, err
	}
	return record, nil
}

type fetchedEntryBatch struct {
	generation uint64
	frontier   uint64
	after      uint64 // Last returned ID, or the covered bound when done.
	done       bool
	payloads   [][]byte
}

// fetchEntries returns original protobuf bytes in (after, to], capped at the
// captured append frontier. Publication is excluded for the batch; appends may
// continue. Resume with batch.after and verify generation on every batch.
// The first record may exceed maxBytes so a large record cannot stall progress.
// This internal experiment does not install erased-tail append accounting or
// provide a public wire protocol. Callers exclude Close.
func (s *Storage) fetchEntries(ctx context.Context, after, to uint64, maxRecords, maxBytes int) (fetchedEntryBatch, error) {
	if ctx == nil || maxRecords <= 0 || maxBytes <= 0 || after > to {
		return fetchedEntryBatch{}, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return fetchedEntryBatch{}, err
	}
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	result := fetchedEntryBatch{generation: s.EventLogGeneration(), after: after}
	var err error
	if s.eventLog.managed != nil {
		result.generation, err = s.eventLog.managed.Generation()
		if err != nil {
			return fetchedEntryBatch{}, err
		}
	}
	result.frontier, _, err = s.eventLog.entries.LastAppended()
	if err != nil {
		return fetchedEntryBatch{}, err
	}
	end := min(to, result.frontier)
	if after >= end {
		result.done = true
		return result, nil
	}
	cursor := s.eventLog.records()
	defer func() { _ = cursor.Close() }()
	used := 0
	for len(result.payloads) < maxRecords {
		if err := ctx.Err(); err != nil {
			return fetchedEntryBatch{}, err
		}
		record, err := cursor.Seek(result.after + 1)
		if errors.Is(err, eventlog.ErrNotFound) {
			result.after, result.done = end, true
			return result, nil
		}
		if err != nil {
			return fetchedEntryBatch{}, err
		}
		if record.ID > end {
			result.after, result.done = end, true
			return result, nil
		}
		if len(result.payloads) > 0 && len(record.Payload) > maxBytes-used {
			return result, nil
		}
		result.payloads = append(result.payloads, record.Payload)
		result.after = record.ID
		if record.ID == end {
			result.done = true
			return result, nil
		}
		// Check before adding to avoid overflow for a first oversized record.
		if len(record.Payload) >= maxBytes-used {
			return result, nil
		}
		used += len(record.Payload)
	}
	return result, nil
}
