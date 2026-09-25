package wal

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// serveRecordEvents implements the existing record wire format over a logical
// cursor. The caller holds the event layout freeze, preserving generation and
// cursor ownership while callbacks run without eventMu held. Appends may continue;
// the stream stops at its captured frontier. Native whole-file serving is separate.
func (s *Storage) serveRecordEvents(ctx context.Context, after, to uint64, sink db.EventSink, maxBytes int) (db.EventServeResult, error) {
	var result db.EventServeResult
	if err := ctx.Err(); err != nil {
		return result, err
	}
	s.eventMu.RLock()
	log := s.eventLog
	generation, err := log.managed.Generation()
	if err == nil {
		result.EventIndex, _, err = log.entries.LastAppended()
	}
	s.eventMu.RUnlock()
	if err != nil {
		return result, err
	}
	result.Generation = generation
	if err := sink.Begin(result.Generation, result.EventIndex); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	end := min(to, result.EventIndex)
	if after >= end {
		return result, sink.End(result)
	}
	cursor := log.records()
	defer func() { _ = cursor.Close() }()
	var data []byte
	var last uint64
	for next := after + 1; next <= end; {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		record, err := cursor.Seek(next)
		if errors.Is(err, eventlog.ErrNotFound) {
			break
		}
		if err != nil {
			return result, err
		}
		if record.ID > end {
			break
		}
		// Like native serving, allow the first record crossing the byte budget.
		// Probe the next survivor before reporting More, even across erased gaps.
		if len(data) >= maxBytes {
			result.More = true
			break
		}
		data = binary.AppendUvarint(data, uint64(frameHeaderSize+len(record.Payload)))
		data = appendFrame(data, record.Payload)
		last = record.ID
		if record.ID == end {
			break
		}
		next = record.ID + 1
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if len(data) > 0 {
		if err := sink.Records(data); err != nil {
			return result, err
		}
		result.LastIndex = last
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, sink.End(result)
}
