package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

const (
	eventCopyBatchBytes   = 1 << 20
	eventCopyBatchRecords = 256
)

// copyEventLog copies the legacy permanent log into a privately owned, fresh
// experimental backend. The caller owns destination and must discard the entire
// copy on ANY error: a synced prefix is not a completed copy and cannot be resumed.
// Success does not activate a format, copy BoltDB/Raft state, or make a backup.
// No production startup path calls this method.
//
// eventMu is held exclusively for the complete copy, blocking appends and swaps.
// This is an offline experiment, not a live migration. The caller must also
// exclude Storage.Close. Cancellation is checked after acquiring the lock and
// between reads/appends; it cannot interrupt lock acquisition or a backend sync.
// Destination methods must not reenter Storage.
//
// Batches are bounded by count and payload bytes, except that one large record
// travels alone (and may exceed the destination backend's supported record size).
// Exact protobuf bytes, including unknown fields, survive checksum removal.
func (s *Storage) copyEventLog(ctx context.Context, destination eventlog.EventLog) error {
	if ctx == nil || destination == nil {
		return eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.eventMu.Lock()
	defer s.eventMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.eventLog == nil {
		return eventlog.ErrInvalid
	}
	if _, has, err := destination.LastAppended(); err != nil {
		return err
	} else if has {
		return fmt.Errorf("event copy requires empty append history: %w", eventlog.ErrInvalid)
	}
	first, err := s.firstEventSeqLocked()
	if err != nil {
		return err
	}
	last, err := s.lastEventSeqLocked()
	if err != nil {
		return err
	}
	head := s.eventIndex.Load()
	if (first == 0) != (last == 0) || first > last || last == ^uint64(0) || (last == 0 && head != 0) {
		return fmt.Errorf("invalid legacy event coverage: %w", ErrCorruptEntry)
	}
	if last == 0 {
		return ctx.Err()
	}
	batch := make([]eventlog.Record, 0, eventCopyBatchRecords)
	batchBytes := 0
	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := destination.Append(batch); err != nil {
			return fmt.Errorf("append event copy: %w", err)
		}
		clear(batch)
		batch = batch[:0]
		batchBytes = 0
		return nil
	}
	var previous uint64
	for seq := first; seq <= last; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw, err := s.readEventAtLocked(seq)
		if err != nil {
			if errors.Is(err, tidwal.ErrNotFound) || errors.Is(err, tidwal.ErrCorrupt) {
				err = errors.Join(ErrCorruptEntry, err)
			}
			return fmt.Errorf("read event copy sequence %d: %w", seq, err)
		}
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil {
			return errors.Join(ErrCorruptEntry, err)
		}
		id := entry.GetIndex()
		if id <= previous || id == ^uint64(0) || id > head || (seq == last && id != head) {
			return fmt.Errorf("invalid legacy event index at sequence %d: %w", seq, ErrCorruptEntry)
		}
		if len(batch) > 0 && (len(batch) == eventCopyBatchRecords || len(raw) > eventCopyBatchBytes-batchBytes) {
			if err := flush(); err != nil {
				return err
			}
		}
		// A legacy handle may use NoCopy; the next Read may invalidate raw.
		batch = append(batch, eventlog.Record{ID: id, Payload: bytes.Clone(raw)})
		batchBytes += len(raw)
		previous = id
	}
	if err := flush(); err != nil {
		return err
	}
	return ctx.Err()
}
