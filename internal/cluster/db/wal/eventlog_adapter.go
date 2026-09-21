package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// eventLogAdapter is an experimental raw-entry adapter. It is intentionally not
// wired into Storage: reader visibility is experimental; metadata recovery, backup, and peer protocols
// still require integration. The caller owns the supplied log and its Close.
// Payloads are unframed raftpb.Entry bytes; each backend supplies integrity framing.
// IDs are the entries' Raft indexes, never tidwall sequence numbers.
// The adapter must not be copied after use. Mutations must go through it while
// readers are live; its lock protects each complete Read from rewrite publication.
type eventLogAdapter struct {
	mu             sync.RWMutex
	log            eventlog.EventLog
	protectedReads atomic.Int64
}

// appendRaw validates the entire batch before appending and preserves the exact
// input bytes, including protobuf unknown fields. Append failure can leave a
// durable prefix; callers must reopen and reconcile before retrying. This method
// does not implement Storage's applied-index or replay-deduplication protocol.
func (l *eventLogAdapter) appendRaw(payloads [][]byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	records, err := eventEntryRecords(payloads)
	if err != nil {
		return err
	}
	return l.log.Append(records)
}

func eventEntryRecords(payloads [][]byte) ([]eventlog.Record, error) {
	records := make([]eventlog.Record, 0, len(payloads))
	var previous uint64
	for _, raw := range payloads {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil || entry.GetIndex() == 0 || entry.GetIndex() == ^uint64(0) || entry.GetIndex() <= previous {
			return nil, fmt.Errorf("invalid event entry batch: %w", eventlog.ErrInvalid)
		}
		previous = entry.GetIndex()
		records = append(records, eventlog.Record{ID: previous, Payload: raw})
	}
	return records, nil
}

// eventIndex reports original durable append progress, including erased entries.
// It returns zero for a fresh log. A poisoned handle must be reopened first.
func (l *eventLogAdapter) eventIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.eventIndexLocked()
}

func (l *eventLogAdapter) eventIndexLocked() (uint64, error) {
	index, ok, err := l.log.LastAppended()
	if err != nil {
		return 0, err
	}
	if ok && index == 0 {
		return 0, ErrCorruptEntry
	}
	return index, nil
}

// appendCommittedRaw appends only the portion of a validated committed batch
// above recovered append progress. It is for replay of the SAME committed
// history, not conflict detection: erased entries cannot be compared with their
// original payloads. Skipped input is still decoded and checked for ordering.
// Empty/all-replayed batches make no writes and return the existing frontier.
// On error the returned index is unusable; reopen after storage failure before
// retrying. Success does not apply entries to BoltDB or advance AppliedIndex.
func (l *eventLogAdapter) appendCommittedRaw(payloads [][]byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	records, err := eventEntryRecords(payloads)
	if err != nil {
		return 0, err
	}
	index, err := l.eventIndexLocked()
	if err != nil {
		return 0, err
	}
	first := 0
	for first < len(records) && records[first].ID <= index {
		first++
	}
	if first == len(records) {
		return index, nil
	}
	if err := l.log.Append(records[first:]); err != nil {
		return 0, err
	}
	return records[len(records)-1].ID, nil
}

// readRaw and seekRaw retain ErrNotFound for absent/erased indexes. Corruption
// must never be interpreted as a gap or EOF. Returned payloads belong to the caller.
// Each lookup uses one atomic backend operation, so it needs no adapter read lock.
// Separate calls may observe different rewrite generations.
func (l *eventLogAdapter) readRaw(index uint64) ([]byte, error) {
	r, err := l.log.Read(index)
	return checkedEventEntry(r, err)
}

func (l *eventLogAdapter) seekRaw(index uint64) (uint64, []byte, error) {
	r, err := l.log.Seek(index)
	raw, err := checkedEventEntry(r, err)
	if err != nil {
		return 0, nil, err
	}
	return r.ID, raw, nil
}

func checkedEventEntry(r eventlog.Record, err error) ([]byte, error) {
	if _, err := decodeEventEntry(r, err); err != nil {
		return nil, err
	}
	return r.Payload, nil
}

// decodeEventEntry validates framing errors and logical identity once, returning
// the decoded entry for callers that also need to interpret its contents.
func decodeEventEntry(r eventlog.Record, err error) (*pb.Entry, error) {
	if err != nil {
		if errors.Is(err, eventlog.ErrCorrupt) {
			return nil, errors.Join(ErrCorruptEntry, err)
		}
		return nil, err
	}
	entry := new(pb.Entry)
	if err := proto.Unmarshal(r.Payload, entry); err != nil {
		return nil, errors.Join(ErrCorruptEntry, err)
	}
	if r.ID == 0 || entry.GetIndex() != r.ID {
		return nil, fmt.Errorf("event record index mismatch: %w", ErrCorruptEntry)
	}
	return entry, nil
}

// rewriteRaw adapts an existing raw-entry transformation such as scrubFilterEntry.
// The caller supplies selections already capped at the authorized scrub bound.
// It does not authorize a scrub, update BoltDB, or declare physical erasure done.
// Even removal validates the original entry's identity before invoking transform;
// surviving replacements must retain it. Callbacks may not reenter the log.
func (l *eventLogAdapter) rewriteRaw(ctx context.Context, generation uint64, transform func([]byte) (bool, []byte, error)) (eventlog.RewriteResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rewriteRawLocked(ctx, generation, transform)
}

// rewriteRawLocked requires the adapter write lock throughout preparation and publication.
func (l *eventLogAdapter) rewriteRawLocked(ctx context.Context, generation uint64, transform func([]byte) (bool, []byte, error)) (eventlog.RewriteResult, error) {
	if ctx == nil || transform == nil {
		return eventlog.RewriteResult{}, eventlog.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return eventlog.RewriteResult{}, err
	}
	if l.protectedReads.Load() > 0 {
		return eventlog.RewriteResult{}, errEventRewriteDeferred
	}
	return l.log.Rewrite(ctx, generation, func(r eventlog.Record) ([]byte, bool, error) {
		raw, err := checkedEventEntry(r, nil)
		if err != nil {
			return nil, false, err
		}
		keep, payload, err := transform(raw)
		if err != nil || !keep {
			return nil, false, err
		}
		payload, err = checkedEventEntry(eventlog.Record{ID: r.ID, Payload: payload}, nil)
		return payload, true, err
	})
}
