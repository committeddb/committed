package wal

import (
	"context"
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/pkg/segmentlog"
)

// segmentEventLog is an experimental raw-entry adapter. It is intentionally not
// wired into Storage: visibility, metadata recovery, backup, and peer protocols
// still require integration. The caller owns the supplied log and its Close.
// Payloads are unframed raftpb.Entry bytes; segmentlog supplies integrity framing.
// IDs are the entries' Raft indexes, never tidwall sequence numbers.
type segmentEventLog struct{ log *segmentlog.Log }

// appendRaw validates the entire batch before appending and preserves the exact
// input bytes, including protobuf unknown fields. Append failure can leave a
// durable prefix; callers must reopen and reconcile before retrying. This method
// does not implement Storage's applied-index or replay-deduplication protocol.
func (l segmentEventLog) appendRaw(payloads [][]byte) error {
	records := make([]segmentlog.Record, 0, len(payloads))
	for _, raw := range payloads {
		entry := new(pb.Entry)
		if err := proto.Unmarshal(raw, entry); err != nil || entry.GetIndex() == 0 || entry.GetIndex() == ^uint64(0) {
			return fmt.Errorf("invalid event entry: %w", segmentlog.ErrInvalid)
		}
		records = append(records, segmentlog.Record{ID: entry.GetIndex(), Payload: raw})
	}
	return l.log.Append(records)
}

// readRaw and seekRaw retain ErrNotFound for absent/erased indexes. Corruption
// must never be interpreted as a gap or EOF. Returned payloads belong to the caller.
func (l segmentEventLog) readRaw(index uint64) ([]byte, error) {
	r, err := l.log.Read(index)
	return checkedSegmentEntry(r, err)
}

func (l segmentEventLog) seekRaw(index uint64) (uint64, []byte, error) {
	r, err := l.log.Seek(index)
	raw, err := checkedSegmentEntry(r, err)
	if err != nil {
		return 0, nil, err
	}
	return r.ID, raw, nil
}

func checkedSegmentEntry(r segmentlog.Record, err error) ([]byte, error) {
	if err != nil {
		if errors.Is(err, segmentlog.ErrCorrupt) {
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
	return r.Payload, nil
}

// rewriteRaw adapts an existing raw-entry transformation such as scrubFilterEntry.
// The caller supplies selections already capped at the authorized scrub bound.
// It does not authorize a scrub, update BoltDB, or declare physical erasure done.
// Even removal validates the original entry's identity before invoking transform;
// surviving replacements must retain it. Callbacks may not reenter the log.
func (l segmentEventLog) rewriteRaw(ctx context.Context, generation uint64, transform func([]byte) (bool, []byte, error)) (segmentlog.RewriteResult, error) {
	if transform == nil {
		return segmentlog.RewriteResult{}, segmentlog.ErrInvalid
	}
	return l.log.Rewrite(ctx, generation, func(r segmentlog.Record) ([]byte, bool, error) {
		raw, err := checkedSegmentEntry(r, nil)
		if err != nil {
			return nil, false, err
		}
		keep, payload, err := transform(raw)
		if err != nil || !keep {
			return nil, false, err
		}
		payload, err = checkedSegmentEntry(segmentlog.Record{ID: r.ID, Payload: payload}, nil)
		return payload, true, err
	})
}
