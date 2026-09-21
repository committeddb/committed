package wal

import (
	"errors"
	"fmt"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

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
