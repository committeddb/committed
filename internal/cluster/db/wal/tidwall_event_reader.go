package wal

import (
	"fmt"
	"iter"

	twal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// tidwallEventReader reads the event log a node before 0.8.3 wrote with the
// tidwall engine. Only ConvertEventLog uses it: 0.8.3 runs nothing else over
// that format. Records come back in log order, each checksum verified.
type tidwallEventReader struct{ log *twal.Log }

func openTidwallEventReader(dir string) (*tidwallEventReader, error) {
	log, err := twal.Open(dir, &twal.Options{SealedSegmentCompression: twal.CompressionZstd})
	if err != nil {
		return nil, logOpenError(dir, "event_log", nil, err)
	}
	return &tidwallEventReader{log: log}, nil
}

func (r *tidwallEventReader) Close() error { return r.log.Close() }

// records yields every record from the first, in order, stopping at the first
// error.
func (r *tidwallEventReader) records() iter.Seq2[eventlog.Record, error] {
	return func(yield func(eventlog.Record, error) bool) {
		first, err := r.log.FirstIndex()
		if err != nil {
			yield(eventlog.Record{}, err)
			return
		}
		last, err := r.log.LastIndex()
		if err != nil {
			yield(eventlog.Record{}, err)
			return
		}
		var prev uint64
		for seq := first; seq != 0 && seq <= last; seq++ {
			rec, err := r.read(seq)
			if err == nil && rec.ID <= prev {
				err = fmt.Errorf("event log sequence %d holds raft index %d after %d: %w", seq, rec.ID, prev, ErrCorruptEntry)
			}
			if !yield(rec, err) || err != nil {
				return
			}
			prev = rec.ID
		}
	}
}

func (r *tidwallEventReader) read(seq uint64) (eventlog.Record, error) {
	raw, err := r.log.Read(seq)
	if err != nil {
		return eventlog.Record{}, fmt.Errorf("read event log sequence %d: %w", seq, err)
	}
	payload, err := unframe(raw)
	if err != nil {
		return eventlog.Record{}, fmt.Errorf("event log sequence %d: %w", seq, err)
	}
	records, err := eventEntryRecords([][]byte{payload})
	if err != nil {
		return eventlog.Record{}, fmt.Errorf("event log sequence %d: %w", seq, err)
	}
	return records[0], nil
}

// each visits every record in order.
func (r *tidwallEventReader) each(visit func(eventlog.Record) error) error {
	for rec, err := range r.records() {
		if err != nil {
			return err
		}
		if err := visit(rec); err != nil {
			return err
		}
	}
	return nil
}
