package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	tidwallbackend "github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/fsutil"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// convertBatchBytes bounds the payload bytes one Append writes (and syncs).
const convertBatchBytes = 4 << 20

// Converted reports a tidwall-to-segmented event-log conversion.
type Converted struct {
	// AlreadySegmented means the event log was segmented already and nothing
	// changed: a rerun after a completed conversion is a no-op.
	AlreadySegmented bool
	// Records is how many records the log holds; LastIndex its last raft
	// index; Generation the scrub bound its bytes reflect.
	Records, LastIndex, Generation uint64
}

// segmentedLogOptions is the segmented log a node creates.
func segmentedLogOptions() segmentlog.LogOptions {
	return segmentlog.LogOptions{Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}}
}

// ConvertEventLog rewrites a stopped node's tidwall event log as a segmented
// one: the same records at the same raft indexes (holes a scrub left
// included), the same scrub generation and append progress. The copy is built
// in a scrub temp directory and checked record by record against the original
// before it replaces events/ the way a scrub swap does, so a crash at any point
// leaves either log whole (Open's recovery rolls an interrupted swap back or
// reaps the temp copy) and the command can be rerun. The original is removed
// once the new log is in place.
func ConvertEventLog(dir string) (*Converted, error) {
	s, err := openStopped(dir)
	if err != nil {
		return nil, err
	}
	if s.eventLog.managed != nil {
		return &Converted{AlreadySegmented: true}, s.Close()
	}
	events := datadir.EventsDir(dir)
	tmp := datadir.ScrubDir(events, s.EventLogGeneration())
	res, err := s.copyToSegmented(tmp)
	if cerr := s.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	// The storage lock went with Close; hold the stopped-node lock across the
	// swap so a node cannot start on a half-swapped directory.
	lock, err := datadir.LockStoppedNodeExclusive(dir)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}
	defer func() { _ = lock.Close() }()
	retired := datadir.RetiredDir(events)
	if err := os.RemoveAll(retired); err != nil {
		return nil, err
	}
	if err := fsutil.SyncDir(tmp); err != nil {
		return nil, err
	}
	if err := tidwallbackend.SwapLegacyDirectories(events, tmp, retired); err != nil {
		return nil, fmt.Errorf("swap the converted event log into place: %w", err)
	}
	if err := fsutil.SyncDir(dir); err != nil {
		return nil, fmt.Errorf("the converted event log is in place but its rename may not survive a crash: %w", err)
	}
	if err := os.RemoveAll(retired); err != nil {
		return nil, fmt.Errorf("the converted event log is in place; remove %s: %w", retired, err)
	}
	return res, nil
}

// copyToSegmented writes s's event log into a new segmented log at tmp and
// verifies it there.
func (s *Storage) copyToSegmented(tmp string) (*Converted, error) {
	if err := os.RemoveAll(tmp); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmp, 0o750); err != nil {
		return nil, err
	}
	res := &Converted{Generation: s.EventLogGeneration()}
	last, hasHistory, err := s.eventLog.entries.LastAppended()
	if err != nil {
		return nil, fmt.Errorf("read the event log's last index: %w", err)
	}
	if hasHistory {
		res.LastIndex = last
	}

	dst, err := segmented.Create(tmp, 1, segmentedLogOptions())
	if err != nil {
		return nil, fmt.Errorf("create the segmented event log: %w", err)
	}
	if res.Generation > 0 {
		// An empty rewrite publishes only the generation; any record aborts it.
		if _, err := dst.Rewrite(context.Background(), res.Generation, func(eventlog.Record) ([]byte, bool, error) {
			return nil, false, eventlog.ErrInvalid
		}); err != nil {
			_ = dst.Close()
			return nil, fmt.Errorf("set the segmented event log's generation: %w", err)
		}
	}
	var batch []eventlog.Record
	size := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := dst.Append(batch); err != nil {
			return fmt.Errorf("append raft indexes %d-%d: %w", batch[0].ID, batch[len(batch)-1].ID, err)
		}
		batch, size = batch[:0], 0
		return nil
	}
	err = s.eachEventRecord(func(r eventlog.Record) error {
		batch = append(batch, r)
		size += len(r.Payload)
		res.Records++
		if size >= convertBatchBytes {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	if err := s.verifyConverted(tmp, res); err != nil {
		return nil, fmt.Errorf("the converted event log does not match the original (nothing was replaced): %w", err)
	}
	return res, nil
}

// eachEventRecord visits every record of s's event log in raft-index order.
func (s *Storage) eachEventRecord(visit func(eventlog.Record) error) error {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	cur := s.eventLog.records()
	defer func() { _ = cur.Close() }()
	for next := uint64(0); ; {
		r, err := cur.Seek(next)
		if errors.Is(err, eventlog.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read the event log at raft index %d: %w", next, err)
		}
		if err := visit(r); err != nil {
			return err
		}
		next = r.ID + 1
	}
}

// verifyConverted reopens the segmented log at tmp and checks it holds exactly
// s's records, generation and append progress.
func (s *Storage) verifyConverted(tmp string, want *Converted) error {
	dst, err := segmented.Open(tmp, segmentedLogOptions().Encoding)
	if err != nil {
		return err
	}
	defer func() { _ = dst.Close() }()
	if gen, err := dst.Generation(); err != nil || gen != want.Generation {
		return fmt.Errorf("generation %d, want %d (%v)", gen, want.Generation, err)
	}
	last, hasHistory, err := dst.LastAppended()
	if err != nil {
		return err
	}
	if hasHistory != (want.Records > 0) || (hasHistory && last != want.LastIndex) {
		return fmt.Errorf("last index %d, want %d", last, want.LastIndex)
	}
	cur := dst.NewCursor()
	defer func() { _ = cur.Close() }()
	var n, next uint64
	err = s.eachEventRecord(func(r eventlog.Record) error {
		got, err := cur.Seek(next)
		if err != nil {
			return fmt.Errorf("raft index %d: %w", r.ID, err)
		}
		if got.ID != r.ID || !bytes.Equal(got.Payload, r.Payload) {
			return fmt.Errorf("record %d differs (found raft index %d)", r.ID, got.ID)
		}
		n++
		next = r.ID + 1
		return nil
	})
	if err != nil {
		return err
	}
	if extra, err := cur.Seek(next); !errors.Is(err, eventlog.ErrNotFound) {
		return fmt.Errorf("an extra record at raft index %d (%v)", extra.ID, err)
	}
	if n != want.Records {
		return fmt.Errorf("%d records, want %d", n, want.Records)
	}
	return nil
}
