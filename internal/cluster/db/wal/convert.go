package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"os"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/fsutil"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// ErrEntryTooLarge refuses a conversion over a tidwall entry larger than one
// segmented record holds; a release before 0.8.3 could commit one only with a
// raised proposal cap.
var ErrEntryTooLarge = errors.New("an event-log entry is too large to convert")

// convertBatchBytes bounds the payload bytes one Append writes (and syncs).
const convertBatchBytes = 4 << 20

// convertSegmentBytes is the converted log's segment size; zero is the
// engine's default, the size a node creates. Tests shrink it to cross segments.
var convertSegmentBytes int

// Converted reports a tidwall-to-segmented event-log conversion.
type Converted struct {
	// AlreadySegmented means the event log was segmented already and nothing
	// changed: a rerun after a completed conversion is a no-op.
	AlreadySegmented bool
	// Records is how many records the log holds; LastIndex its last raft
	// index; Generation the scrub bound its bytes reflect.
	Records, LastIndex, Generation uint64
}

// isTidwallEventLog reports whether a stopped node's event log was written by
// the tidwall engine: present, non-empty, and holding none of the segmented
// engine's files. A swap interrupted after renaming events/ aside leaves the
// log at events.retired/, which Open rolls back, so that is inspected instead.
func isTidwallEventLog(dir string) (bool, error) {
	events := datadir.EventsDir(dir)
	entries, err := os.ReadDir(events)
	if errors.Is(err, os.ErrNotExist) {
		events = datadir.RetiredDir(events)
		entries, err = os.ReadDir(events)
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil || len(entries) == 0 {
		return false, err
	}
	segmented, err := segmentlog.RecognizeDirectory(events)
	if err != nil {
		return false, fmt.Errorf("inspect %s: %w", events, err)
	}
	return !segmented, nil
}

// segmentedLogOptions is the segmented log a node creates.
func segmentedLogOptions() segmentlog.LogOptions {
	return segmentlog.LogOptions{Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault}}
}

// ConvertEventLog rewrites a stopped node's tidwall event log as a segmented
// one: the same records at the same raft indexes (holes a scrub left
// included), and the same scrub generation. The copy is built in a scrub temp
// directory and checked record by record against the original before it
// replaces events/ the way a scrub swap did, so a crash at any point leaves
// either log whole (an interrupted swap is rolled back first) and the command
// can be rerun. The original is removed once the new log is in place. It holds
// the stopped-node lock throughout and opens only the event log and bbolt,
// read-only: this release runs nothing else over a tidwall log.
func ConvertEventLog(dir string) (*Converted, error) {
	lock, err := datadir.LockStoppedNodeExclusive(dir)
	if err != nil {
		return nil, err
	}
	if lock == nil {
		return nil, fmt.Errorf("%s holds no metadata database: not a node's data directory", dir)
	}
	defer func() { _ = lock.Close() }()
	if err := datadir.RecoverScrubDirs(dir); err != nil {
		return nil, fmt.Errorf("recover an interrupted swap: %w", err)
	}
	tidwall, err := isTidwallEventLog(dir)
	if err != nil {
		return nil, err
	}
	if !tidwall {
		return &Converted{AlreadySegmented: true}, nil
	}
	generation, err := completedScrubBound(lock)
	if err != nil {
		return nil, err
	}
	events := datadir.EventsDir(dir)
	src, err := openTidwallEventReader(events)
	if err != nil {
		return nil, err
	}
	tmp := datadir.ScrubDir(events, generation)
	res, err := copyToSegmented(src, tmp, generation)
	if cerr := src.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	retired := datadir.RetiredDir(events)
	if err := os.RemoveAll(retired); err != nil {
		return nil, err
	}
	if err := fsutil.SyncDir(tmp); err != nil {
		return nil, err
	}
	if err := swapEventDirectories(events, tmp, retired); err != nil {
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

// convertTidwallEventLog converts a stopped node's tidwall event log before an
// offline command rewrites the directory, so the command, and the node it
// prepares, run the segmented engine. Nil when there was nothing to convert.
func convertTidwallEventLog(dir string) (*Converted, error) {
	tidwall, err := isTidwallEventLog(dir)
	if err != nil || !tidwall {
		return nil, err
	}
	converted, err := ConvertEventLog(dir)
	if err != nil {
		return nil, fmt.Errorf("convert the tidwall event log: %w", err)
	}
	return converted, nil
}

// swapEventDirectories moves live aside to retired and replacement into its
// place, putting live back if the second rename fails. A crash between the two
// renames leaves retired alone, which datadir.RecoverScrubDirs restores.
func swapEventDirectories(live, replacement, retired string) error {
	if err := os.Rename(live, retired); err != nil {
		return fmt.Errorf("move the event log aside: %w", err)
	}
	if err := os.Rename(replacement, live); err != nil {
		if rerr := os.Rename(retired, live); rerr != nil {
			return fmt.Errorf("rename the converted log into place: %w (putting the original back failed: %v)", err, rerr)
		}
		return fmt.Errorf("rename the converted log into place: %w", err)
	}
	return nil
}

// completedScrubBound is the scrub generation a tidwall log's bytes reflect,
// which that engine kept only in bbolt.
func completedScrubBound(db *bolt.DB) (uint64, error) {
	var bound uint64
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(pendingScrubBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if v := b.Get(scrubCompletedKey); len(v) == 8 {
			bound = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return bound, err
}

// copyToSegmented writes src's records into a new segmented log at tmp with
// the given generation and verifies it there.
func copyToSegmented(src *tidwallEventReader, tmp string, generation uint64) (*Converted, error) {
	if err := os.RemoveAll(tmp); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(tmp, 0o750); err != nil {
		return nil, err
	}
	res := &Converted{Generation: generation}
	opts := segmentedLogOptions()
	opts.SegmentBytes = convertSegmentBytes
	dst, err := segmented.Create(tmp, 1, opts)
	if err != nil {
		return nil, fmt.Errorf("create the segmented event log: %w", err)
	}
	if generation > 0 {
		// An empty rewrite publishes only the generation; any record aborts it.
		if _, err := dst.Rewrite(context.Background(), generation, func(eventlog.Record) ([]byte, bool, error) {
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
		// Compress each segment as it seals, as a running node's sealer would,
		// so the copy never holds more than one uncompressed segment.
		for {
			did, err := dst.CompressNextSealed()
			if err != nil {
				return fmt.Errorf("compress the segmented event log: %w", err)
			}
			if !did {
				return nil
			}
		}
	}
	err = src.each(func(r eventlog.Record) error {
		if len(r.Payload) > segmentlog.MaxRecordBytes {
			return fmt.Errorf("raft index %d holds a %d-byte entry, over the %d bytes this release stores in one event-log record: %w",
				r.ID, len(r.Payload), segmentlog.MaxRecordBytes, ErrEntryTooLarge)
		}
		batch = append(batch, r)
		size += len(r.Payload)
		res.Records++
		res.LastIndex = r.ID
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
	if err := verifyConverted(src, tmp, res); err != nil {
		return nil, fmt.Errorf("the converted event log does not match the original (nothing was replaced): %w", err)
	}
	return res, nil
}

// verifyConverted reopens the segmented log at tmp and checks it holds exactly
// src's records, generation and append progress, walking both in step.
func verifyConverted(src *tidwallEventReader, tmp string, want *Converted) error {
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
	next, stop := iter.Pull2(src.records())
	defer stop()
	var n uint64
	err = dst.Scan(context.Background(), eventlog.Coverage{Start: 0, End: ^uint64(0)}, func(got eventlog.Record) error {
		r, err, ok := next()
		if !ok {
			return fmt.Errorf("an extra record at raft index %d", got.ID)
		}
		if err != nil {
			return err
		}
		if got.ID != r.ID || !bytes.Equal(got.Payload, r.Payload) {
			return fmt.Errorf("record %d differs (found raft index %d)", r.ID, got.ID)
		}
		n++
		return nil
	})
	if err != nil {
		return err
	}
	if r, err, ok := next(); ok {
		return fmt.Errorf("raft index %d is missing (%v)", r.ID, err)
	}
	if n != want.Records {
		return fmt.Errorf("%d records, want %d", n, want.Records)
	}
	return nil
}
