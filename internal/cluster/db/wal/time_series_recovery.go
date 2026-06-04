package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nakabonne/tstorage"
	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// This file owns the durability story for the time-series store. tstorage is
// a PURE DERIVED VIEW: every point is one user-defined entity (type id +
// timestamp), all of which is already durable in the permanent event log via
// handleUserDefined. tstorage itself never fsyncs and keeps its newest
// partition in memory, so a crash loses the in-memory head — and because the
// on-disk partitions it does keep are flushed mid-run (not aligned to any
// watermark), the surviving disk state is not a clean index-prefix.
//
// So the model is: trust the on-disk view only after a CLEAN shutdown (proven
// by the tsAppliedIndex watermark equalling appliedIndex), and otherwise throw
// it away and rebuild from the event log over the retention window. Clean
// restarts pay nothing; an unclean restart pays a bounded rebuild.

// timeSeriesRetention bounds how far back the view (and therefore a rebuild)
// reaches. It is passed explicitly to tstorage so the rebuild window matches
// what tstorage would actually retain — rather than silently depending on the
// library's default (which happens to be the same 336h today).
const timeSeriesRetention = 336 * time.Hour

// openTimeSeries constructs the tstorage handle. On disk it lives under dir
// (this node's per-node time-series directory); in-memory mode (tests) keeps
// nothing on disk. Timestamps are milliseconds to match the propose path,
// which stamps Entity.Timestamp in unix milliseconds.
func openTimeSeries(dir string, inMemory bool) (tstorage.Storage, error) {
	opts := []tstorage.Option{
		tstorage.WithTimestampPrecision(tstorage.Milliseconds),
		tstorage.WithRetention(timeSeriesRetention),
	}
	if !inMemory {
		opts = append(opts, tstorage.WithDataPath(dir))
	}
	return tstorage.NewStorage(opts...)
}

// recoverTimeSeries reconciles the derived time-series view with the committed
// event log on Open. If the watermark proves a clean shutdown (tsAppliedIndex
// >= appliedIndex) the on-disk view is a trustworthy prefix and we keep it.
// Otherwise we discard it and rebuild from the event log — safe because the
// view is fully derivable. inMemory mode has no disk to discard; it just opens
// a fresh empty store and rebuilds (a no-op on a brand-new node).
func (s *Storage) recoverTimeSeries(dir string, inMemory bool) error {
	applied := s.appliedIndex.Load()
	watermark, err := s.loadTSAppliedIndex()
	if err != nil {
		return err
	}
	if watermark >= applied {
		// Clean shutdown (or nothing applied yet): the on-disk store is a
		// complete prefix of the committed log; trust it as-is. We leave the
		// lag-gauge cursor at 0 rather than scanning every metric to find the
		// newest point — that scan would undo the "clean restart pays nothing"
		// guarantee. The cursor (and the gauge) repopulate on the first
		// user-defined apply after restart.
		return nil
	}

	s.logger.Info("rebuilding derived time-series view after unclean shutdown",
		zap.Uint64("tsAppliedIndex", watermark), zap.Uint64("appliedIndex", applied))

	// The surviving on-disk partitions are not a clean index-prefix
	// (tstorage flushes mid-run, independent of our watermark), and tstorage
	// exposes no way to truncate a suffix — so discard everything and rebuild
	// rather than risk double-counting points that were mid-run-flushed.
	if err := s.TimeSeriesStorage.Close(); err != nil {
		s.logger.Warn("closing stale time-series before rebuild (continuing)", zap.Error(err))
	}
	if !inMemory {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("discard time-series dir: %w", err)
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("recreate time-series dir: %w", err)
		}
	}
	fresh, err := openTimeSeries(dir, inMemory)
	if err != nil {
		return err
	}
	s.TimeSeriesStorage = fresh
	s.tsLatestUnixNano.Store(0)

	cutoff := time.Now().Add(-timeSeriesRetention).UnixMilli()
	startIndex, err := s.timeSeriesRebuildStartIndex(cutoff)
	if err != nil {
		return err
	}
	if startIndex > 0 {
		if err := s.replayTimeSeries(startIndex, applied); err != nil {
			return err
		}
	}

	// Advance the watermark so a crash during/right after a rebuild doesn't
	// pointlessly redo it. The freshly rebuilt on-disk state is durable once
	// the next clean Close flushes it; persisting the watermark now matches
	// the in-memory state we just produced.
	return s.saveTSAppliedIndex(applied)
}

// timeSeriesRebuildStartIndex returns the lowest raft index whose user-defined
// points fall within the retention window (cutoff = now-retention, in unix
// milliseconds), so a rebuild reads only ~the retention window of the event
// log rather than its full (permanent) history. It scans backward from the
// tail until it finds a user-data proposal older than cutoff; the first
// in-window entry is the next one up. Returns 0 if the event log is empty.
//
// Backward linear scan (rather than binary search) keeps it robust to the
// event log's timestamp column not being strictly monotonic — interleaved
// metadata proposals carry no user timestamp, and proposer clock skew can
// reorder neighbours by milliseconds. The cost is ~window entries read, which
// is the bound we want.
func (s *Storage) timeSeriesRebuildStartIndex(cutoff int64) (uint64, error) {
	first, err := s.firstEventSeq()
	if err != nil {
		return 0, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return 0, err
	}
	if first == 0 || last == 0 || last < first {
		return 0, nil
	}

	startSeq := first
	for seq := last; seq >= first; seq-- {
		ts, ok, err := s.proposalMaxUserTimestamp(seq)
		if err != nil {
			return 0, err
		}
		if ok && ts < cutoff {
			startSeq = seq + 1
			break
		}
		if seq == first {
			// Reached the head of the log without crossing the cutoff: the
			// whole log is within the window. Avoids uint underflow too.
			break
		}
	}
	if startSeq > last {
		return 0, nil
	}

	bs, err := s.readEventAt(startSeq)
	if err != nil {
		return 0, fmt.Errorf("event log read seq %d: %w", startSeq, err)
	}
	ent := &pb.Entry{}
	if err := ent.Unmarshal(bs); err != nil {
		return 0, err
	}
	return ent.Index, nil
}

// proposalMaxUserTimestamp reads the event-log entry at wal seq and returns the
// largest timestamp among its user-defined (non-internal) entities. ok is
// false when the entry carries no user-defined entity with a real timestamp
// (pure metadata, an empty entry, or only pre-PR4 zero-stamped entities) — the
// caller treats those as "no signal" and keeps scanning.
func (s *Storage) proposalMaxUserTimestamp(seq uint64) (int64, bool, error) {
	bs, err := s.readEventAt(seq)
	if err != nil {
		return 0, false, fmt.Errorf("event log read seq %d: %w", seq, err)
	}
	ent := &pb.Entry{}
	if err := ent.Unmarshal(bs); err != nil {
		return 0, false, err
	}
	if ent.Type != pb.EntryNormal || ent.Data == nil {
		return 0, false, nil
	}
	p := &cluster.Proposal{}
	if err := p.Unmarshal(ent.Data, s); err != nil {
		return 0, false, err
	}
	var maxTS int64
	var ok bool
	for _, e := range p.Entities {
		if isInternalEntity(e.ID) || e.Timestamp <= 0 {
			continue
		}
		if e.Timestamp > maxTS {
			maxTS = e.Timestamp
		}
		ok = true
	}
	return maxTS, ok, nil
}

// replayTimeSeries re-derives the time-series points for every user-defined
// entity in [startIndex, through], reusing the Reader (which already streams
// committed proposals in raft-index order and skips syncable-metadata). Only
// genuinely user-defined entities are fed to handleUserDefined; config and
// other internal entities are durable in bbolt and must not touch tstorage.
func (s *Storage) replayTimeSeries(startIndex, through uint64) error {
	// Reader returns indices STRICTLY greater than raftIndex, so seed it one
	// below the first index we want to include.
	var from uint64
	if startIndex > 0 {
		from = startIndex - 1
	}
	r := &Reader{raftIndex: from, s: s}
	for {
		idx, p, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if idx > through {
			return nil
		}
		for _, e := range p.Entities {
			if isInternalEntity(e.ID) {
				continue
			}
			if err := s.handleUserDefined(e); err != nil {
				return fmt.Errorf("rebuild time-series at index %d: %w", idx, err)
			}
		}
	}
}

// isInternalEntity reports whether an entity id belongs to a built-in entity
// type (type/database/ingestable/syncable config, syncable bookkeeping,
// ingestable position) rather than user data. Mirrors applyEntity's dispatch
// so a rebuild classifies entities exactly as the live apply path does.
func isInternalEntity(id string) bool {
	for _, ie := range internalEntities {
		if ie.is(id) {
			return true
		}
	}
	return false
}

// TimeSeriesLatestUnixNano returns the largest user-defined timestamp (unix
// milliseconds, despite the historical name) written into the time-series
// view, or 0 if none. The metrics sampler turns it into
// committed_tstorage_lag_seconds = now - latest. Satisfies the optional
// reporter interface the raft loop type-asserts.
func (s *Storage) TimeSeriesLatestUnixNano() int64 {
	return s.tsLatestUnixNano.Load()
}

// saveTSAppliedIndex persists the time-series watermark to bbolt. Written only
// on a clean Close, after the tstorage flush, so its presence at == appliedIndex
// on the next Open is the proof of a clean shutdown.
func (s *Storage) saveTSAppliedIndex(idx uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(tsAppliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		return b.Put(tsAppliedIndexKey, buf[:])
	})
}

// loadTSAppliedIndex reads the time-series watermark, returning 0 when it has
// never been written (a fresh store, or a database created before this feature
// existed) — which correctly forces a rebuild on first Open after upgrade.
func (s *Storage) loadTSAppliedIndex() (uint64, error) {
	var idx uint64
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(tsAppliedIndexBucket)
		if b == nil {
			return nil
		}
		v := b.Get(tsAppliedIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("tsAppliedIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	})
	return idx, err
}
