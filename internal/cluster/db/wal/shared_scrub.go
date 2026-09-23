package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

var errScrubApplyPending = errors.New("scrub command is waiting for its apply watermark")

// runPendingSharedScrub owns generation assignment for application scrubs:
// generation is the authorized upper bound, never a rewrite attempt counter.
// A published bound is finished before preparing a newer pending command. This
// preserves recovery identity even when the pending record has been superseded.
// Like the native worker, it has one caller and excludes Close/replacement.
func (s *Storage) runPendingSharedScrub() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-s.scrubStop:
			cancel()
		case <-ctx.Done():
		}
	}()
	defer func() {
		stopping := ctx.Err() != nil
		cancel()
		<-stopped
		if stopping && err != nil {
			err = errors.Join(errScrubStopped, err)
		}
	}()
	if err := s.runOwedCompaction(); err != nil {
		return err
	}
	for {
		select {
		case <-s.scrubStop:
			return errScrubStopped
		default:
		}
		bound, hash, cmdIndex, err := s.loadPendingScrub()
		if err != nil {
			return err
		}
		if s.catchingUp.Load() {
			return nil
		}
		selected, err := s.eventLog.managed.Generation()
		if err != nil {
			return err
		}
		completed := s.lastScrubbedBound.Load()
		// A fetched generation may be completed without a local scrub request.
		// Selection above completion still requires pending authorization below.
		if selected < completed || selected > max(bound, completed) {
			return fmt.Errorf("scrub generation %d outside completed/pending bounds %d/%d: %w", selected, completed, bound, eventlog.ErrInvalid)
		}
		if selected > completed {
			// History survives metadata GC and pending-request replacement. Do
			// not interpret an arbitrary experimental rewrite as an applied scrub.
			// Applied progress is saved after the batch; history and a rewrite
			// may already be durable when that save is interrupted. Include
			// durable events ahead of apply so recovery can wait for replay.
			applied := s.AppliedIndex()
			history, err := s.loadScrubHistory(s.EventIndex())
			if err != nil {
				return err
			}
			authorized := false
			awaitingApply := false
			for _, row := range history {
				if row.bound == selected && row.cmdIndex > selected {
					if row.cmdIndex > applied {
						awaitingApply = true
						continue
					}
					authorized = true
					break
				}
			}
			if !authorized {
				if awaitingApply {
					return errScrubApplyPending
				}
				return fmt.Errorf("scrub generation %d has no applied authorization: %w", selected, eventlog.ErrInvalid)
			}
			if err := s.finishSharedScrub(ctx, selected); err != nil {
				return err
			}
			continue
		}
		if bound == 0 || bound <= completed {
			return nil
		}
		// The mirrored command beyond the bound must be durable and applied.
		if cmdIndex <= bound || s.AppliedIndex() > s.EventIndex() {
			return eventlog.ErrInvalid
		}
		// handleScrub signals before the enclosing apply finishes. Wait for
		// its watermark; no later traffic is required to wake the worker.
		if cmdIndex > s.AppliedIndex() {
			return errScrubApplyPending
		}
		// Avoid rescanning the log on every retry while a long-lived reader
		// or file listing blocks publication. rewriteSharedPlan checks again
		// under its mutation locks after preparation.
		s.fromZeroMu.Lock()
		pinned := s.fromZeroReads != 0
		s.fromZeroMu.Unlock()
		if pinned {
			return errEventRewriteDeferred
		}
		release, ok := s.eventLayout.move()
		if !ok {
			return ErrLayoutFrozen
		}
		release()
		plan, err := s.prepareScrubPlan(bound, hash, cmdIndex)
		if err != nil {
			return err
		}
		if _, err := s.rewriteSharedPlan(ctx, bound, plan); err != nil {
			return err
		}
	}
}

// finishSharedScrub derives cadence bookkeeping from the selected survivors,
// not by rerunning the erasure gate against already-scrubbed metadata. It needs
// no persisted subject keys, saved transform, or separate completion receipt.
func (s *Storage) finishSharedScrub(ctx context.Context, bound uint64) error {
	s.eventMu.Lock()
	first, last, err := s.eventBoundsLocked()
	if err == nil && last != s.EventIndex() {
		err = eventlog.ErrInvalid
	}
	if err == nil {
		s.firstEventIndex.Store(first)
		s.swappedBound.Store(bound)
	}
	s.eventMu.Unlock()
	if err != nil {
		return err
	}
	if _, err := s.reclaimSharedGeneration(ctx, bound); err != nil {
		return err
	}
	raws, err := s.sharedScrubDeleteSurvivors(ctx, bound)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.reconcileUnhashedDeletes(bound, 0, raws, nil); err != nil {
		return err
	}
	return s.markScrubComplete(bound)
}

// sharedScrubDeleteSurvivors inspects only the durable pending-delete IDs.
// Apply records each raw user delete before advancing its watermark, and a
// scrub can only remove or erase deletes. Thus this index is a superset of
// survivors, including after interrupted publication or reconciliation. New
// appends lie above bound and are left for a later scrub.
func (s *Storage) sharedScrubDeleteSurvivors(ctx context.Context, bound uint64) ([]rawDelete, error) {
	var candidates []uint64
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(unhashedDeleteBucket)
		if b == nil {
			return ErrBucketMissing
		}
		cursor := b.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(k) != 8 {
				return eventlog.ErrInvalid
			}
			index := binary.BigEndian.Uint64(k)
			if index > bound {
				break
			}
			candidates = append(candidates, index)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, ctx.Err()
	}
	// Release the metadata transaction before acquiring the publication lock.
	// One cursor keeps segment caching and decoding on the application boundary.
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	cursor := s.eventLog.entries.NewEntryCursor(candidates[0])
	defer func() { _ = cursor.Close() }()
	raws := make([]rawDelete, 0, len(candidates))
	for _, index := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := cursor.SeekGE(index); err != nil {
			return nil, err
		}
		entry, err := cursor.Current()
		if errors.Is(err, eventlog.ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if entry.GetIndex() != index {
			continue // Metadata GC removed this record.
		}
		if entry.GetType() != pb.EntryNormal || entry.Data == nil {
			continue
		}
		retained := false
		err = cluster.ForEachProposalEntity(entry.Data, func(typeID string, key, _ []byte, isDelete bool) error {
			if isUserDefinedType(typeID) && isDelete && !cluster.IsErasedKey(key) {
				retained = true
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if retained {
			raws = append(raws, rawDelete{index: index})
		}
	}
	return raws, nil
}
