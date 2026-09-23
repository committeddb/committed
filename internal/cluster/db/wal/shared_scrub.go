package wal

import (
	"context"
	"errors"
	"fmt"

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
func (s *Storage) runPendingSharedScrub() error {
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
		if selected < completed || selected > bound {
			return fmt.Errorf("scrub generation %d outside completed/pending bounds %d/%d: %w", selected, completed, bound, eventlog.ErrInvalid)
		}
		if selected > completed {
			// History survives metadata GC and pending-request replacement. Do
			// not interpret an arbitrary experimental rewrite as an applied scrub.
			history, err := s.loadScrubHistory(s.AppliedIndex())
			if err != nil {
				return err
			}
			authorized := false
			for _, row := range history {
				if row.bound == selected && row.cmdIndex > selected {
					authorized = true
					break
				}
			}
			if !authorized {
				return fmt.Errorf("scrub generation %d has no applied authorization: %w", selected, eventlog.ErrInvalid)
			}
			if err := s.finishSharedScrub(selected); err != nil {
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
		if _, err := s.rewriteSharedPlan(context.Background(), bound, plan); err != nil {
			return err
		}
	}
}

// finishSharedScrub derives cadence bookkeeping from the selected survivors,
// not by rerunning the erasure gate against already-scrubbed metadata. It needs
// no persisted subject keys, saved transform, or separate completion receipt.
func (s *Storage) finishSharedScrub(bound uint64) error {
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
	if _, err := s.reclaimSharedGeneration(context.Background(), bound); err != nil {
		return err
	}
	var raws []rawDelete
	err = s.scanEventEntries(bound, func(entry *pb.Entry) error {
		if entry.GetType() != pb.EntryNormal || entry.Data == nil {
			return nil
		}
		return cluster.ForEachProposalEntity(entry.Data, func(typeID string, key, _ []byte, isDelete bool) error {
			if isUserDefinedType(typeID) && isDelete && !cluster.IsErasedKey(key) {
				raws = append(raws, rawDelete{index: entry.GetIndex()})
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	if err := s.reconcileUnhashedDeletes(bound, 0, raws, nil); err != nil {
		return err
	}
	return s.markScrubComplete(bound)
}
