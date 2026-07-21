package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// Position returns the most recently applied checkpoint position for
// the given ingestable ID. Returns nil when the ingestable has never
// checkpointed (or when the bucket is missing — treated as nil rather
// than an error so a brand-new ingestable boots cleanly without
// special-casing the supervisor).
//
// The dialect decodes nil as "no prior position" and runs a full
// snapshot. Any non-nil value is opaque dialect-specific bytes (for
// the Postgres dialect: pgoutput LSN + optional in-progress snapshot
// progress, encoded by encodePosition in postgres.go).
func (s *Storage) Position(id string) cluster.Position {
	var pos cluster.Position
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestablePositionBucket)
		if b == nil {
			return nil
		}
		raw := b.Get([]byte(id))
		if raw == nil {
			return nil
		}
		ip := &cluster.IngestablePosition{}
		if err := ip.Unmarshal(raw); err != nil {
			return nil
		}
		// Copy so the returned slice isn't backed by bbolt's mmap'd
		// memory, which is only valid for the duration of the
		// transaction.
		pos = append(cluster.Position(nil), ip.Position...)
		return nil
	})
	return pos
}

// saveIngestablePosition persists an IngestablePosition checkpoint.
// Called from the apply path. The entity carries the ingestable ID in
// e.Key and the marshaled IngestablePosition in e.Data; we store
// e.Data verbatim so Position() can unmarshal it without re-wrapping.
//
// Last-writer-wins: every new checkpoint overwrites the previous one
// for this ingestable ID. There is no version history — only the
// most-recent position is needed for resume.
func (s *Storage) saveIngestablePosition(e *cluster.Entity, _ uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		if e.IsDelete() {
			// A delete tombstone clears the checkpoint (an ingestable DELETE), so a
			// same-id recreate starts from a full snapshot instead of resuming from
			// a stale LSN whose replication slot has been dropped.
			b := tx.Bucket(ingestablePositionBucket)
			if b == nil {
				return ErrBucketMissing
			}
			if err := b.Delete(e.Key); err != nil {
				return fmt.Errorf("[wal.ingestable_position] delete: %w", err)
			}
			return nil
		}
		return putIngestablePositionGuarded(tx, e.Key, e.Data)
	})
}

// putIngestablePositionGuarded persists a checkpoint (data is the marshaled
// LogIngestablePosition, keyed by the ingestable id bytes) but ONLY while the
// ingestable config still exists — otherwise it reaps any lingering value
// rather than re-establishing an orphan.
//
// Invariant: a position persists only while its ingestable config exists. A
// worker submits its position via an async consensus bump that can commit AFTER
// the config was deleted; without this guard the Put would re-establish an
// orphaned position (config gone, LSN lingering) that a same-id recreate
// resumes from — resuming CDC from an LSN whose replication slot Teardown has
// since dropped (error) or a silent data gap. Deterministic: config existence
// is replicated state applied in identical log order on every node. Shared by
// the entity-apply path (saveIngestablePosition) and the atomic bundled-position
// path (applyBundledIngestablePosition).
func putIngestablePositionGuarded(tx *bolt.Tx, key, data []byte) error {
	b := tx.Bucket(ingestablePositionBucket)
	if b == nil {
		return ErrBucketMissing
	}
	if !configExists(tx, ingestableBucket, key) {
		if err := b.Delete(key); err != nil {
			return fmt.Errorf("[wal.ingestable_position] reap orphan: %w", err)
		}
		return nil
	}
	if err := b.Put(key, data); err != nil {
		return fmt.Errorf("[wal.ingestable_position] put: %w", err)
	}
	return nil
}

// applyBundledIngestablePosition persists the resume checkpoint a proposal
// carried inline (Proposal.Position), keyed by its IngestableID, through the
// SAME guarded put a standalone position entity uses. Called from ApplyCommitted
// so the checkpoint advances in the same raft entry as the proposal's rows —
// closing the crash window between a committed snapshot batch and a separate
// position proposal (the window SourceSeq dedup can't cover, since snapshot rows
// carry SourceSeq 0). pos is the raw dialect position bytes; we wrap it in a
// LogIngestablePosition exactly as proposeIngestablePosition would, so Position()
// unwraps it identically.
func (s *Storage) applyBundledIngestablePosition(id string, pos cluster.Position) error {
	data, err := (&cluster.IngestablePosition{ID: id, Position: pos}).Marshal()
	if err != nil {
		return fmt.Errorf("[wal.ingestable_position] marshal bundled: %w", err)
	}
	return s.update(func(tx *bolt.Tx) error {
		return putIngestablePositionGuarded(tx, []byte(id), data)
	})
}
