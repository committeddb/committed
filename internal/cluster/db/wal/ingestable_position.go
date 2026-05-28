package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/philborlin/committed/internal/cluster"
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
func (s *Storage) saveIngestablePosition(e *cluster.Entity) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestablePositionBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put(e.Key, e.Data); err != nil {
			return fmt.Errorf("[wal.ingestable_position] put: %w", err)
		}
		return nil
	})
}
