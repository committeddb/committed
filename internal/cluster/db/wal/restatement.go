package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/interpretation"
)

// The restatement registry's applied state: one record per restatement id, the value
// prefixed with the restatement's raft index (its interpretation coordinate) —
// what makes the later-in-log-wins fold deterministic on every node. The
// registry is APPEND-ONLY: records are never edited (admission refuses a
// re-POST with different content) and never swept — a deleted type's restatements
// stay, inert, because replay determinism at historical interpretation
// indexes needs them.

// handleRestatement applies one committed restatement: persist it and swap in a
// freshly compiled interpretation registry snapshot (restatements are rare — a full
// rebuild per apply is cheap, and readers stay lock-free on the atomic
// pointer).
func (s *Storage) handleRestatement(e *cluster.Entity, index uint64) error {
	if e.IsDelete() {
		// No deleter exists (the registry is append-only); a tombstone here
		// would be a bug upstream. Warn-and-skip keeps apply deterministic
		// and alive rather than fatal on an impossible record.
		s.logger.Warn("[wal.restatement] ignoring delete for append-only restatement record", zap.String("id", string(e.Key)))
		return nil
	}
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(restatementsBucket)
		if b == nil {
			return ErrBucketMissing
		}
		val := make([]byte, 8+len(e.Data))
		binary.BigEndian.PutUint64(val, index)
		copy(val[8:], e.Data)
		return b.Put(e.Key, val)
	})
	if err != nil {
		return err
	}
	return s.reloadInterpretationRegistry()
}

// reloadInterpretationRegistry compiles the applied restatements into a fresh
// snapshot and swaps it in. Called per restatement apply and once at Open.
func (s *Storage) reloadInterpretationRegistry() error {
	applied, err := s.AppliedRestatements()
	if err != nil {
		return fmt.Errorf("[wal.restatement] load applied restatements: %w", err)
	}
	reg, err := interpretation.NewRegistry(applied)
	if err != nil {
		// Admission validates predicates compile, so this is a consistency
		// violation, not an operator mistake — fail the apply loudly.
		return fmt.Errorf("[wal.restatement] compile registry: %w", err)
	}
	s.interpretationRegistry.Store(reg)
	return nil
}

// AppliedRestatements returns every applied restatement with its raft index, unordered
// (NewRegistry sorts).
func (s *Storage) AppliedRestatements() ([]cluster.AppliedRestatement, error) {
	var out []cluster.AppliedRestatement
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(restatementsBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(v) < 8 {
				return fmt.Errorf("[wal.restatement] corrupt record %q: %d bytes", k, len(v))
			}
			e := cluster.Restatement{ID: string(k)}
			if err := e.Unmarshal(v[8:]); err != nil {
				return fmt.Errorf("[wal.restatement] unmarshal %q: %w", k, err)
			}
			out = append(out, cluster.AppliedRestatement{Restatement: e, Index: binary.BigEndian.Uint64(v[:8])})
			return nil
		})
	})
	return out, err
}

// RestatementByID returns the applied restatement with the given id, its raft index,
// and whether it exists — the admission read behind the immutability rule.
func (s *Storage) RestatementByID(id string) (*cluster.Restatement, uint64, bool) {
	var raw []byte
	_ = s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(restatementsBucket); b != nil {
			if v := b.Get([]byte(id)); v != nil {
				raw = append([]byte{}, v...)
			}
		}
		return nil
	})
	if len(raw) < 8 {
		return nil, 0, false
	}
	e := &cluster.Restatement{ID: id}
	if err := e.Unmarshal(raw[8:]); err != nil {
		s.logger.Warn("[wal.restatement] unmarshal failed", zap.String("id", id), zap.Error(err))
		return nil, 0, false
	}
	return e, binary.BigEndian.Uint64(raw[:8]), true
}

// InterpretationRegistry returns the current compiled registry snapshot —
// lock-free, immutable, swapped whole on each restatement apply. Never nil.
func (s *Storage) InterpretationRegistry() *interpretation.Registry {
	if r := s.interpretationRegistry.Load(); r != nil {
		return r
	}
	return interpretation.EmptyRegistry
}
