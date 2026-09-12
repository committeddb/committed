package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// putTypeMigrationEditTx records the raft index of an in-place migration edit
// for typeID, inside the caller's (saveType apply) transaction — the
// interpretation coordinate the edit moves. Last writer wins: a later edit
// supersedes; staleness only ever compares against the LATEST edit.
func putTypeMigrationEditTx(tx *bolt.Tx, typeID string, raftIndex uint64) error {
	b := tx.Bucket(typeMigrationEditBucket)
	if b == nil {
		return ErrBucketMissing
	}
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, raftIndex)
	if err := b.Put([]byte(typeID), v); err != nil {
		return fmt.Errorf("[wal.type] record migration-edit index: %w", err)
	}
	return nil
}

// TypeMigrationEditedAt returns the raft index of typeID's latest in-place
// migration edit, or 0 when its migration was never edited in place (the
// common case — version-bump migrations declare a NEW reading with the bump
// and are pinned by the version itself, not this coordinate).
func (s *Storage) TypeMigrationEditedAt(typeID string) uint64 {
	var idx uint64
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeMigrationEditBucket)
		if b == nil {
			return nil
		}
		if v := b.Get([]byte(typeID)); len(v) == 8 {
			idx = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return idx
}
