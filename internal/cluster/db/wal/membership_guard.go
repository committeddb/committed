package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// membershipGuardKey is the one key of membershipGuardBucket.
var membershipGuardKey = []byte("index")

// RecordRemovedMember records that the conf change at index removed id. The
// first removal wins, so a re-delivered entry, or a later removal of an id
// re-added before the guard existed, keeps the original index.
func (s *Storage) RecordRemovedMember(id, index uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(removedMemberBucket)
		if b == nil {
			return ErrBucketMissing
		}
		k := peerURLKey(id)
		if b.Get(k) != nil {
			return nil
		}
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, index)
		if err := b.Put(k, v); err != nil {
			return fmt.Errorf("[wal.membership_guard] record removed member: %w", err)
		}
		return nil
	})
}

// RemovedMembers returns every recorded removed id → the index that removed it.
func (s *Storage) RemovedMembers() map[uint64]uint64 {
	out := make(map[uint64]uint64)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(removedMemberBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) == 8 && len(v) == 8 {
				out[binary.BigEndian.Uint64(k)] = binary.BigEndian.Uint64(v)
			}
			return nil
		})
	})
	return out
}

// SetMembershipGuardIndex records the index of the marker that switched on the
// apply-time membership guard. The first value wins.
func (s *Storage) SetMembershipGuardIndex(index uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(membershipGuardBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if b.Get(membershipGuardKey) != nil {
			return nil
		}
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, index)
		if err := b.Put(membershipGuardKey, v); err != nil {
			return fmt.Errorf("[wal.membership_guard] set guard index: %w", err)
		}
		return nil
	})
}

// MembershipGuardIndex returns the index recorded by SetMembershipGuardIndex.
func (s *Storage) MembershipGuardIndex() (uint64, bool) {
	var index uint64
	var ok bool
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(membershipGuardBucket)
		if b == nil {
			return nil
		}
		if v := b.Get(membershipGuardKey); len(v) == 8 {
			index, ok = binary.BigEndian.Uint64(v), true
		}
		return nil
	})
	return index, ok
}
