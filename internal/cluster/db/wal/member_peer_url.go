package wal

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// peerURLKey encodes a raft node id as the 8 big-endian bytes used to key the
// memberPeerURLBucket (the same encoding memberAPIURLBucket uses).
func peerURLKey(id uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, id)
	return k
}

// PutMemberPeerURL durably records the raft peer URL for node id. Called from
// raft.applyConfChange when an add-member/add-learner conf change commits: raft's
// ConfState replicates the member id but never its address, and the URL rides
// only transiently on the ConfChange Context, so persisting it here is what lets
// a restarted node (or one restored from a snapshot) reconcile its transport
// against the real membership instead of the stale static COMMITTED_PEERS set.
//
// Last-writer-wins per id; the value is the raw URL bytes. The bucket rides along
// in snapshots (bbolt is serialized whole into CreateSnapshot), so the mapping is
// durable across restart, log compaction, and InstallSnapshot to a lagging
// follower — the same guarantee memberAPIURLBucket gives.
func (s *Storage) PutMemberPeerURL(id uint64, rawURL []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberPeerURLBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put(peerURLKey(id), rawURL); err != nil {
			return fmt.Errorf("[wal.member_peer_url] put: %w", err)
		}
		return nil
	})
}

// MemberPeerURLs returns every durably-known node id → raft peer URL. raft.reconcileTransport
// reads it at startup and after a snapshot install to (re-)AddPeer every current
// member the transport doesn't already know. A missing bucket yields an empty map.
func (s *Storage) MemberPeerURLs() map[uint64]string {
	out := make(map[uint64]string)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberPeerURLBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				return nil
			}
			out[binary.BigEndian.Uint64(k)] = string(v)
			return nil
		})
	})
	return out
}

// DeleteMemberPeerURL drops the peer URL for node id. Called from the
// membership-remove apply path (raft.applyConfChange) so a removed node's entry
// doesn't accumulate across the add/remove churn of rebalancing, and so a
// restarted node's transport reconcile doesn't re-add a member that is gone.
// Idempotent: deleting an absent key is a no-op.
func (s *Storage) DeleteMemberPeerURL(id uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberPeerURLBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Delete(peerURLKey(id)); err != nil {
			return fmt.Errorf("[wal.member_peer_url] delete: %w", err)
		}
		return nil
	})
}
