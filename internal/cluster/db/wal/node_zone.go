package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleNodeZone persists a node's self-announced zone. Called from the apply
// path; mirrors handleNodeVersion (last-writer-wins per node id, snapshot-
// durable via the bbolt ride-along). An empty zone is stored as-is: it means
// "explicitly unpinned", overwriting a stale identity after an operator
// clears COMMITTED_ZONE.
func (s *Storage) handleNodeZone(e *cluster.Entity, _ uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberZoneBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put(e.Key, e.Data); err != nil {
			return fmt.Errorf("[wal.node_zone] put: %w", err)
		}
		return nil
	})
}

// MemberZone returns the zone announced by node id, and whether one is known.
// ("", false) for an un-announced node — a binary predating zones, or one
// with COMMITTED_ZONE unset that never announced; both mean "no placement
// identity".
func (s *Storage) MemberZone(id uint64) (string, bool) {
	var (
		zone  string
		found bool
	)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberZoneBucket)
		if b == nil {
			return nil
		}
		raw := b.Get(cluster.NodeVersionKey(id))
		if raw == nil {
			return nil
		}
		n := &cluster.NodeZone{}
		if err := n.Unmarshal(raw); err != nil {
			return nil
		}
		zone = n.Zone
		found = true
		return nil
	})
	return zone, found
}

// MemberZones returns every known node id → announced zone (empty zones
// included — they mean explicitly unpinned). Ownership resolution intersects
// this with CURRENT membership, so a removed node's lingering entry is
// harmless; DeleteMemberZone is hygiene, matching DeleteMemberVersion.
func (s *Storage) MemberZones() map[uint64]string {
	out := make(map[uint64]string)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberZoneBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			n := &cluster.NodeZone{}
			if err := n.Unmarshal(v); err != nil {
				return nil
			}
			out[n.NodeID] = n.Zone
			return nil
		})
	})
	return out
}

// DeleteMemberZone drops the announced zone for node id. Called from the
// membership-remove apply path so a removed node's entry doesn't accumulate.
func (s *Storage) DeleteMemberZone(id uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberZoneBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Delete(cluster.NodeVersionKey(id)); err != nil {
			return fmt.Errorf("[wal.node_zone] delete: %w", err)
		}
		return nil
	})
}
