package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleNodeAPIURL persists a node's self-announced advertised API base URL.
// Called from the apply path. The entity carries the node id in e.Key (8
// big-endian bytes) and the marshaled cluster.NodeAPIURL in e.Data; we store
// e.Data verbatim so the read methods can unmarshal it without re-wrapping.
//
// Last-writer-wins per node id: a node whose advertised URL changed simply
// re-announces and overwrites. The bucket rides along in snapshots (bbolt is
// serialized whole into CreateSnapshot), so the mapping is durable across
// restart, log compaction, and InstallSnapshot to a lagging follower.
func (s *Storage) handleNodeAPIURL(e *cluster.Entity, _ uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberAPIURLBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put(e.Key, e.Data); err != nil {
			return fmt.Errorf("[wal.member_api_url] put: %w", err)
		}
		return nil
	})
}

// MemberAPIURL returns the advertised API base URL announced by node id, and
// whether one is known. Used by the leader-read proxy to resolve the leader's
// API address on a follower. A missing bucket or unknown id reports ("", false)
// rather than an error — an un-announced node (e.g. one with no
// COMMITTED_API_URL set) is a normal, non-fatal condition the proxy degrades on.
func (s *Storage) MemberAPIURL(id uint64) (string, bool) {
	var (
		url   string
		found bool
	)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberAPIURLBucket)
		if b == nil {
			return nil
		}
		raw := b.Get(cluster.NodeAPIURLKey(id))
		if raw == nil {
			return nil
		}
		n := &cluster.NodeAPIURL{}
		if err := n.Unmarshal(raw); err != nil {
			return nil
		}
		url = n.APIURL
		found = true
		return nil
	})
	return url, found
}

// MemberAPIURLs returns every known node id → advertised API base URL. Used to
// fill the api_url field of GET /v1/membership. A missing bucket yields an
// empty map.
func (s *Storage) MemberAPIURLs() map[uint64]string {
	out := make(map[uint64]string)
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberAPIURLBucket)
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			n := &cluster.NodeAPIURL{}
			if err := n.Unmarshal(v); err != nil {
				return nil
			}
			out[n.NodeID] = n.APIURL
			return nil
		})
	})
	return out
}

// DeleteMemberAPIURL drops the announced URL for node id. Called from the
// membership-remove apply path (raft.applyConfChange) so a removed node's
// entry doesn't accumulate across the frequent add/remove churn of an
// operator rebalancing the cluster. Idempotent: deleting an absent key is a
// no-op.
func (s *Storage) DeleteMemberAPIURL(id uint64) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberAPIURLBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Delete(cluster.NodeAPIURLKey(id)); err != nil {
			return fmt.Errorf("[wal.member_api_url] delete: %w", err)
		}
		return nil
	})
}
