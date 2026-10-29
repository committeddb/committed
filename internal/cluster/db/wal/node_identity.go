package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/durablefs"
)

// Node-local identity: files at the top of the data directory, outside the
// metadata db. A snapshot install replaces the metadata db wholesale, so
// anything that must not change when a leader's snapshot lands lives here.
// Backups carry these files; restore rewrites them.
const (
	// clusterIDFile is the authoritative cluster id, written once.
	clusterIDFile = "cluster-id"
	// peersAtLastBootFile is the COMMITTED_PEERS this node booted with last
	// time, so a boot can tell an entry the operator edited (which wins over
	// the recorded address) from an unedited one (which doesn't).
	peersAtLastBootFile = "peers-at-last-boot.json"
	clusterIDKey        = "id"
)

// LocalClusterID returns this node's authoritative cluster id, if it has one.
func (s *Storage) LocalClusterID() (cluster.ClusterID, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.rootDir, clusterIDFile)) //nolint:gosec // G304: the node's own data dir
	if errors.Is(err, os.ErrNotExist) {
		return cluster.ClusterID{}, false, nil
	}
	if err != nil {
		return cluster.ClusterID{}, false, fmt.Errorf("read %s: %w", clusterIDFile, err)
	}
	id, err := cluster.ParseClusterID(string(raw))
	if err != nil {
		return cluster.ClusterID{}, false, fmt.Errorf("%s is corrupt: %w", clusterIDFile, err)
	}
	return id, true, nil
}

// InstallLocalClusterID durably writes this node's cluster id when it has
// none. Installing the id it already has is a no-op; installing a different
// one is refused — a node's cluster id never changes in place (restore and
// recover write a new data directory).
func (s *Storage) InstallLocalClusterID(id cluster.ClusterID) error {
	if id.IsZero() {
		return fmt.Errorf("refusing to install a zero cluster id")
	}
	have, ok, err := s.LocalClusterID()
	if err != nil {
		return err
	}
	if ok {
		if have == id {
			return nil
		}
		return fmt.Errorf("this node belongs to cluster %s, not %s", have, id)
	}
	d, err := durablefs.Open(s.rootDir)
	if err != nil {
		return err
	}
	if _, err := d.Install(clusterIDFile, func(w io.Writer) error {
		_, err := io.WriteString(w, id.String()+"\n")
		return err
	}); err != nil {
		return fmt.Errorf("write %s: %w", clusterIDFile, err)
	}
	return nil
}

// PeersAtLastBoot returns the COMMITTED_PEERS (id → canonical URL) recorded at
// the previous boot; ok is false before the first boot that recorded one (a
// fresh node, or the first boot after upgrading from a release without it).
func (s *Storage) PeersAtLastBoot() (map[uint64]string, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.rootDir, peersAtLastBootFile)) //nolint:gosec // G304: the node's own data dir
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read %s: %w", peersAtLastBootFile, err)
	}
	var byKey map[string]string
	if err := json.Unmarshal(raw, &byKey); err != nil {
		return nil, false, fmt.Errorf("%s is corrupt: %w", peersAtLastBootFile, err)
	}
	out := make(map[uint64]string, len(byKey))
	for k, v := range byKey {
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, false, fmt.Errorf("%s has a bad node id %q", peersAtLastBootFile, k)
		}
		out[id] = v
	}
	return out, true, nil
}

// SetPeersAtLastBoot durably records the COMMITTED_PEERS this boot used.
func (s *Storage) SetPeersAtLastBoot(peers map[uint64]string) error {
	byKey := make(map[string]string, len(peers))
	for id, u := range peers {
		byKey[strconv.FormatUint(id, 10)] = u
	}
	raw, err := json.Marshal(byKey)
	if err != nil {
		return err
	}
	d, err := durablefs.Open(s.rootDir)
	if err != nil {
		return err
	}
	if _, err := d.Replace(peersAtLastBootFile, func(w io.Writer) error {
		_, err := w.Write(raw)
		return err
	}); err != nil {
		return fmt.Errorf("write %s: %w", peersAtLastBootFile, err)
	}
	return nil
}

// ReplicatedClusterID returns the cluster id recorded in the metadata db, the
// copy snapshots carry to other members.
func (s *Storage) ReplicatedClusterID() (cluster.ClusterID, bool) {
	var id cluster.ClusterID
	var ok bool
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(clusterIdentityBucket)
		if b == nil {
			return nil
		}
		if v := b.Get([]byte(clusterIDKey)); len(v) == len(id) {
			copy(id[:], v)
			ok = !id.IsZero()
		}
		return nil
	})
	return id, ok
}

// PutReplicatedClusterID records the cluster id in the metadata db, first
// value wins: a different id arriving later is refused, so a stale proposal
// or a snapshot from another cluster can never replace it.
func (s *Storage) PutReplicatedClusterID(id cluster.ClusterID) error {
	if id.IsZero() {
		return fmt.Errorf("refusing to record a zero cluster id")
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(clusterIdentityBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if v := b.Get([]byte(clusterIDKey)); len(v) > 0 {
			var have cluster.ClusterID
			copy(have[:], v)
			if have == id {
				return nil
			}
			return fmt.Errorf("the metadata db already records cluster %s, not %s", have, id)
		}
		return b.Put([]byte(clusterIDKey), id[:])
	})
}

// handleClusterIdentity applies a replicated cluster id. The metadata copy is
// first value wins: a different id arriving later (a stale proposal from an
// earlier leader) is logged and ignored. A node with no node-local id takes
// this one (a joiner replaying the log, or every member of an upgraded
// cluster). A node whose node-local id differs from the one its cluster
// recorded is in the wrong cluster: apply stops loudly rather than run on.
func (s *Storage) handleClusterIdentity(e *cluster.Entity, _ uint64) error {
	if e.Variant() == cluster.EntityVariantDelete {
		return nil // the id is never deleted
	}
	id, err := cluster.ClusterIdentityFromEntity(e)
	if err != nil {
		return err
	}
	if err := s.PutReplicatedClusterID(id); err != nil {
		s.logger.Error("ignoring a cluster id proposal that disagrees with the recorded one",
			zap.String("proposed", id.String()), zap.Error(err))
	}
	recorded, ok := s.ReplicatedClusterID()
	if !ok {
		return nil
	}
	local, have, err := s.LocalClusterID()
	if err != nil {
		return err
	}
	if !have {
		return s.InstallLocalClusterID(recorded)
	}
	if local != recorded {
		return fmt.Errorf("this node's cluster id is %s but its cluster recorded %s — it belongs to another cluster; stop it and restore or rebuild it", local, recorded)
	}
	return nil
}
