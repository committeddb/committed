package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/durablefs"
)

// incarnationFile holds this data directory's incarnation (see
// cluster.Incarnation), node-local like the cluster id. Restore and recover
// remove it, so the directory they prepare mints its own.
const incarnationFile = "incarnation"

// memberIncarnationBucket maps a node id (cluster.NodeIncarnationKey) to the
// first incarnation recorded for it (16 bytes). Entity-driven
// (handleNodeIncarnation), so it replicates and rides in snapshots.
var memberIncarnationBucket = []byte("memberIncarnations")

// LocalIncarnation returns this data directory's incarnation, minting and
// durably writing one the first time the directory boots without one.
func (s *Storage) LocalIncarnation() (cluster.Incarnation, error) {
	inc, have, err := s.readLocalIncarnation()
	if err != nil || have {
		return inc, err
	}
	inc, err = cluster.NewIncarnation()
	if err != nil {
		return inc, err
	}
	d, err := durablefs.Open(s.rootDir)
	if err != nil {
		return inc, err
	}
	if _, err := d.Install(incarnationFile, func(w io.Writer) error {
		_, err := io.WriteString(w, inc.String()+"\n")
		return err
	}); err != nil {
		return inc, fmt.Errorf("write %s: %w", incarnationFile, err)
	}
	return inc, nil
}

func (s *Storage) readLocalIncarnation() (cluster.Incarnation, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.rootDir, incarnationFile)) //nolint:gosec // G304: the node's own data dir
	if errors.Is(err, os.ErrNotExist) {
		return cluster.Incarnation{}, false, nil
	}
	if err != nil {
		return cluster.Incarnation{}, false, fmt.Errorf("read %s: %w", incarnationFile, err)
	}
	inc, err := cluster.ParseIncarnation(string(raw))
	if err != nil {
		return cluster.Incarnation{}, false, fmt.Errorf("%s is corrupt: %w", incarnationFile, err)
	}
	return inc, true, nil
}

// MemberIncarnation returns the incarnation the cluster recorded for node id.
func (s *Storage) MemberIncarnation(id uint64) (cluster.Incarnation, bool) {
	var inc cluster.Incarnation
	var ok bool
	_ = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(memberIncarnationBucket)
		if b == nil {
			return nil
		}
		if v := b.Get(cluster.NodeIncarnationKey(id)); len(v) == len(inc) {
			copy(inc[:], v)
			ok = !inc.IsZero()
		}
		return nil
	})
	return inc, ok
}

// handleNodeIncarnation records a node's incarnation, first value wins: a
// later, different incarnation for the same node id is another process
// running as that node, and is never recorded. Applying such a proposal on the
// node that made it — its incarnation is this directory's — stops this node:
// another data directory already holds its id.
func (s *Storage) handleNodeIncarnation(e *cluster.Entity, index uint64) error {
	if e.Variant() == cluster.EntityVariantDelete {
		return nil // a recorded incarnation is never deleted
	}
	nodeID, inc, err := cluster.NodeIncarnationFromEntity(e)
	if err != nil {
		return err
	}
	var recorded []byte
	if err := s.update(func(tx *bolt.Tx) error {
		if index <= addressesIgnoredThrough(tx) {
			return nil // a previous cluster's announcement (see new_cluster.go)
		}
		b := tx.Bucket(memberIncarnationBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if v := b.Get(e.Key); v != nil {
			recorded = bytes.Clone(v)
			return nil
		}
		return b.Put(e.Key, e.Data)
	}); err != nil {
		return fmt.Errorf("[wal.node_incarnation] put: %w", err)
	}
	if recorded == nil || bytes.Equal(recorded, e.Data) {
		return nil
	}
	var first cluster.Incarnation
	copy(first[:], recorded)
	local, have, err := s.readLocalIncarnation()
	if err != nil {
		return err
	}
	if have && local == inc {
		return fmt.Errorf("another process is running as node %d: the cluster recorded incarnation %s for it, and this data directory is incarnation %s. Stop this process and give it its own node id", nodeID, first, inc)
	}
	s.logger.Warn("refused a second incarnation for a node: two processes are running as one node id",
		zap.Uint64("node", nodeID), zap.String("recorded", first.String()), zap.String("refused", inc.String()))
	return nil
}
