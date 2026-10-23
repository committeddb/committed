package db

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// Cluster identity (see cluster.ClusterID). The authoritative copy is
// node-local (a file a snapshot install can't replace); the replicated copy
// in the metadata db lets a joiner check the id it adopted. How a node gets
// one:
//
//   - First boot of a new cluster: derived from the canonical COMMITTED_PEERS
//     and COMMITTED_CLUSTER_NAME, identically on every node; the first leader
//     then proposes it so the replicated copy and later joiners have it.
//   - Upgrade of a cluster that predates ids: once every member runs a
//     release that understands them (featureLevelClusterIdentity), the
//     leader mints one and proposes it; applying it gives every node its id.
//   - A joiner (COMMITTED_JOIN, empty state): adopts the id on the first peer
//     message addressed to it — it was started explicitly to join, and the
//     membership add already named it.
//   - Restore and recover write one (a new data directory).
//
// A node without an id behaves as before (it accepts peers that send none);
// once it has one it refuses peers that send none or another one.

// featureLevelClusterIdentity gates minting an id for an upgraded cluster:
// every member must understand the cluster-identity entity and the id
// headers first. (A new cluster derives its id at bootstrap; no gate.)
const featureLevelClusterIdentity uint64 = 4

// NodeIdentityStorage is the storage side of cluster identity. Storage that
// has none (the in-memory test doubles) runs without an id.
type NodeIdentityStorage interface {
	LocalClusterID() (cluster.ClusterID, bool, error)
	InstallLocalClusterID(cluster.ClusterID) error
	ReplicatedClusterID() (cluster.ClusterID, bool)
}

// ClusterIdentifier is what the peer transport asks of the node about
// identity. A TransportRaft that doesn't implement it runs without an id.
type ClusterIdentifier interface {
	// ClusterID returns this node's cluster id, if it has one.
	ClusterID() (cluster.ClusterID, bool)
	// AdoptClusterID gives a node with no id the one a peer's message
	// carried. Only a fresh joiner may adopt; any other node refuses.
	AdoptClusterID(cluster.ClusterID) error
}

// ErrNotAdoptable is returned when a node that isn't a fresh joiner is offered
// an id: it must get one from its own bootstrap, the replicated entity, or a
// restore — never from whoever messages it first.
var ErrNotAdoptable = errors.New("only a node joining with empty state adopts a cluster id from a peer")

// clusterIdentityRefresh bounds how often a node without a cached id re-reads
// storage (an upgrade mint or a joiner's replay installs it out of band).
const clusterIdentityRefresh = time.Second

// identity is the Raft's view of its cluster id.
type identity struct {
	store     NodeIdentityStorage // nil: storage without identity support
	id        atomic.Pointer[cluster.ClusterID]
	checkedAt atomic.Int64 // unix nanos of the last storage read while unset
	join      bool
	applied   func() uint64
	logger    *zap.Logger
	inc       incarnations
}

// initClusterIdentity sets up this node's id at startup. fresh is true when
// raft is bootstrapping a brand-new cluster from ps (StartNode).
func (n *Raft) initClusterIdentity(fresh bool, ps []raft.Peer) {
	n.ident.join = n.join
	n.ident.applied = n.storage.AppliedIndex
	n.ident.logger = n.logger
	store, ok := n.storage.(NodeIdentityStorage)
	if !ok {
		return
	}
	n.ident.store = store
	local, have, err := store.LocalClusterID()
	if err != nil {
		n.logger.Error("cluster identity", zap.Error(err))
		panic("db: " + err.Error())
	}
	if !have && fresh {
		peers := make(map[uint64]string, len(ps))
		for _, p := range ps {
			peers[p.ID] = string(p.Context)
		}
		local = cluster.DeriveClusterID(peers, n.clusterName)
		if err := store.InstallLocalClusterID(local); err != nil {
			n.logger.Error("cluster identity", zap.Error(err))
			panic("db: " + err.Error())
		}
		have = true
		n.logger.Info("new cluster id derived from COMMITTED_PEERS", zap.String("cluster", local.String()))
	}
	if !have {
		return
	}
	if err := n.checkReplicatedIdentity(local); err != nil {
		n.logger.Error("cluster identity", zap.Error(err))
		panic("db: " + err.Error())
	}
	n.ident.id.Store(&local)
}

// checkReplicatedIdentity refuses a node whose node-local id disagrees with the
// id its metadata db (its cluster, via the log or a snapshot) recorded.
func (n *Raft) checkReplicatedIdentity(local cluster.ClusterID) error {
	if n.ident.store == nil {
		return nil
	}
	if rep, ok := n.ident.store.ReplicatedClusterID(); ok && rep != local {
		return fmt.Errorf("this node's cluster id is %s but its metadata records cluster %s — it belongs to another cluster; restore or rebuild it rather than start it here", local, rep)
	}
	return nil
}

// ClusterID implements ClusterIdentifier.
func (i *identity) ClusterID() (cluster.ClusterID, bool) {
	if p := i.id.Load(); p != nil {
		return *p, true
	}
	if i.store == nil {
		return cluster.ClusterID{}, false
	}
	now := time.Now().UnixNano()
	last := i.checkedAt.Load()
	if now-last < int64(clusterIdentityRefresh) || !i.checkedAt.CompareAndSwap(last, now) {
		return cluster.ClusterID{}, false
	}
	local, have, err := i.store.LocalClusterID()
	if err != nil || !have {
		return cluster.ClusterID{}, false
	}
	i.id.Store(&local)
	return local, true
}

// AdoptClusterID implements ClusterIdentifier.
func (i *identity) AdoptClusterID(id cluster.ClusterID) error {
	if have, ok := i.ClusterID(); ok {
		if have != id {
			return fmt.Errorf("this node belongs to cluster %s, not %s", have, id)
		}
		return nil
	}
	if i.store == nil {
		return nil
	}
	if !i.join || i.applied() > 0 {
		return ErrNotAdoptable
	}
	if err := i.store.InstallLocalClusterID(id); err != nil {
		return err
	}
	i.id.Store(&id)
	i.logger.Info("joined cluster", zap.String("cluster", id.String()))
	return nil
}

// announceClusterIdentity makes sure the cluster's id is replicated: the
// leader proposes its own id when the metadata db has none (a new cluster's
// derived id), or mints one for an upgraded cluster once every member
// understands ids. Runs on every node; only the leader acts. Exits once the
// id is replicated.
func (db *DB) announceClusterIdentity() {
	store, ok := db.storage.(NodeIdentityStorage)
	if !ok {
		return
	}
	interval := db.announceInterval
	if interval <= 0 {
		interval = defaultTickInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, replicated := store.ReplicatedClusterID(); replicated {
			return
		}
		if db.raft.Leader() == db.raft.id {
			id, have := db.raft.ident.ClusterID()
			if !have && db.featureEnabled(featureLevelClusterIdentity) {
				minted, err := cluster.NewClusterID()
				if err != nil {
					db.logger.Error("minting a cluster id", zap.Error(err))
				} else {
					id, have = minted, true
				}
			}
			if have {
				if err := db.proposeClusterIdentity(id); err != nil && db.ctx.Err() == nil {
					db.logger.Warn("proposing the cluster id; will retry", zap.Error(err))
				}
			}
		}
		select {
		case <-ticker.C:
		case <-db.ctx.Done():
			return
		}
	}
}

func (db *DB) proposeClusterIdentity(id cluster.ClusterID) error {
	e, err := cluster.NewClusterIdentityEntity(id)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(db.ctx, 10*time.Second)
	defer cancel()
	return db.Propose(ctx, &cluster.Proposal{Entities: []*cluster.Entity{e}})
}

// adoptSnapshotIdentity runs after a snapshot install replaced the metadata db.
// A node with no id yet takes the cluster's from it; a node whose id differs
// from the snapshot's belongs to another cluster and must not run on.
func (n *Raft) adoptSnapshotIdentity() {
	if n.ident.store == nil {
		return
	}
	rep, ok := n.ident.store.ReplicatedClusterID()
	if !ok {
		return
	}
	local, have, err := n.ident.store.LocalClusterID()
	if err != nil {
		n.logger.Fatal("cluster identity after snapshot install", zap.Error(err))
	}
	if !have {
		if err := n.ident.store.InstallLocalClusterID(rep); err != nil {
			n.logger.Fatal("cluster identity after snapshot install", zap.Error(err))
		}
		n.ident.id.Store(&rep)
		return
	}
	if local != rep {
		n.logger.Fatal("snapshot from another cluster",
			zap.String("this node", local.String()), zap.String("snapshot", rep.String()))
	}
	if err := n.ident.checkOwnIncarnation(); err != nil {
		n.logger.Fatal("incarnation after snapshot install", zap.Error(err))
	}
}
