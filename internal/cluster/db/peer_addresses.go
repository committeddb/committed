package db

import (
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// PeerBootStorage is the node-local record of the COMMITTED_PEERS a node
// booted with last time. Storage that has none (the in-memory test doubles)
// boots as if every entry were unchanged.
type PeerBootStorage interface {
	PeersAtLastBoot() (map[uint64]string, bool, error)
	SetPeersAtLastBoot(map[uint64]string) error
}

// choosePeerAddresses picks the one address each peer starts with and returns
// the transport seeds: this node's own entry (its listen URL) plus the chosen
// address for every other peer known to COMMITTED_PEERS or the recorded table.
//
// The order of authority at boot:
//
//  1. A COMMITTED_PEERS entry whose value changed since this node's last boot:
//     the operator edited it (a restore onto new machines, every node moving,
//     a switch to https), so it wins — and is written into the recorded table
//     so the next boot keeps it.
//  2. Otherwise the recorded address (what the cluster agreed on, including
//     members added through the membership API).
//  3. Otherwise COMMITTED_PEERS (a member with no record).
//
// The first boot of a release with this rule on an existing data directory has
// no last-boot record; it keeps the previous behavior (recorded wins) and
// records the env for next time. An unedited, stale COMMITTED_PEERS entry
// therefore never overrides an address the cluster has since agreed on.
func (n *Raft) choosePeerAddresses(ps []raft.Peer) []raft.Peer {
	env := make(map[uint64]string, len(ps))
	var self *raft.Peer
	for i := range ps {
		env[ps[i].ID] = string(ps[i].Context)
		if ps[i].ID == n.id {
			self = &ps[i]
		}
	}
	recorded := n.storage.MemberPeerURLs()
	var lastBoot map[uint64]string
	haveLastBoot := false
	bootStore, _ := n.storage.(PeerBootStorage)
	if bootStore != nil {
		var err error
		if lastBoot, haveLastBoot, err = bootStore.PeersAtLastBoot(); err != nil {
			n.logger.Error("reading the peers recorded at the last boot; treating every COMMITTED_PEERS entry as unchanged", zap.Error(err))
			haveLastBoot = false
		}
	}

	chosen := make(map[uint64]string)
	for id, rec := range recorded {
		if id == n.id {
			continue
		}
		if c, err := cluster.CanonicalPeerURL(rec); err == nil {
			rec = c
		}
		chosen[id] = rec
	}
	for id, u := range env {
		if id == n.id {
			continue
		}
		rec, isRecorded := chosen[id]
		edited := haveLastBoot && lastBoot[id] != u
		switch {
		case !isRecorded:
			chosen[id] = u
		case edited && rec != u:
			n.logger.Info("peer address from COMMITTED_PEERS replaces the recorded one (the entry changed since the last boot)",
				zap.Uint64("peer", id), zap.String("recorded", rec), zap.String("address", u))
			chosen[id] = u
			if err := n.storage.PutMemberPeerURL(id, []byte(u)); err != nil {
				n.logger.Error("recording the changed peer address", zap.Uint64("peer", id), zap.Error(err))
			}
		case rec != u:
			n.logger.Info("using the recorded peer address; COMMITTED_PEERS differs but has not changed since the last boot",
				zap.Uint64("peer", id), zap.String("recorded", rec), zap.String("COMMITTED_PEERS", u))
		}
	}
	if bootStore != nil {
		if err := bootStore.SetPeersAtLastBoot(env); err != nil {
			n.logger.Error("recording this boot's COMMITTED_PEERS", zap.Error(err))
		}
	}

	seeds := make([]raft.Peer, 0, len(chosen)+1)
	if self != nil {
		seeds = append(seeds, *self)
	}
	for id, u := range chosen {
		seeds = append(seeds, raft.Peer{ID: id, Context: []byte(u)})
	}
	return seeds
}
