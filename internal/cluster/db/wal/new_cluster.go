package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
	bolterrors "go.etcd.io/bbolt/errors"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/durablefs"
)

// A restored or recovered data directory becomes a member of a new cluster:
// restore copies another cluster's state onto new machines, and recover turns
// one survivor of a lost majority into a cluster of its own. Either way the
// directory must stop being the old cluster before it starts — its peers
// refuse the old id, and nothing in it may send the node to the old
// cluster's addresses:
//
//   - the cluster id is replaced, node-local and replicated;
//   - recorded peer and API addresses are dropped (COMMITTED_PEERS supplies
//     them at boot, and each node announces its own API address);
//   - the log's entries so far — which carry the old cluster's addresses in
//     their membership changes and announcements — never record an address
//     again (addressesThrough), and the log is compacted up to the applied
//     index behind a fresh snapshot, so a joiner gets the rewritten metadata
//     instead of replaying the original bootstrap;
//   - the node id it was prepared for is recorded, and the node refuses to
//     start under another;
//   - recorded member incarnations are dropped and so is this directory's
//     own, so it mints a new one at its first boot (restoring one archive
//     onto two machines under one node id leaves two incarnations, which
//     the cluster refuses).

// nodeIDFile records the node id restore or recover prepared the directory
// for.
const nodeIDFile = "node-id"

// addressesThroughKey, in clusterIdentityBucket: entries at or below this
// index carry a previous cluster's addresses.
var addressesThroughKey = []byte("addressesThrough")

// AddressesIgnoredThrough returns the index at or below which entries carry a
// previous cluster's addresses (0 for a directory that was never restored or
// recovered). Applying such an entry records no address.
func (s *Storage) AddressesIgnoredThrough() uint64 {
	var through uint64
	_ = s.view(func(tx *bolt.Tx) error {
		through = addressesIgnoredThrough(tx)
		return nil
	})
	return through
}

func addressesIgnoredThrough(tx *bolt.Tx) uint64 {
	if b := tx.Bucket(clusterIdentityBucket); b != nil {
		if v := b.Get(addressesThroughKey); len(v) == 8 {
			return binary.BigEndian.Uint64(v)
		}
	}
	return 0
}

// RecordedNodeID returns the node id restore or recover prepared this
// directory for, if either did.
func (s *Storage) RecordedNodeID() (uint64, bool, error) {
	raw, err := os.ReadFile(filepath.Join(s.rootDir, nodeIDFile)) //nolint:gosec // G304: the node's own data dir
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read %s: %w", nodeIDFile, err)
	}
	id, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || id == 0 {
		return 0, false, fmt.Errorf("%s is corrupt: %q", nodeIDFile, raw)
	}
	return id, true, nil
}

// Restored is what RestoreAs did.
type Restored struct {
	// Membership is the configuration the log holds through its last index.
	Membership *db.LogMembership
	// Converted reports the conversion of a tidwall event log (a backup taken
	// before 0.8.3); nil when the log was segmented already.
	Converted *Converted
}

// RestoreAs prepares the restored data directory dir, not yet started, to run
// as node nodeID of cluster id. nodeID must be a member of the configuration
// the directory's log holds through its last index. A tidwall event log is
// converted first.
func RestoreAs(dir string, id cluster.ClusterID, nodeID uint64) (*Restored, error) {
	converted, err := convertTidwallEventLog(dir)
	if err != nil {
		return nil, err
	}
	s, err := openStopped(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	m, err := db.ReadLogMembership(s)
	if err != nil {
		return nil, err
	}
	if !m.IsMember(nodeID) {
		return nil, fmt.Errorf("node %d is not a member of the backed-up cluster (members: %s); restore each machine as one of them", nodeID, MembersString(m.ConfState))
	}
	if err := s.becomeNewCluster(id, nodeID, m.LastIndex); err != nil {
		return nil, err
	}
	return &Restored{Membership: m, Converted: converted}, nil
}

// Recovered is what Recover did.
type Recovered struct {
	// Converted reports the conversion of a tidwall event log (a node last run
	// before 0.8.3); nil when the log was segmented already.
	Converted *Converted
	ClusterID cluster.ClusterID
	// LastIndex is the log's last index after the removals; everything up to
	// it is committed on this node.
	LastIndex uint64
	// Removed lists the members removed, voters and learners.
	Removed []uint64
}

// Recover turns the stopped node nodeID, at dir, into the only member of a
// new cluster: the way back from losing a majority. It must be a voter of the
// configuration its log holds through its last index. It leaves a joint
// configuration first, then removes every other member, one change per entry,
// commits the whole log (the uncommitted tail included), and mints a new
// cluster id. The old majority must never run again: it would elect its own
// leader and diverge. A tidwall event log is converted first.
func Recover(dir string, nodeID uint64) (*Recovered, error) {
	converted, err := convertTidwallEventLog(dir)
	if err != nil {
		return nil, err
	}
	s, err := openStopped(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = s.Close() }()
	m, err := db.ReadLogMembership(s)
	if err != nil {
		return nil, err
	}
	if !m.IsVoter(nodeID) {
		if m.IsMember(nodeID) {
			return nil, fmt.Errorf("node %d is a learner; recover from a voter (its log may lack committed entries)", nodeID)
		}
		return nil, fmt.Errorf("node %d is not a member of the configuration in this log (members: %s); check COMMITTED_NODE_ID or --node-id", nodeID, MembersString(m.ConfState))
	}

	hs, _, err := s.InitialState()
	if err != nil {
		return nil, err
	}
	term := hs.GetTerm()
	index := m.LastIndex
	var ents []*pb.Entry
	appendChange := func(cc *pb.ConfChangeV2) error {
		data, err := proto.Marshal(cc)
		if err != nil {
			return err
		}
		index++
		t, i, typ := term, index, pb.EntryConfChangeV2
		ents = append(ents, &pb.Entry{Term: &t, Index: &i, Type: &typ, Data: data})
		return nil
	}
	if m.Joint() {
		if err := appendChange(&pb.ConfChangeV2{}); err != nil {
			return nil, err
		}
	}
	// After leaving, the incoming voters and the learners (those named
	// next included) remain.
	cs := m.ConfState
	var removed []uint64
	for _, set := range [][]uint64{cs.GetVoters(), cs.GetLearners(), cs.GetLearnersNext()} {
		for _, id := range set {
			if id != nodeID && !slices.Contains(removed, id) {
				removed = append(removed, id)
			}
		}
	}
	slices.Sort(removed)
	for _, id := range removed {
		typ, nid := pb.ConfChangeRemoveNode, id
		if err := appendChange(&pb.ConfChangeV2{Changes: []*pb.ConfChangeSingle{{Type: &typ, NodeId: &nid}}}); err != nil {
			return nil, err
		}
	}
	// Commit through the last entry: there is no one left to agree with.
	commit := index
	next := &pb.HardState{Term: &term, Vote: proto.Uint64(hs.GetVote()), Commit: &commit}
	if err := s.Save(next, ents, &pb.Snapshot{}); err != nil {
		return nil, fmt.Errorf("append the removals: %w", err)
	}

	id, err := cluster.NewClusterID()
	if err != nil {
		return nil, err
	}
	if err := s.becomeNewCluster(id, nodeID, index); err != nil {
		return nil, err
	}
	return &Recovered{Converted: converted, ClusterID: id, LastIndex: index, Removed: removed}, nil
}

// openStopped opens a stopped node's directory for offline maintenance: the
// exclusive lock refuses a running node, safe mode holds the background
// workers, and persisted configs aren't built. Its event log must be
// segmented; callers convert a tidwall one first.
func openStopped(dir string) (*Storage, error) {
	opts := []Option{WithSafeMode(), WithLogger(zap.NewNop()), func(o *options) { o.offline = true }}
	s, err := Open(dir, nil, nil, nil, opts...)
	if err != nil {
		return nil, fmt.Errorf("open %s (is the node stopped?): %w", dir, err)
	}
	return s, nil
}

// becomeNewCluster rewrites the directory as node nodeID of cluster id, with
// entries through addressesThrough carrying the previous cluster's addresses.
// Every step is idempotent: rerunning after a crash finishes the job.
func (s *Storage) becomeNewCluster(id cluster.ClusterID, nodeID, addressesThrough uint64) error {
	if err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(clusterIdentityBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := b.Put([]byte(clusterIDKey), id[:]); err != nil {
			return err
		}
		through := make([]byte, 8)
		binary.BigEndian.PutUint64(through, addressesThrough)
		if err := b.Put(addressesThroughKey, through); err != nil {
			return err
		}
		for _, name := range [][]byte{memberPeerURLBucket, memberAPIURLBucket, memberIncarnationBucket} {
			if err := tx.DeleteBucket(name); err != nil && !errors.Is(err, bolterrors.ErrBucketNotFound) {
				return err
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("rewrite the metadata for cluster %s: %w", id, err)
	}

	d, err := durablefs.Open(s.rootDir)
	if err != nil {
		return err
	}
	for name, content := range map[string]string{clusterIDFile: id.String(), nodeIDFile: strconv.FormatUint(nodeID, 10)} {
		if _, err := d.Replace(name, func(w io.Writer) error {
			_, err := io.WriteString(w, content+"\n")
			return err
		}); err != nil {
			return fmt.Errorf("write %s: %w", name, err)
		}
	}
	// The previous machines' COMMITTED_PEERS mean nothing here, and the
	// archive's incarnation is the source directory's, not this one's.
	for _, name := range []string{peersAtLastBootFile, incarnationFile} {
		if _, err := d.Remove(name); err != nil {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}

	// Replace the saved snapshot, which holds the old metadata, and drop the
	// entries it covers.
	applied := s.AppliedIndex()
	if applied == 0 {
		return nil
	}
	if _, err := s.CreateSnapshot(applied, nil); err != nil {
		return fmt.Errorf("snapshot the rewritten metadata: %w", err)
	}
	if applied > s.boundary() {
		if err := s.Compact(applied); err != nil {
			return fmt.Errorf("compact the log through %d: %w", applied, err)
		}
		return nil
	}
	return s.persistPendingSnapshot()
}

// MembersString renders a configuration's members for an operator.
func MembersString(cs *pb.ConfState) string {
	parts := make([]string, 0, len(cs.GetVoters())+len(cs.GetLearners()))
	for _, id := range cs.GetVoters() {
		parts = append(parts, strconv.FormatUint(id, 10))
	}
	for _, id := range cs.GetLearners() {
		parts = append(parts, strconv.FormatUint(id, 10)+" (learner)")
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// SnapshotLacksMember reports whether a member of the applied configuration
// is missing from the held snapshot's — a member added since — so the node
// takes a new snapshot that member can install (see db's prepareCompaction).
// False when no snapshot is held: the log then starts at index 1.
func (s *Storage) SnapshotLacksMember() bool {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	if s.snapshot == nil || s.appliedConfState == nil {
		return false
	}
	held := map[uint64]bool{}
	cs := s.snapshot.GetMetadata().GetConfState()
	for _, set := range [][]uint64{cs.GetVoters(), cs.GetVotersOutgoing(), cs.GetLearners(), cs.GetLearnersNext()} {
		for _, id := range set {
			held[id] = true
		}
	}
	a := s.appliedConfState
	for _, set := range [][]uint64{a.GetVoters(), a.GetVotersOutgoing(), a.GetLearners(), a.GetLearnersNext()} {
		for _, id := range set {
			if !held[id] {
				return true
			}
		}
	}
	return false
}
