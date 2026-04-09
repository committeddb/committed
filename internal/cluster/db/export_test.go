package db

import "go.etcd.io/etcd/raft/v3"

// PartitionPeerForTest drops the named peer from this Raft's transport so
// outbound messages to it become no-ops. Combined with calling the same
// method on the OTHER side of the partition, this simulates a transport-
// level network partition without touching the etcd raft state machine.
//
// This is exposed as an exported method on a *_test.go file so it lives
// in the production package (giving it access to n.transport) but does
// not ship in non-test builds. Tests in package db_test reach it via
// db.(*Raft).PartitionPeerForTest.
//
// Used by TestPreVote_PartitionedFollowerDoesNotDisruptLeader to verify
// that a partitioned-then-rejoining follower does not disrupt a healthy
// leader when PreVote is enabled.
func (n *Raft) PartitionPeerForTest(id uint64) {
	n.transport.RemovePeer(id)
}

// UnpartitionPeerForTest re-adds a previously-removed peer to this Raft's
// transport, healing the partition. The peer struct must carry the same
// Context (URL) the cluster was started with — the test passes the
// original raft.Peer back through.
func (n *Raft) UnpartitionPeerForTest(p raft.Peer) error {
	return n.transport.AddPeer(p)
}
