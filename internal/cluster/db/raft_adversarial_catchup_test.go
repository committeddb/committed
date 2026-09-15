//go:build adversarial

package db_test

import (
	"fmt"
	"testing"
	"time"

	"go.etcd.io/raft/v3"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// The catch-up scenarios beyond scenario (e) (raft_adversarial_test.go): a
// brand-new node joining an established cluster from an empty data
// directory, and a member that missed a right-to-be-forgotten scrub while
// it was away. Both are the operator flows docs/operations/membership.md
// and rebuild.md now promise happen by themselves.

// catchUpNodeOpts: compaction aggressive enough that the raft log is past
// index 1 after a short burst, so a joining node can only be served a
// snapshot — the production shape of "joining an established cluster".
func catchUpNodeOpts() []db.Option {
	return []db.Option{db.WithCompactMaxSize(512), db.WithCompactMaxAge(0)}
}

// proposeBurst proposes n distinct entries through the leader among rs and
// waits for every node in rs to apply the last one.
func proposeBurst(t *testing.T, rs Rafts, seq *uint64, n int) {
	t.Helper()
	var last []byte
	for i := 0; i < n; i++ {
		*seq++
		last = severeLagProposal(t, *seq)
		proposeAndCheckBytes(t, rs, last)
	}
	for _, r := range rs {
		waitForUserEntry(t, r, last)
	}
}

func requireCompactedPast(t *testing.T, rs Rafts, index uint64) {
	t.Helper()
	var compacted uint64
	for _, r := range rs {
		if c := r.raft.LastCompactedIndexForTest(); c > compacted {
			compacted = c
		}
	}
	if compacted <= index {
		t.Fatalf("raft-log compaction reached %d, not past %d — plain replication could still serve the joiner", compacted, index)
	}
}

func requireNoFatal(t *testing.T, fatalC <-chan fatalEvent) {
	t.Helper()
	select {
	case ev := <-fatalC:
		t.Fatalf("node %d fatal-exited: %q", ev.nodeID, ev.message)
	default:
	}
}

// A node added as a learner over an EMPTY data directory, to a cluster whose
// raft log has long compacted past index 1, catches up by itself: the
// leader's snapshot triggers a fetch of the whole event log from a peer,
// the snapshot installs, replication takes over, and the node can be
// promoted and participate — with byte-identical events. This is the
// "add a node" flow with no rsync step.
func TestAdversarial_EmptyLearnerCatchesUp(t *testing.T) {
	ports := pickFreePorts(4)
	all := make([]raft.Peer, 4)
	for i := range all {
		all[i] = raft.Peer{ID: uint64(i + 1), Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", ports[i]))}
	}
	fc := NewFaultyCluster(all)
	fatalC := make(chan fatalEvent, 16)
	opts := catchUpNodeOpts()

	dirs := make([]string, 4)
	rafts := make(Rafts, 0, 4)
	for i := 0; i < 3; i++ {
		dirs[i] = t.TempDir()
		rafts = append(rafts, openWalRaft(t, all[i].ID, all[:3], dirs[i], fc, opts, fatalC))
	}
	defer func() {
		for _, r := range rafts {
			_ = r.Close()
		}
		for _, r := range rafts {
			r.mu.RLock()
			s := r.storage
			r.mu.RUnlock()
			_ = s.Close()
		}
	}()
	rafts.WaitForLeader(t)

	// An established cluster: enough history that compaction has run past
	// index 1, so a fresh node cannot be served by AppendEntries alone.
	var seq uint64
	proposeBurst(t, rafts, &seq, 60)
	requireCompactedPast(t, rafts, 1)
	waitForSurvivorConvergence(t, rafts, 10*time.Second)

	// The new node: an empty data directory, join mode, every peer known.
	dirs[3] = t.TempDir()
	joinOpts := append([]db.Option{db.WithJoin()}, opts...)
	node4 := openWalRaft(t, 4, all, dirs[3], fc, joinOpts, fatalC)
	rafts = append(rafts, node4)

	leader := rafts[:3].LeaderRaft()
	leader.submitConfChange(addLearnerCC(4, string(all[3].Context)))
	waitForLearner(t, leader, 4)

	// It catches up: the fetch path runs once, the snapshot installs, and
	// the learner applies everything the cluster has.
	deadline := time.Now().Add(30 * time.Second)
	for node4.storage.AppliedIndex() < leader.storage.AppliedIndex() || node4.storage.EventIndex() != node4.storage.AppliedIndex() {
		if time.Now().After(deadline) {
			st, active := node4.raft.CatchUp()
			t.Fatalf("learner did not catch up: applied=%d event=%d leader applied=%d catchingUp=%v %+v",
				node4.storage.AppliedIndex(), node4.storage.EventIndex(), leader.storage.AppliedIndex(), active, st)
		}
		requireNoFatal(t, fatalC)
		time.Sleep(20 * time.Millisecond)
	}
	if runs := node4.raft.CatchUpRunsForTest(); runs != 1 {
		t.Fatalf("learner began %d catch-ups, expected exactly 1", runs)
	}
	if _, active := node4.raft.CatchUp(); active {
		t.Fatal("catch-up status must clear once the snapshot has installed")
	}

	// Promote it and prove it is a full participant.
	leader.submitConfChange(promoteCC(4))
	waitForVoter(t, leader, 4)
	proposeBurst(t, rafts, &seq, 3)
	requireNoFatal(t, fatalC)

	// Byte-identical events across all four.
	waitForSurvivorConvergence(t, rafts, 10*time.Second)
	fc.Partition([]uint64{1}, []uint64{2, 3, 4})
	fc.Partition([]uint64{2}, []uint64{3, 4})
	fc.Partition([]uint64{3}, []uint64{4})
	nodes := rafts
	for _, r := range nodes {
		_ = r.Close()
	}
	for _, r := range nodes {
		r.mu.RLock()
		s := r.storage
		r.mu.RUnlock()
		_ = s.Close()
	}
	rafts = nil // the deferred cleanup has nothing left to close
	assertEventLogPrefixMatches(t, nodes, dirs)
}

// A member that was away while the cluster committed and completed a scrub
// comes back with an event log at an OLDER generation: its bytes still hold
// the erased subject's upsert, and the snapshot it is offered carries a
// bbolt whose tombstones for that scrub are already pruned — nothing could
// bring its log forward. The node must discard its log and fetch it whole
// from a peer at the cluster's generation, ending byte-identical with the
// peers and without the erased upsert. Right-to-be-forgotten survives the
// member's absence.
func TestAdversarial_MemberBackFromAScrubRefetchesWhole(t *testing.T) {
	rafts, fc, dirs, fatalC := newSevereLagCluster(t, 3, catchUpNodeOpts())
	alive := []bool{true, true, true}
	defer func() {
		for i, r := range rafts {
			if alive[i] {
				_ = r.Close()
			}
		}
		for i, r := range rafts {
			if alive[i] {
				r.mu.RLock()
				s := r.storage
				r.mu.RUnlock()
				_ = s.Close()
			}
		}
	}()
	rafts.WaitForLeader(t)

	// A subject's data, then its delete: the tombstone the scrub acts on.
	typeEntity, err := cluster.NewUpsertTypeEntity(&cluster.Type{ID: "subjects", Name: "subjects", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	propose := func(rs Rafts, entities ...*cluster.Entity) []byte {
		bs, err := (&cluster.Proposal{Entities: entities}).Marshal()
		if err != nil {
			t.Fatal(err)
		}
		proposeAndCheckBytes(t, rs, bs)
		for _, r := range rs {
			waitForUserEntry(t, r, bs)
		}
		return bs
	}
	propose(rafts, typeEntity)
	propose(rafts, &cluster.Entity{Type: &cluster.Type{ID: "subjects"}, Key: []byte("alice"), Data: []byte(`{"pii":true}`)})
	propose(rafts, cluster.NewDeleteEntity(&cluster.Type{ID: "subjects"}, []byte("alice")))
	var seq uint64
	proposeBurst(t, rafts, &seq, 10)
	waitForSurvivorConvergence(t, rafts, 10*time.Second)

	// The upsert's raft index, read from the permanent event log (the raft
	// log compacts aggressively here and may already be past it).
	upsertIndex := func(ws *wal.Storage) uint64 {
		t.Helper()
		for i := ws.EventIndex(); i >= 1; i-- {
			a, err := ws.ActualAt(i)
			if err != nil {
				continue
			}
			for _, e := range a.Entities {
				if string(e.Key) == "alice" && !e.IsDelete() {
					return i
				}
			}
		}
		t.Fatal("alice's upsert not found in the event log")
		return 0
	}
	aliceIdx := upsertIndex(rafts[2].storage.(*wal.Storage))
	if _, err := rafts[2].storage.(*wal.Storage).ActualAt(aliceIdx); err != nil {
		t.Fatalf("alice's upsert must be in every log before the scrub: %v", err)
	}

	// Node 3 leaves; the survivors scrub and move on.
	follower3 := rafts[2]
	if err := follower3.Close(); err != nil {
		t.Fatal(err)
	}
	if err := follower3.storage.Close(); err != nil {
		t.Fatal(err)
	}
	alive[2] = false
	survivors := Rafts{rafts[0], rafts[1]}
	survivors.WaitForLeader(t)

	bound := survivors.LeaderRaft().storage.AppliedIndex()
	scrub, err := cluster.NewScrubEntity(bound, false)
	if err != nil {
		t.Fatal(err)
	}
	propose(survivors, scrub)
	for _, r := range survivors {
		ws := r.storage.(*wal.Storage)
		completed := func() uint64 { _, c := ws.ScrubProgress(); return c }
		deadline := time.Now().Add(20 * time.Second)
		for completed() < bound {
			if time.Now().After(deadline) {
				t.Fatalf("node %d never completed the scrub to %d (at %d)", r.id, bound, completed())
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := ws.ActualAt(aliceIdx); err == nil {
			t.Fatalf("node %d still holds alice's upsert after the scrub", r.id)
		}
	}
	// More history, so compaction (and a fresh snapshot carrying the
	// completed bound) moves past node 3's position.
	proposeBurst(t, survivors, &seq, 40)
	requireCompactedPast(t, survivors, aliceIdx)
	waitForSurvivorConvergence(t, survivors, 10*time.Second)

	// Node 3 returns over its stale, unscrubbed log.
	rebootWalNode(t, rafts[2], dirs[2], catchUpNodeOpts(), fatalC)
	alive[2] = true
	time.Sleep(adversarialSettleTime)
	waitForLeaderExtended(t, rafts, 15*time.Second)
	proposeBurst(t, rafts, &seq, 2)
	waitForSurvivorConvergence(t, rafts, 20*time.Second)
	requireNoFatal(t, fatalC)

	back := rafts[2].storage.(*wal.Storage)
	if runs := rafts[2].raft.CatchUpRunsForTest(); runs != 1 {
		t.Fatalf("node 3 began %d catch-ups, expected exactly 1", runs)
	}
	if _, err := back.ActualAt(aliceIdx); err == nil {
		t.Fatal("node 3 still holds alice's upsert: its stale-generation log was not replaced")
	}
	if got := back.EventLogGeneration(); got < bound {
		t.Fatalf("node 3's event log is at generation %d, the cluster's scrub was to %d", got, bound)
	}

	// Byte-identical with the survivors.
	fc.Partition([]uint64{1}, []uint64{2, 3})
	fc.Partition([]uint64{2}, []uint64{3})
	for i, r := range rafts {
		if alive[i] {
			_ = r.Close()
		}
	}
	for i, r := range rafts {
		if alive[i] {
			_ = r.storage.Close()
			alive[i] = false
		}
	}
	assertEventLogPrefixMatches(t, rafts, dirs)
}
