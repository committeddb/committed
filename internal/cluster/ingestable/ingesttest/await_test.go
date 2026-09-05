package ingesttest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/ingesttest"
)

func proposalWith(pos cluster.Position, keys ...string) *cluster.Proposal {
	p := &cluster.Proposal{Position: pos}
	for _, k := range keys {
		p.Entities = append(p.Entities, &cluster.Entity{Key: []byte(k)})
	}
	return p
}

// event is one emission from a fake dialect: a proposal or a channel
// checkpoint.
type event struct {
	p   *cluster.Proposal
	pos cluster.Position
}

// feed plays events in order over UNBUFFERED channels, modeling a dialect's
// single goroutine: each send completes before the next begins, so Await
// observes exactly the emission order.
func feed(events ...event) (<-chan *cluster.Proposal, <-chan cluster.Position) {
	pr := make(chan *cluster.Proposal)
	po := make(chan cluster.Position)
	go func() {
		for _, e := range events {
			if e.p != nil {
				pr <- e.p
			} else {
				po <- e.pos
			}
		}
	}()
	return pr, po
}

// nonEmpty is the loosest useful predicate: any checkpoint at all.
func nonEmpty(pos cluster.Position) bool { return len(pos) > 0 }

// TestAwait_BundledCheckpointSatisfiesTheWait pins the contract's primary
// shape: the commit checkpoint rides the transaction's final proposal, so a
// single proposal carrying the last key AND the position completes the wait
// with that bundled position — nothing ever arrives on the position channel.
func TestAwait_BundledCheckpointSatisfiesTheWait(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(nil, "a")},
		event{p: proposalWith(cluster.Position("ckpt"), "b")},
	)

	res := ingesttest.Await(t, pr, po, 5*time.Second, nonEmpty, "a", "b")
	require.Equal(t, cluster.Position("ckpt"), res.Position)
	require.Len(t, res.Proposals, 2)
	require.True(t, res.Seen["a"] && res.Seen["b"])
}

// TestAwait_ChannelCheckpointSatisfiesTheWait pins the contract's other
// source: a checkpoint with no proposal to ride (an empty-flush commit)
// arrives on the position channel after the data and completes the wait.
func TestAwait_ChannelCheckpointSatisfiesTheWait(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(nil, "a")},
		event{pos: cluster.Position("ckpt")},
	)

	res := ingesttest.Await(t, pr, po, 5*time.Second, nonEmpty, "a")
	require.Equal(t, cluster.Position("ckpt"), res.Position)
}

// TestAwait_PreKeyCheckpointsAreNeverReturned pins the staleness guard: a
// checkpoint observed BEFORE every key has been seen (a snapshot-phase or
// prior-commit position) must not satisfy the wait, even when the predicate
// would accept it — the returned Position always covers the awaited data.
func TestAwait_PreKeyCheckpointsAreNeverReturned(t *testing.T) {
	pr, po := feed(
		event{pos: cluster.Position("stale")},
		event{p: proposalWith(cluster.Position("stale-bundle"), "not-the-key")},
		event{p: proposalWith(cluster.Position("covering"), "key")},
	)

	res := ingesttest.Await(t, pr, po, 5*time.Second, nonEmpty, "key")
	require.Equal(t, cluster.Position("covering"), res.Position)
}

// TestAwait_PredicateRejectionKeepsWaiting pins predicate filtering: an
// acceptable-looking but predicate-rejected checkpoint (e.g. snapshot
// progress under a streaming predicate) keeps the wait open until one
// passes.
func TestAwait_PredicateRejectionKeepsWaiting(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(cluster.Position("snapshot-progress"), "key")},
		event{pos: cluster.Position("commit")},
	)

	isCommit := func(pos cluster.Position) bool { return string(pos) == "commit" }
	res := ingesttest.Await(t, pr, po, 5*time.Second, isCommit, "key")
	require.Equal(t, cluster.Position("commit"), res.Position)
}

// TestAwait_NilPredicateIsAKeysOnlyWait pins the nil-wantPos mode: the wait
// ends as soon as the keys are seen, and Position best-effort carries a
// checkpoint observed at or after that point (possibly nil).
func TestAwait_NilPredicateIsAKeysOnlyWait(t *testing.T) {
	pr, po := feed(
		event{pos: cluster.Position("stale")},
		event{p: proposalWith(nil, "key")},
	)
	res := ingesttest.Await(t, pr, po, 5*time.Second, nil, "key")
	require.Nil(t, res.Position, "a pre-key checkpoint must not surface; nothing after the keys means nil")

	pr, po = feed(event{p: proposalWith(cluster.Position("bundle"), "key")})
	res = ingesttest.Await(t, pr, po, 5*time.Second, nil, "key")
	require.Equal(t, cluster.Position("bundle"), res.Position,
		"a bundle on the key-completing proposal itself is at-or-after the keys")
}

func marker(epoch uint64) *cluster.Proposal {
	return &cluster.Proposal{Entities: []*cluster.Entity{cluster.NewRefreshBoundaryEntity(&cluster.Type{ID: "t"}, epoch)}}
}

// TestAwaitRefresh_ClosesOnKeysMarkerAndCheckpoint pins the primary shape: rows,
// the closing marker, then the completion checkpoint on the position channel.
func TestAwaitRefresh_ClosesOnKeysMarkerAndCheckpoint(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(nil, "a")},
		event{p: proposalWith(nil, "b")},
		event{p: marker(2)},
		event{pos: cluster.Position("done")},
	)
	res := ingesttest.AwaitRefresh(t, pr, po, 2*time.Second, nonEmpty, "a", "b")
	require.Equal(t, uint64(2), res.MarkerEpoch)
	require.Equal(t, cluster.Position("done"), res.Position)
	require.NotNil(t, res.Entity("a"))
	require.Nil(t, res.Entity("zzz"))
}

// TestAwaitRefresh_CheckpointBeforeMarkerIsKept pins the ordering the two
// buffered channels do not guarantee: the completion checkpoint can be
// received before the marker proposal and must not be dropped — the flake
// that motivated the primitive.
func TestAwaitRefresh_CheckpointBeforeMarkerIsKept(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(nil, "a")},
		event{pos: cluster.Position("done")},
		event{p: marker(3)},
	)
	res := ingesttest.AwaitRefresh(t, pr, po, 2*time.Second, nonEmpty, "a")
	require.Equal(t, uint64(3), res.MarkerEpoch)
	require.Equal(t, cluster.Position("done"), res.Position)
}

// TestAwaitRefresh_EarlyMarkerIsNotTheClosingOne: a marker received before the
// keys belongs to an earlier enumeration (a no-op resume, say) and is ignored;
// the closing marker is the first one AFTER every key.
func TestAwaitRefresh_EarlyMarkerIsNotTheClosingOne(t *testing.T) {
	pr, po := feed(
		event{p: marker(1)},
		event{p: proposalWith(nil, "a")},
		event{p: marker(2)},
		event{pos: cluster.Position("done")},
	)
	res := ingesttest.AwaitRefresh(t, pr, po, 2*time.Second, nonEmpty, "a")
	require.Equal(t, uint64(2), res.MarkerEpoch)
}

// TestAwaitRefresh_NilPredicateWaitsForKeysAndMarkerOnly.
func TestAwaitRefresh_NilPredicateWaitsForKeysAndMarkerOnly(t *testing.T) {
	pr, po := feed(
		event{p: proposalWith(nil, "a")},
		event{p: marker(5)},
	)
	res := ingesttest.AwaitRefresh(t, pr, po, 2*time.Second, nil, "a")
	require.Equal(t, uint64(5), res.MarkerEpoch)
	require.Nil(t, res.Position)
}
