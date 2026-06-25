//go:build integration

package httptransport_test

import (
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/httptransport"
	dbtesting "github.com/committeddb/committed/internal/cluster/db/testing"
)

// TestPerf_Transport is a transport A/B harness, not an assertion: it prints
// steady-state throughput and commit-latency percentiles for a 3-node loopback
// cluster wired to the real httptransport. Run it against the homegrown
// transport, then `git stash` back to the old rafthttp one and run again, and
// compare. Storage is in-memory (no fsync), so the numbers isolate transport +
// raft overhead.
//
// It lives in httptransport (not db) and builds its cluster from db's public
// API — db.NewRaft + db.WithTransportFactory(httptransport.Factory()) + the
// importable in-memory db.Storage — rather than db's internal test harness, so
// the transport's benchmark sits next to the transport it measures.
//
// Run: go test -tags integration -run TestPerf_Transport -v -timeout 300s ./internal/cluster/db/httptransport/
//
// The backlog-catch-up scenario (a partitioned follower draining the leader's
// log on heal) is intentionally not here: it needs db's fault-injection harness
// (FaultyCluster.Partition rides on the db-test-only WithTransportWrapperForTest
// seam, which httptransport can't reach), the pipelining question it explored is
// settled and recorded in this package's doc comment, and the adversarial
// suite's severe-lag-rebuild case already gates catch-up correctness.
func TestPerf_Transport(t *testing.T) {
	nodes := newPerfCluster(t, 3)
	defer closePerfCluster(nodes)

	leaderID := waitForLeader(t, nodes)
	var leader, follower *perfNode
	for _, n := range nodes {
		switch {
		case n.id == leaderID:
			leader = n
		case follower == nil:
			follower = n // any follower — its LastIndex advances only on transport delivery
		}
	}

	li := func(n *perfNode) uint64 { return n.lastIndex(t) }

	// Warm up so leader election / first-Ready costs don't pollute the numbers.
	for i := 0; i < 100; i++ {
		leader.proposeC <- []byte(fmt.Sprintf("warm-%d", i))
	}
	for li(follower) < 100 {
		time.Sleep(time.Millisecond)
	}

	// --- Steady-state throughput: fire N proposes, time until the FOLLOWER has
	// them all (i.e. they were transported leader→follower). ---
	const N = 10000
	base := li(follower)
	start := time.Now()
	go func() {
		for i := 0; i < N; i++ {
			leader.proposeC <- []byte(fmt.Sprintf("thr-%d", i))
		}
	}()
	for li(follower) < base+N {
		time.Sleep(200 * time.Microsecond)
	}
	thrElapsed := time.Since(start)

	// --- Commit latency: M synchronous propose→follower-has-it, finely polled.
	// One entry at a time, so this is a single leader→follower round trip. ---
	const M = 1000
	lats := make([]time.Duration, 0, M)
	for i := 0; i < M; i++ {
		want := li(follower) + 1
		t0 := time.Now()
		leader.proposeC <- []byte(fmt.Sprintf("lat-%d", i))
		for li(follower) < want {
			time.Sleep(25 * time.Microsecond)
		}
		lats = append(lats, time.Since(t0))
	}
	sort.Slice(lats, func(a, b int) bool { return lats[a] < lats[b] })
	pct := func(p float64) time.Duration { return lats[int(float64(len(lats)-1)*p)] }
	var sum time.Duration
	for _, d := range lats {
		sum += d
	}

	fmt.Printf("\n========== TRANSPORT PERF ==========\n")
	fmt.Printf("throughput : %6.0f entries/sec  (%d entries in %v)\n",
		float64(N)/thrElapsed.Seconds(), N, thrElapsed.Round(time.Millisecond))
	fmt.Printf("latency    : p50=%-10v p99=%-10v mean=%-10v max=%v\n",
		pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond),
		(sum / time.Duration(len(lats))).Round(time.Microsecond), lats[len(lats)-1].Round(time.Microsecond))
}

// perfNode is one raft node in the self-contained perf cluster: the db.Raft
// under test plus the handles the benchmark drives it through — propose in,
// storage to read the committed head out of.
type perfNode struct {
	id       uint64
	raft     *db.Raft
	storage  *dbtesting.MemoryStorage
	proposeC chan []byte
}

func (n *perfNode) lastIndex(t *testing.T) uint64 {
	t.Helper()
	idx, err := n.storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

// newPerfCluster builds an n-node loopback raft cluster wired to the real
// httptransport (via the injected factory) with in-memory, no-fsync storage, so
// the measured time is transport + raft and not disk. Each peer's URL is a
// freshly picked free loopback port. Mirrors what db's own test harness does,
// but using only db's exported surface so it can live here next to the transport.
func newPerfCluster(t *testing.T, n int) []*perfNode {
	t.Helper()

	ports := pickFreePorts(t, n)
	peers := make([]raft.Peer, 0, n)
	for i := 0; i < n; i++ {
		peers = append(peers, raft.Peer{
			ID:      uint64(i + 1),
			Context: []byte(fmt.Sprintf("http://127.0.0.1:%d", ports[i])),
		})
	}

	nodes := make([]*perfNode, 0, n)
	for _, p := range peers {
		proposeC := make(chan []byte)
		confChangeC := make(chan *raftpb.ConfChangeV2)
		s := dbtesting.NewMemoryStorage()

		_, r := db.NewRaft(p.ID, peers, s, proposeC, confChangeC,
			db.WithTickInterval(10*time.Millisecond),
			db.WithTransportFactory(httptransport.Factory()))

		nodes = append(nodes, &perfNode{id: p.ID, raft: r, storage: s, proposeC: proposeC})
	}

	return nodes
}

func closePerfCluster(nodes []*perfNode) {
	for _, n := range nodes {
		_ = n.raft.Close()
	}
}

// waitForLeader blocks until every node reports the same non-zero leader ID and
// that leader is one of the nodes, or fails after a fixed timeout. This is the
// barrier before proposing — without it a propose can race a not-yet-elected
// node and hang or get dropped on a stale term.
func waitForLeader(t *testing.T, nodes []*perfNode) uint64 {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("waitForLeader: no stable leader within 5s")
		}
		if id, ok := stableLeader(nodes); ok {
			return id
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stableLeader returns the leader ID when every node agrees on the same
// non-zero leader and that leader is itself one of the nodes; (0, false)
// otherwise. The "leader is one of the nodes" check rejects the transient
// window where a node still points at a stale ID before re-election settles.
func stableLeader(nodes []*perfNode) (uint64, bool) {
	if len(nodes) == 0 {
		return 0, false
	}
	first := nodes[0].raft.Leader()
	if first == 0 {
		return 0, false
	}
	for _, n := range nodes[1:] {
		if n.raft.Leader() != first {
			return 0, false
		}
	}
	for _, n := range nodes {
		if n.id == first {
			return first, true
		}
	}
	return 0, false
}

// pickFreePorts grabs n free loopback TCP ports by binding ephemeral listeners
// and reading back the assigned port, then closing them. There is a small race
// (another process could claim a port between close and the transport's bind),
// but it is negligible for a local benchmark.
func pickFreePorts(t *testing.T, n int) []int {
	t.Helper()

	listeners := make([]net.Listener, 0, n)
	ports := make([]int, 0, n)
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("pickFreePorts: %v", err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}

	return ports
}
