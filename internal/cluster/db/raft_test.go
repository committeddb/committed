package db_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// multiNodeTickInterval is the per-tick interval used by multi-node raft
// tests. Single-node tests use 1ms (testTickInterval) because there's no
// network round trip and election completes in one tick. Multi-node tests
// communicate over real httptransport (loopback TCP), and 1ms is too tight
// for the heartbeat round trip — leaders lose contact with followers within
// the 10-tick election timeout, triggering re-election thrash that prevents
// any user proposal from being committed before the next election starts.
//
// 10ms gives a 100ms election timeout, which is comfortable for loopback
// HTTP. The trade-off is that multi-node tests pay ~100ms of startup latency
// (one election cycle) instead of ~10ms.
const multiNodeTickInterval = 10 * time.Millisecond

// multiNodeStartupTimeout bounds how long a multi-node test will wait for
// the cluster to elect a stable leader before failing. With a 100ms election
// timeout and HTTP transport startup, 5s is a generous upper bound.
const multiNodeStartupTimeout = 5 * time.Second

// TestRaftPropose covers the single-node propose path: a fresh cluster of
// one, propose N inputs, verify they appear in storage. The single-node
// shape uses the empty-URL escape hatch in httptransport so it doesn't
// bind any TCP listener — that's what keeps it as a unit test rather than
// an integration test.
//
// The 3-node ("cluster3") variant lives in raft_multinode_test.go behind
// the `integration` build tag because it binds real loopback ports and is
// timing-sensitive in a way unit tests shouldn't be.
func TestRaftPropose(t *testing.T) {
	tests := map[string]struct {
		clusterSize int
		inputs      []string
	}{
		"simple": {clusterSize: 1, inputs: []string{"a/b/c"}},
		"two":    {clusterSize: 1, inputs: []string{"a/b/c", "foo"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rafts := createRafts(tc.clusterSize)
			defer rafts.Close()
			// Drainers must stop BEFORE rafts.Close so they unblock via
			// the stop channel rather than racing commitC's close. Defer
			// LIFO ordering puts this stop call before rafts.Close.
			stopDrainers := rafts.StartDrainers()
			defer stopDrainers()

			// Multi-node clusters need to settle on a leader before any
			// propose can succeed. Single-node clusters elect themselves
			// in one tick so the wait is a no-op for them, but it's safe
			// to call uniformly. Without this barrier, a 3-node propose
			// races the initial election: the first propose may land on
			// a not-yet-elected node and either hang or be dropped on a
			// stale term.
			rafts.WaitForLeader(t)

			r := rafts[0]

			for _, input := range tc.inputs {
				proposeAndCheck(t, r, input)
			}

			// Verify each node's storage contains the user proposals.
			// We can't hardcode "first proposal is at index N" because
			// the index depends on bootstrap shape: a 1-node StartNode
			// generates 1 conf-change entry, a 3-node StartNode generates
			// 3, and either way etcd raft adds an empty leader entry on
			// term start. Walking each node's filtered EntryNormal
			// entries with non-nil Data is robust against both shapes.
			for _, r := range rafts {
				userEnts, err := r.ents()
				if err != nil {
					t.Fatal(err)
				}
				if len(userEnts) < len(tc.inputs) {
					t.Fatalf("node %d: expected at least %d user entries, got %d",
						r.id, len(tc.inputs), len(userEnts))
				}
				// Take the LAST len(tc.inputs) user entries — earlier
				// ones may exist from a previous restart cycle in
				// TestRaftRestart, but TestRaftPropose's clusters are
				// fresh so the take is exact.
				start := len(userEnts) - len(tc.inputs)
				for i, input := range tc.inputs {
					diff := cmp.Diff([]byte(input), userEnts[start+i].Data)
					if diff != "" {
						t.Fatalf("node %d, entry %d: %s", r.id, i, diff)
					}
				}
			}
		})
	}
}

// TestRaftRestart covers single-node propose → restart in place → propose
// more → verify everything is in storage. The 3-node variant lives in
// raft_multinode_test.go behind the `integration` build tag.
func TestRaftRestart(t *testing.T) {
	tests := map[string]struct {
		clusterSize int
		inputs1     []string
		inputs2     []string
	}{
		"simple": {clusterSize: 1, inputs1: []string{"foo", "bar"}, inputs2: []string{"baz"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rafts := createRafts(tc.clusterSize)
			defer rafts.Close()

			// Per-node drainers so we can swap the one belonging to the
			// node that gets restarted. A test-wide StartDrainers() would
			// hold a stale commitC reference for the restarted node and
			// silently stop draining, which would deadlock the new Ready
			// loop on its first commit.
			drainers := make([]func(), len(rafts))
			for i, r := range rafts {
				drainers[i] = r.startDrainer()
			}
			defer func() {
				for _, d := range drainers {
					if d != nil {
						d()
					}
				}
			}()

			rafts.WaitForLeader(t)

			r := rafts[0]

			for _, input := range tc.inputs1 {
				proposeAndCheck(t, r, input)
			}

			lastIndex(t, r)

			// Stop drainer for node 0 BEFORE Restart so the goroutine
			// exits cleanly via its stop channel before db.Raft.Close
			// closes the channel out from under it.
			drainers[0]()
			drainers[0] = nil

			// Restart node 0 in place. For multi-node clusters this only
			// restarts one of three replicas — the others stay up, the
			// cluster keeps quorum, and node 0 catches back up via raft
			// log replication when it returns.
			err := rafts[0].Restart()
			if err != nil {
				t.Fatal(err)
			}

			// New commitC after Restart needs a fresh drainer to keep the
			// new Ready loop unblocked. Without this, the first replayed
			// committed entry blocks processCommittedEntry, which blocks
			// the entire Ready loop, which blocks the next propose.
			drainers[0] = rafts[0].startDrainer()

			// After a single-node restart there's no other voter to elect a
			// leader, so this re-elects itself. After a multi-node restart
			// the existing leader (which may or may not have been node 0)
			// is still alive and accepting proposes; node 0 reconnects and
			// the cluster converges back on a stable leader.
			rafts.WaitForLeader(t)

			lastIndex(t, r)

			r = rafts[0]

			hs, cs, err := r.storage.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("hard state: %v, conf state: %v\n", hs, cs)

			for _, input := range tc.inputs2 {
				proposeAndCheck(t, r, input)
			}

			lastIndex(t, rafts[0])

			for _, r := range rafts {
				inputs := slices.Concat(tc.inputs1, tc.inputs2)

				es, err := r.ents()
				if err != nil {
					t.Fatal(err)
				}

				if len(es) < len(inputs) {
					t.Fatalf("node %d: expected at least %d user entries, got %d",
						r.id, len(inputs), len(es))
				}

				start := len(es) - len(inputs)
				for i, want := range inputs {
					diff := cmp.Diff(want, string(es[start+i].Data))
					if diff != "" {
						t.Fatalf("node %d, entry %d: %s", r.id, i, diff)
					}
				}
			}
		})
	}
}

// proposeAndCheck sends `input` on the raft's proposeC and waits until the
// proposal appears at the tail of the node's user-entry log. It does NOT
// read from commitC — that's handled by background drainers (see
// Rafts.StartDrainers) so the Ready loop never blocks on a synchronous send
// when no test goroutine is reading. Polling storage is the correct
// synchronization barrier here because storage is the durable state we
// actually care about asserting on.
func proposeAndCheck(t *testing.T, r *Raft, input string) {
	t.Helper()
	r.proposeC <- []byte(input)
	waitForUserEntry(t, r, []byte(input))
}

// waitForUserEntry polls r's storage until the most recent user entry
// (EntryNormal with non-nil Data, post-filtering) equals `want`. Bounded
// by multiNodeStartupTimeout so a hung propose surfaces as a useful error
// instead of a test timeout.
func waitForUserEntry(t *testing.T, r *Raft, want []byte) {
	t.Helper()
	deadline := time.Now().Add(multiNodeStartupTimeout)
	for {
		es, _ := r.ents()
		if len(es) > 0 && cmp.Equal(es[len(es)-1].Data, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %d: timed out waiting for user entry %q (have %d entries)",
				r.id, string(want), len(es))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func lastIndex(t *testing.T, r *Raft) uint64 {
	li, err := r.storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("LastIndex: %d\n", li)

	return li
}

// createRafts builds a `replicas`-node raft cluster wired together over real
// loopback HTTP. Single-node clusters use the empty-URL escape hatch in
// httptransport (no listener bound) and the fast 1ms tick interval; clusters
// with replicas > 1 dynamically allocate free ports on 127.0.0.1 and use the
// slower multiNodeTickInterval so the HTTP-transported heartbeats keep up
// with the election timeout.
func createRafts(replicas int) Rafts {
	rafts, _ := buildRafts(replicas, nil, nil)
	return rafts
}

// createFaultyRafts builds a `replicas`-node cluster like createRafts, but
// additionally constructs a FaultyCluster and wraps every node's transport
// with a FaultyTransport registered in it. Returns both so the caller can
// call cluster.Partition / cluster.Heal directly.
//
// The peer set is determined inside this function (via pickFreePorts), so
// the FaultyCluster must be constructed here too — callers can't build one
// ahead of time because they don't know the URLs yet.
func createFaultyRafts(replicas int) (Rafts, *FaultyCluster) {
	return buildRafts(replicas, newFaultyCluster, nil)
}

// createFaultyRaftsWithStorageWrapper is like createFaultyRafts but lets the
// caller wrap each node's storage at construction time. The wrapper
// receives the default MemoryStorage and returns a db.Storage that will be
// used in its place. A nil wrapper, or a wrapper that returns its input
// unchanged, is equivalent to createFaultyRafts.
//
// Used by scenario (f) to swap node 1's storage for a FaultyStorage that
// trips ENOSPC after a threshold. The wrapper is called per node so the
// caller can decide which nodes to wrap and which to leave alone.
func createFaultyRaftsWithStorageWrapper(replicas int, wrapStorage func(id uint64, s db.Storage) db.Storage) (Rafts, *FaultyCluster) {
	return buildRafts(replicas, newFaultyCluster, wrapStorage)
}

// newFaultyCluster is a factory used by buildRafts when the caller asks for
// a faulty cluster. Kept as a function pointer rather than a boolean so the
// buildRafts signature stays narrow — if a future helper wants a different
// cluster-wrapper shape (e.g., a latency-only wrapper), it plugs in the
// same way.
func newFaultyCluster(peers []raft.Peer) *FaultyCluster {
	return NewFaultyCluster(peers)
}

// buildRafts is the internal constructor shared by createRafts and
// createFaultyRafts. When clusterFactory is nil, it behaves exactly like
// the legacy createRafts — no FaultyCluster is attached, no transport
// wrapping happens, and the returned cluster pointer is nil. When non-nil,
// it builds a FaultyCluster from the peer set and threads Wrap(id) into
// each node via WithTransportWrapperForTest.
//
// wrapStorage, when non-nil, receives each freshly-constructed
// MemoryStorage and may return a wrapper (e.g., a FaultyStorage) to use in
// its place. nil leaves storage as the plain MemoryStorage.
func buildRafts(replicas int, clusterFactory func([]raft.Peer) *FaultyCluster, wrapStorage func(id uint64, s db.Storage) db.Storage) (Rafts, *FaultyCluster) {
	tick := testTickInterval
	if replicas > 1 {
		tick = multiNodeTickInterval
	}

	var peers []raft.Peer
	if replicas == 1 {
		// Empty Context tells the transport to skip binding a listener.
		// Matches db.New's default and keeps single-node tests free of
		// any TCP / port-allocation concerns.
		peers = append(peers, raft.Peer{ID: 1, Context: []byte("")})
	} else {
		ports := pickFreePorts(replicas)
		for i := 0; i < replicas; i++ {
			id := uint64(i + 1)
			ctx := fmt.Sprintf("http://127.0.0.1:%d", ports[i])
			peers = append(peers, raft.Peer{ID: id, Context: []byte(ctx)})
		}
	}

	var cluster *FaultyCluster
	if clusterFactory != nil {
		cluster = clusterFactory(peers)
	}

	var rafts Rafts
	for _, p := range peers {
		var s db.Storage = NewMemoryStorage()
		if wrapStorage != nil {
			s = wrapStorage(p.ID, s)
		}
		rafts = append(rafts, createRaft(p.ID, peers, s, tick, cluster))
	}

	return rafts, cluster
}

// portCounter is bumped per port allocation so successive calls within the
// same process don't pick adjacent ports (which can interact badly with
// listener-close + TIME_WAIT cycles on `go test -count=N` runs).
var portCounter atomic.Uint64

// pickFreePorts asks the kernel for `n` free TCP ports on 127.0.0.1 by
// briefly opening listeners, recording the assigned port, and closing them.
// There's an inherent TOCTOU window — another process could grab the port
// before we re-bind in httptransport — but in practice, sequential allocate
// + immediate use is reliable enough for in-process test clusters.
//
// Holding the listeners open until the end of the loop (rather than closing
// each as we go) reduces the chance that the kernel reuses the same port
// for two of our requested slots.
func pickFreePorts(n int) []int {
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
			panic(fmt.Sprintf("pickFreePorts: %v", err))
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	portCounter.Add(uint64(n))
	return ports
}

// createRaft builds a single *Raft. When cluster is non-nil, the raft is
// constructed with WithTransportWrapperForTest(cluster.Wrap(id)) so the
// node's transport becomes a FaultyTransport registered with the cluster.
// When cluster is nil, the raft uses the plain HttpTransport (same as the
// pre-adversarial-suite behaviour).
//
// The cluster reference is retained on the returned *Raft so Restart can
// re-wrap after swapping the underlying db.Raft — without the reference,
// a restarted node would come back up with a plain transport and
// cluster.Partition would silently stop affecting it.
func createRaft(id uint64, peers []raft.Peer, s db.Storage, tickInterval time.Duration, cluster *FaultyCluster) *Raft {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	opts := []db.Option{db.WithTickInterval(tickInterval)}
	if cluster != nil {
		opts = append(opts, db.WithTransportWrapperForTest(cluster.Wrap(id)))
	}

	commitC, errorC, r := db.NewRaft(id, peers, s, proposeC, confChangeC, opts...)

	return &Raft{
		storage:       s,
		commitC:       commitC,
		errorC:        errorC,
		raft:          r,
		peers:         peers,
		proposeC:      proposeC,
		confChangeC:   confChangeC,
		id:            id,
		tickInterval:  tickInterval,
		faultyCluster: cluster,
	}
}

type Raft struct {
	// mu guards the swap-on-Restart fields (storage, commitC, errorC,
	// raft, proposeC, confChangeC, peers). Restart() rebuilds the
	// underlying db.Raft and reassigns these fields, which races with
	// concurrent readers — notably the adversarial suite's flap
	// scenario where a background proposer goroutine calls ents() and
	// proposeC send while the main goroutine kills + restarts the
	// leader. A RWMutex keeps readers cheap (RLock in ents, Leader,
	// etc.) while serialising the one Restart writer.
	//
	// faultyCluster, id, and tickInterval don't change across Restart
	// so they don't need the lock, but including them keeps the struct
	// cohesive — an explicit accessor for each is an unnecessary
	// indirection.
	mu           sync.RWMutex
	storage      db.Storage
	peers        []raft.Peer
	commitC      <-chan []byte
	errorC       <-chan error
	raft         *db.Raft
	proposeC     chan<- []byte
	confChangeC  chan<- raftpb.ConfChange
	id           uint64
	tickInterval time.Duration
	// faultyCluster is nil unless this Raft was built via createFaultyRafts.
	// Kept here so Restart() can pass the cluster back through to
	// createRaft, re-wrapping the new db.Raft's transport with a fresh
	// FaultyTransport registered in the same cluster. Without this,
	// Restart would come back up with a plain transport and any
	// subsequent cluster.Partition would silently skip this node.
	faultyCluster *FaultyCluster
}

// Leader returns the node ID this Raft believes is the current leader, or
// 0 if no leader is known. Thin pass-through to the underlying db.Raft so
// tests can poll for cluster readiness without touching internals.
func (rs *Raft) Leader() uint64 {
	rs.mu.RLock()
	r := rs.raft
	rs.mu.RUnlock()
	return r.Leader()
}

// Close shuts down the underlying db.Raft. db.Raft.Close is idempotent so
// it's safe to call from both Restart and Rafts.Close.
func (rs *Raft) Close() error {
	rs.mu.RLock()
	r := rs.raft
	rs.mu.RUnlock()
	return r.Close()
}

func (rs *Raft) Restart() error {
	// Close the current underlying db.Raft before we build a replacement.
	// We hold only the RLock to read `rs.raft` — Close itself is
	// idempotent and thread-safe, and grabbing the write lock here would
	// deadlock against any in-flight ents() / proposeC readers that
	// also need the RLock.
	rs.mu.RLock()
	old := rs.raft
	storage := rs.storage
	peers := rs.peers
	id := rs.id
	tick := rs.tickInterval
	fc := rs.faultyCluster
	rs.mu.RUnlock()

	if err := old.Close(); err != nil {
		return err
	}

	// Pass the original FaultyCluster back in so the restarted node comes
	// back up with a fresh FaultyTransport registered against the same
	// cluster. FaultyCluster.Wrap overwrites the old (id → transport)
	// entry, so post-restart partitions target the new transport and no
	// stale pointer leaks from the pre-close transport.
	r := createRaft(id, peers, storage, tick, fc)

	// Swap the new fields in under the write lock. Concurrent readers
	// (ents, Leader, Close) see either the old complete state or the
	// new complete state, never a torn mixture.
	rs.mu.Lock()
	rs.storage = r.storage
	rs.peers = r.peers
	rs.commitC = r.commitC
	rs.errorC = r.errorC
	rs.raft = r.raft
	rs.proposeC = r.proposeC
	rs.confChangeC = r.confChangeC
	rs.id = r.id
	rs.faultyCluster = r.faultyCluster
	rs.mu.Unlock()

	return nil
}

func (rs *Raft) ents() ([]raftpb.Entry, error) {
	// Snapshot the storage pointer under the RLock so a concurrent
	// Restart (which swaps rs.storage) doesn't interleave the field
	// reassignment with our reads. The actual Storage implementation
	// has its own internal synchronisation for FirstIndex/LastIndex/
	// Entries.
	rs.mu.RLock()
	s := rs.storage
	rs.mu.RUnlock()

	fi, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}

	li, err := s.LastIndex()
	if err != nil {
		return nil, err
	}

	// Entries semantics are [lo, hi) — pass li+1 so the last entry is
	// actually included. The previous off-by-one was masked because tests
	// only checked the prefix of the log, never the tail.
	//
	// MaxUint64 on maxSize so we never truncate the returned slice. The
	// old 10000-byte cap silently dropped entries on high-volume tests
	// (e.g., scenario (d) with 1000 proposes): each entry is ~10 bytes
	// of user data but ~100 bytes total including raft framing, so the
	// log overflows the cap around ~100 entries and the test sees fewer
	// entries than actually exist — which reads as "entries missing"
	// when they're really just not being returned here.
	ents, err := s.Entries(fi, li+1, math.MaxUint64)
	if err != nil {
		return nil, err
	}

	var es []raftpb.Entry
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			es = append(es, e)
		}
	}

	return es, nil
}

// proposeChan returns the current proposeC under the RLock. The flap
// scenario uses this so a concurrent Restart's channel swap doesn't race
// with the proposer's `ch <- payload` send; the proposer reads the channel
// pointer via proposeChan and then sends on that snapshot.
//
// Tests that don't restart rafts mid-test can still use rs.proposeC
// directly — the field race only manifests when Restart runs concurrently
// with another goroutine reading the field.
func (rs *Raft) proposeChan() chan<- []byte {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.proposeC
}

type Rafts []*Raft

func (rs Rafts) Close() error {
	var err error

	for _, r := range rs {
		ierr := r.Close()
		if ierr != nil {
			err = ierr
		}
	}

	return err
}

// WaitForLeader blocks until every node in the cluster reports the same
// non-zero leader ID, or until multiNodeStartupTimeout elapses. This is the
// barrier multi-node tests use to know that the initial election has settled
// before they start proposing — without it, a propose can race a not-yet-
// elected node and either hang or get dropped on a stale term.
//
// Polling at 5ms is fine: the leader ID is read from etcd raft's Status()
// channel, which is cheap. Stable agreement (all nodes pointing at the same
// leader) is required because a transient state like "node A says leader=B,
// node B says leader=0" is exactly the window where re-election thrash can
// still happen, and we'd rather wait it out than start proposing into it.
func (rs Rafts) WaitForLeader(t *testing.T) uint64 {
	t.Helper()
	deadline := time.Now().Add(multiNodeStartupTimeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("WaitForLeader: no stable leader within %v", multiNodeStartupTimeout)
		}
		leader, ok := rs.stableLeader()
		if ok {
			return leader
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// stableLeader returns the leader ID if every node in the slice agrees on
// the same non-zero leader AND that leader is itself one of the nodes in
// the slice. Returns (0, false) otherwise.
//
// The "leader is in the slice" check matters for kill-leader tests: after
// killing the leader, the survivors may briefly still report the dead
// leader's ID (until their heartbeat deadlines fire and re-election starts).
// During that window stableLeader returns false, so callers correctly keep
// waiting for the new leader to emerge instead of acting on a dead one.
func (rs Rafts) stableLeader() (uint64, bool) {
	if len(rs) == 0 {
		return 0, false
	}
	first := rs[0].Leader()
	if first == 0 {
		return 0, false
	}
	for _, r := range rs[1:] {
		if r.Leader() != first {
			return 0, false
		}
	}
	leaderInSlice := false
	for _, r := range rs {
		if r.id == first {
			leaderInSlice = true
			break
		}
	}
	if !leaderInSlice {
		return 0, false
	}
	return first, true
}

// LeaderRaft returns the *Raft that the cluster currently believes is the
// leader. Caller must have already called WaitForLeader. If the leader can't
// be found in the slice (e.g., it was killed), returns nil.
func (rs Rafts) LeaderRaft() *Raft {
	leaderID, ok := rs.stableLeader()
	if !ok {
		return nil
	}
	for _, r := range rs {
		if r.id == leaderID {
			return r
		}
	}
	return nil
}

// FollowerRaft returns any non-leader Raft in the cluster, or nil if none.
// Useful for "propose on follower → forwards to leader" tests.
func (rs Rafts) FollowerRaft() *Raft {
	leaderID, ok := rs.stableLeader()
	if !ok {
		return nil
	}
	for _, r := range rs {
		if r.id != leaderID {
			return r
		}
	}
	return nil
}

// startDrainer spawns a single goroutine that continuously consumes from
// r.commitC and discards the result. The returned stop function signals
// the goroutine to exit and waits for it. Per-node so that Restart can
// swap drainers cleanly: the restarted node has a brand-new commitC, so
// the test must stop the old drainer (which is reading from the now-closed
// old channel) and start a new one against r.commitC after Restart returns.
//
// commitC is captured at startDrainer call time, so a later Restart that
// reassigns r.commitC does NOT change which channel this goroutine reads
// from. The Restart helper handles this by making the test stop and
// re-start the drainer explicitly.
func (r *Raft) startDrainer() func() {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	commitC := r.commitC
	go func() {
		defer wg.Done()
		for {
			select {
			case _, ok := <-commitC:
				if !ok {
					return
				}
			case <-stop:
				return
			}
		}
	}()
	return func() {
		close(stop)
		wg.Wait()
	}
}

// StartDrainers spawns one drainer per node (see startDrainer) and returns
// a stop function that closes them all in parallel. This is the bridge
// between db.Raft (which sends every committed user proposal synchronously
// to commitC inside the Ready loop) and these tests (which assert on
// storage rather than commitC). Without an active reader on commitC,
// processCommittedEntry blocks the Ready loop, which in turn blocks every
// subsequent commit and deadlocks any multi-propose test.
//
// Drainers must be stopped BEFORE Rafts are closed so the goroutines exit
// via their stop channels rather than racing db.Raft.Close's commitC close.
func (rs Rafts) StartDrainers() func() {
	stops := make([]func(), len(rs))
	for i, r := range rs {
		stops[i] = r.startDrainer()
	}
	return func() {
		for _, s := range stops {
			s()
		}
	}
}

type MemoryPosition struct {
	ProIndex int
	PosIndex int
}

type MemoryStorageSaveArgsForCall struct {
	st   raftpb.HardState
	ents []raftpb.Entry
	snap raftpb.Snapshot
}

type MemoryStorage struct {
	*raft.MemoryStorage
	indexes   map[string]uint64
	positions map[string]*MemoryPosition

	// nodeMu guards node. Tests mutate node from the test goroutine while
	// the DB's worker goroutines read it via Node() to determine leadership;
	// without the mutex this is a data race that the race detector flags.
	nodeMu sync.RWMutex
	node   uint64

	// stateMu guards both saveArgsForCall (which is appended to in Save) and
	// reads of the underlying etcd hardState via InitialState. etcd's
	// MemoryStorage.InitialState is NOT internally synchronised — it just
	// returns ms.hardState directly — so any test that reads it concurrently
	// with the raft worker calling Save would race. We override InitialState
	// to take this mutex and Save acquires it around the SetHardState call.
	stateMu         sync.RWMutex
	saveArgsForCall []*MemoryStorageSaveArgsForCall
}

func NewMemoryStorage() *MemoryStorage {
	indexes := make(map[string]uint64)
	positions := make(map[string]*MemoryPosition)
	return &MemoryStorage{
		MemoryStorage: raft.NewMemoryStorage(),
		indexes:       indexes,
		positions:     positions,
	}
}

func (ms *MemoryStorage) Close() error {
	return nil
}

// InitialState overrides the embedded etcd MemoryStorage's InitialState so
// reads are serialised against Save's SetHardState. etcd's implementation
// returns ms.hardState directly without taking its internal lock, which
// races with concurrent SetHardState writes from the raft worker.
func (ms *MemoryStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ms.stateMu.RLock()
	defer ms.stateMu.RUnlock()
	return ms.MemoryStorage.InitialState()
}

func (ms *MemoryStorage) Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
	err := ms.Append(ents)
	if err != nil {
		return err
	}

	ms.stateMu.Lock()
	ms.maybeAppendArgsForCallLocked(st, ents, snap)
	err = ms.MemoryStorage.SetHardState(st)
	ms.stateMu.Unlock()
	return err
}

// ApplyCommitted is a no-op for the test MemoryStorage. The raft_test
// MemoryStorage is purposely a thin wrapper around etcd's raft.MemoryStorage
// for testing the raft layer in isolation; there are no buckets to apply
// to. AppliedIndex always returns 0 for the same reason.
func (ms *MemoryStorage) ApplyCommitted(entry raftpb.Entry) error {
	return nil
}

func (ms *MemoryStorage) AppliedIndex() uint64 {
	return 0
}

// maybeAppendArgsForCallLocked must be called with stateMu held.
func (ms *MemoryStorage) maybeAppendArgsForCallLocked(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) {
	normalEntry := false
	for _, ent := range ents {
		if ent.Type == raftpb.EntryNormal {
			normalEntry = true
		}
	}

	if normalEntry {
		ms.saveArgsForCall = append(ms.saveArgsForCall, &MemoryStorageSaveArgsForCall{st, ents, snap})
	}
}

func (ms *MemoryStorage) SaveCallCount() int {
	ms.stateMu.RLock()
	defer ms.stateMu.RUnlock()
	return len(ms.saveArgsForCall)
}

func (ms *MemoryStorage) SaveArgsForCall(i int) (raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) {
	ms.stateMu.RLock()
	defer ms.stateMu.RUnlock()
	a := ms.saveArgsForCall[i]
	return a.st, a.ents, a.snap
}

func (ms *MemoryStorage) Type(id string) (*cluster.Type, error) {
	return nil, nil
}

func (ms *MemoryStorage) TimePoints(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error) {
	return nil, nil
}

func (ms *MemoryStorage) Position(id string) cluster.Position {
	pos := ms.positions[id]

	if pos != nil {
		bs, err := json.Marshal(pos)
		if err == nil {
			return bs
		}
	}

	return nil
}

func (ms *MemoryStorage) Reader(id string) db.ProposalReader {
	i, ok := ms.indexes[id]
	if !ok {
		i = 0
	}

	return &Reader{index: i, s: ms}
}

func (ms *MemoryStorage) Node(id string) uint64 {
	ms.nodeMu.RLock()
	defer ms.nodeMu.RUnlock()
	return ms.node
}

func (ms *MemoryStorage) SetNode(n uint64) {
	ms.nodeMu.Lock()
	defer ms.nodeMu.Unlock()
	ms.node = n
}

func (ms *MemoryStorage) Database(id string) (cluster.Database, error) {
	return nil, nil
}

func (ms *MemoryStorage) Databases() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Ingestables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Syncables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Types() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) DatabaseVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) DatabaseVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) SyncableVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) SyncableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) TypeVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) TypeVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Proposals() []*cluster.Proposal {
	fi, _ := ms.FirstIndex()
	li, _ := ms.LastIndex()
	ents, _ := ms.Entries(fi, li+1, math.MaxUint64)

	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(e.Data, ms); err != nil {
				continue
			}

			if len(p.Entities) > 0 {
				ps = append(ps, p)
			}
		}
	}

	return ps
}

// TODO Pull this reader out and make it concrete instead of an interface
type Reader struct {
	sync.Mutex
	index uint64
	s     db.Storage
}

func (r *Reader) Read() (uint64, *cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	for {
		readIndex := r.index + 1

		li, err := r.s.LastIndex()
		if err != nil {
			return 0, nil, err
		}

		if readIndex > li {
			return 0, nil, io.EOF
		}

		ents, err := r.s.Entries(readIndex, readIndex+1, math.MaxUint)
		if err != nil {
			return 0, nil, err
		}

		ent := ents[0]

		r.index = readIndex

		if ent.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(ent.Data, r.s); err != nil {
				return 0, nil, err
			}

			if len(p.Entities) > 0 && !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
				return readIndex, p, nil
			}
		}
	}
}
