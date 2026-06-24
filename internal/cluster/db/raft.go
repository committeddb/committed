package db

import (
	"context"
	"encoding/binary"
	"log"
	"sync"
	"sync/atomic"
	"time"

	tlstransport "go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// configBuildErrorReporter is an optional Storage extension that reports
// the configs that could not be built on this node (a missing ${VAR}
// secret degrades the config rather than crashing the node). The Ready
// loop type-asserts it to emit the committed_config_build_errors gauge
// from the count; db.DB type-asserts it to serve the full list on GET
// /node/status. wal.Storage implements it, the in-memory test double does
// not.
type configBuildErrorReporter interface {
	ConfigBuildErrorCount() int
	ConfigBuildErrors() []cluster.ConfigBuildError
}

type Raft struct {
	proposeC     <-chan []byte              // proposed messages
	proposeConfC <-chan raftpb.ConfChangeV2 // proposed cluster config changes
	// join marks this node as joining an existing cluster: startRaft uses
	// raft.RestartNode (empty state, learn membership from the leader)
	// instead of raft.StartNode (bootstrap from the static peer set). See
	// WithJoin and the join field on options.
	join         bool
	raftErrorC   chan<- error
	raftStopC    chan struct{}
	id           uint64
	leaderState  *LeaderState
	tickInterval time.Duration
	// compactMaxSize and compactMaxAge implement the "10GB or 1hr"
	// policy described in docs/event-log-architecture.md §
	// "Compaction policy". maybeCompact fires when either threshold is
	// crossed; 0 disables that limb. Both zero disables automatic
	// compaction entirely.
	compactMaxSize uint64
	compactMaxAge  time.Duration
	// lastCompactedIndex is the raft index the log was most recently
	// compacted to on this node. Used so maybeCompact doesn't attempt
	// to re-compact to the same point. Atomic so tests (notably the
	// severe-lag adversarial scenario) can read it from a non-serve-
	// channels goroutine to assert compaction progress without racing
	// the writer.
	lastCompactedIndex atomic.Uint64
	// lastCompactTime is the wall-clock moment of the most recent
	// compaction (or startRaft time if no compaction has happened).
	// The age limb of the policy fires when (time.Now() -
	// lastCompactTime) exceeds compactMaxAge. Mutated only from the
	// serveChannels goroutine.
	lastCompactTime time.Time
	// compactionPressure is a hint set by db's disk-usage watcher when free
	// space drops to critical/full: while set, maybeCompact triggers on every
	// Ready iteration (subject to the same compact-point safety constraints)
	// regardless of the size/age limbs, so the node tries to free raft-log
	// disk sooner. Atomic because the watcher goroutine sets it while the
	// serveChannels goroutine reads it in maybeCompact.
	compactionPressure atomic.Bool

	node    raft.Node
	storage Storage

	// applyNotifier is invoked after each successful Storage.ApplyCommitted
	// call with the raw entry data. db.New supplies db.notifyApplied here
	// so blocking db.Propose can release waiters once their proposal has
	// been applied. nil disables the callback (used by raft_test which
	// constructs Raft directly without a db.DB).
	applyNotifier func(data []byte)

	// appliedIndexNotifier is invoked once per Ready iteration that
	// applied at least one committed entry (i.e. AppliedIndex advanced).
	// db.New supplies db.notifyAppliedIndexAdvanced so a LinearizableRead
	// blocked waiting for AppliedIndex to reach its ReadIndex wakes up
	// promptly instead of polling. nil disables it (raft_test path).
	appliedIndexNotifier func()

	// readMu guards readWaiters. ReadIndex registers a waiter keyed by the
	// request-context token it hands to node.ReadIndex; the Ready loop
	// dispatches each rd.ReadStates entry back to the matching waiter by
	// the same token. The token is a monotonic counter (nextReadReq) so
	// concurrent ReadIndex calls never collide.
	readMu      sync.Mutex
	readWaiters map[string]chan uint64
	nextReadReq atomic.Uint64

	transport      Transport
	transportStopC chan struct{} // signals http transport to shutdown
	transportDoneC chan struct{} // signals http transport shutdown complete

	// transportWrapper is captured from the options in newRaftWithOptions
	// so startRaft can apply it after constructing the HttpTransport. nil
	// in all production paths; set by the adversarial test suite via
	// WithTransportWrapperForTest to inject a fault-injection layer
	// around the real transport.
	transportWrapper func(Transport) Transport
	// transportFactory is captured from the options so startRaft can build the
	// peer Transport without db depending on a concrete implementation. The
	// composition root injects it (WithTransportFactory); nil is a wiring error.
	transportFactory TransportFactory
	// tlsInfo is captured from the options so startRaft can pass it to the
	// transport factory. nil means plaintext peer transport (default).
	tlsInfo *tlstransport.TLSInfo

	// closeC is closed by Close() to tell serveChannels (both its inner
	// proposeC reader and its outer Ready loop) to exit. Without this,
	// serveChannels only exits when proposeC is closed externally — which
	// means Close() alone could leave serveChannels running and racing
	// against any new Raft constructed on the same Storage.
	closeC chan struct{}
	// serveChannelsDoneC is closed by serveChannels on exit. Close() waits
	// on it to guarantee serveChannels (and therefore Storage.Save) is no
	// longer running before returning.
	serveChannelsDoneC chan struct{}
	// closeOnce guards close(closeC) so Close() is idempotent and safe to
	// call from multiple paths.
	closeOnce sync.Once

	logger  *zap.Logger
	metrics *metrics.Metrics
}

func NewRaft(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChangeV2, opts ...Option) (<-chan error, *Raft) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newRaftWithOptions(id, ps, s, proposeC, proposeConfC, nil, nil, nil, cfg.logger, cfg)
}

func newRaftWithOptions(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChangeV2, applyNotifier func(data []byte), appliedIndexNotifier func(), lostNotifier func([]uint64), logger *zap.Logger, cfg options) (<-chan error, *Raft) {
	errorC := make(chan error)

	n := &Raft{
		id:                   id,
		proposeC:             proposeC,
		proposeConfC:         proposeConfC,
		raftErrorC:           errorC,
		raftStopC:            make(chan struct{}),
		leaderState:          NewLeaderState(false),
		tickInterval:         cfg.tickInterval,
		compactMaxSize:       cfg.compactMaxSize,
		compactMaxAge:        cfg.compactMaxAge,
		lastCompactTime:      time.Now(),
		storage:              s,
		applyNotifier:        applyNotifier,
		appliedIndexNotifier: appliedIndexNotifier,
		readWaiters:          make(map[string]chan uint64),
		transportStopC:       make(chan struct{}),
		transportDoneC:       make(chan struct{}),
		transportWrapper:     cfg.transportWrapper,
		transportFactory:     cfg.transportFactory,
		tlsInfo:              cfg.tlsInfo,
		join:                 cfg.join,
		closeC:               make(chan struct{}),
		serveChannelsDoneC:   make(chan struct{}),

		logger:  logger,
		metrics: cfg.metrics,
	}
	// Install the truncation lost-notifier on the storage BEFORE startRaft
	// spawns serveChannels (the sole appendEntries caller, where the
	// callback fires). Setting it here, on this goroutine, before the spawn
	// gives the field a happens-before edge to every appendEntries read, so
	// no lock is needed on the storage side. db.New supplies db.notifyLost;
	// NewRaft (raft_test path) passes nil. The in-memory test double doesn't
	// implement the setter, so truncation detection is simply a wal.Storage
	// feature — the leader-change watcher still covers its waiters.
	if lostNotifier != nil {
		if setter, ok := s.(lostNotifierSetter); ok {
			setter.SetLostNotifier(lostNotifier)
		}
	}

	// startRaft itself doesn't block — it sets up the raft.Node and transport
	// then spawns serveRaft/serveChannels as their own goroutines. Calling it
	// synchronously here guarantees that n.transport and n.node are non-nil
	// and the worker goroutines have been launched by the time NewRaft
	// returns, so a fast caller-side Close() can't race with goroutine
	// startup.
	n.startRaft(id, ps)

	return errorC, n
}

// raftElectionTicks is the number of Tick()s without a heartbeat from the
// leader before a follower starts an election (raft.Config.ElectionTick).
// Named so the read-index retry cadence can express itself in the same unit
// — "one election timeout" — rather than duplicating the literal.
const raftElectionTicks = 10

func (n *Raft) startRaft(id uint64, ps []raft.Peer) {
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              raftElectionTicks,
		HeartbeatTick:             1,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		// PreVote runs an "am I electable" round before incrementing term,
		// so a partitioned-then-rejoining node cannot disrupt the cluster
		// by force-stepping-down a healthy leader with an inflated term.
		// Recommended by Ongaro's thesis §9.6 and used by every production
		// etcd-raft deployment. See docs/event-log-architecture.md
		// § "PreVote and election timeout".
		PreVote: true,
		// CheckQuorum is PreVote's required companion for the DIRECTIONAL
		// partition case. etcd raft's PreVote grant logic allows peers to
		// grant a pre-vote at m.Term > r.Term even when they're hearing
		// from a leader — UNLESS CheckQuorum is on, in which case the
		// leader-lease check (electionElapsed < electionTimeout) short-
		// circuits the grant. Without CheckQuorum, scenario (b) of the
		// adversarial suite reveals the gap: the leader can't reach one
		// follower, that follower campaigns at a higher term, the other
		// followers grant the pre-vote (still at the old leader's term,
		// with the old leader reachable), and the old leader steps down.
		// With CheckQuorum, the other followers reject the pre-vote because
		// they've heard from the leader within the election window.
		//
		// CheckQuorum also lets a leader proactively step down if it loses
		// quorum (bounded by electionTimeout), which is the right posture
		// for a production database: a partitioned-away leader shouldn't
		// keep accepting reads indefinitely.
		CheckQuorum: true,
	}

	hs, _, err := n.storage.InitialState()
	if err != nil {
		n.logger.Error("initial state", zap.Error(err))
	}

	switch {
	case hs.Term > 0:
		n.logger.Info("restarting node", zap.Uint64("id", id))
		n.node = raft.RestartNode(c)
	case n.join:
		// Joining an existing cluster. RestartNode (not StartNode) reads the
		// empty InitialState and brings the node up with no configuration —
		// it learns its membership from the leader once the AddNode conf
		// change naming it commits. StartNode(c, ps) would instead bootstrap
		// a brand-new cluster from ps and split-brain against the one we mean
		// to join. The transport below is still seeded from ps so this node
		// can reach the existing members and bind its own listener.
		n.logger.Info("joining cluster", zap.Uint64("id", id))
		n.node = raft.RestartNode(c)
	default:
		n.logger.Info("starting node", zap.Uint64("id", id))
		n.node = raft.StartNode(c, ps)
	}

	if n.transportFactory == nil {
		// A wiring mistake, not a runtime condition: the composition root must
		// inject a transport (WithTransportFactory). Production passes one; the
		// db test suite registers a default. Fail loud rather than start a node
		// that can never reach a peer.
		panic("db: no transport factory configured — wire one with WithTransportFactory")
	}
	r := &httpTransportRaft{node: n.node}
	t := n.transportFactory(id, ps, n.logger, r, n.tlsInfo)
	if n.transportWrapper != nil {
		// Wrap once, before serveRaft starts driving the transport. The
		// wrapper returns a Transport that conforms to the same interface,
		// so downstream Start/Send/AddPeer/RemovePeer calls flow through
		// it transparently. Only the adversarial test suite sets this.
		t = n.transportWrapper(t)
	}
	n.transport = t

	go n.serveRaft()
	go n.serveChannels()
}

// applyConfChange applies a committed membership change (v1 or v2 wire form)
// to the raft node, mirrors the resulting ConfState into storage so it
// survives a restart (see Storage.ConfState — without it a restarted node
// reads an empty voter set from InitialState and can't participate), and
// updates the peer transport so this node starts or stops exchanging raft
// messages with the affected peer.
//
// ccCtx is the change's Context. For an add it carries the new peer's
// advertised URL (db.AddMember stashes it there); the transport needs the
// URL to dial the peer, and raft's ConfState replicates only node IDs, never
// addresses — so the URL must ride along on the conf change itself. The
// JointImplicit auto-leave that raft proposes once a joint configuration
// commits is a ConfChangeV2 with zero Changes, so it advances the ConfState
// out of the joint state while doing nothing to the transport.
//
// The membership API submits one add or one remove at a time, so a v2 change
// carries a single ConfChangeSingle and one shared Context. A hand-built
// multi-add v2 would share that one Context across every add, which can't
// express distinct URLs — not a concern for the paths we expose.
func (n *Raft) applyConfChange(cc raftpb.ConfChangeI, ccCtx []byte) {
	cs := n.node.ApplyConfChange(cc)
	n.storage.ConfState(cs)

	for _, ch := range cc.AsV2().Changes {
		// A node needs no transport entry for itself: skip self so applying
		// our own AddNode doesn't try to dial our own URL, and so a
		// self-removal doesn't tear down a peer that was never added.
		if ch.NodeID == n.id {
			continue
		}
		switch ch.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if len(ccCtx) == 0 {
				// No URL to dial — happens for the bootstrap conf changes a
				// node already knows from its static peer set, so the
				// transport is already wired. Nothing to do.
				continue
			}
			if err := n.transport.AddPeer(raft.Peer{ID: ch.NodeID, Context: ccCtx}); err != nil {
				n.logger.Error("conf change: add peer to transport",
					zap.Uint64("peer", ch.NodeID), zap.Error(err))
			}
		case raftpb.ConfChangeRemoveNode:
			n.transport.RemovePeer(ch.NodeID)
			// Drop the removed node's announced API URL so the
			// memberAPIURLs map doesn't accumulate stale entries across
			// the add/remove churn of rebalancing. Best-effort cleanup:
			// a stale entry is harmless (the node is gone from the
			// configuration, so it never surfaces in membership reads),
			// so a delete failure is logged, not fatal.
			if err := n.storage.DeleteMemberAPIURL(ch.NodeID); err != nil {
				n.logger.Error("conf change: delete member api url",
					zap.Uint64("peer", ch.NodeID), zap.Error(err))
			}
		}
	}
}

// memberStatus reports the current raft configuration as observed by this
// node: voters is the union of the incoming and (during a joint transition)
// outgoing voter sets, learners is the learner set, and joint is true while
// the configuration is still in the joint state (an outgoing config is
// present). db.AddMember / AddLearner / RemoveMember / PromoteMember poll this
// (via waitForMembership) to block until a change has fully taken effect
// locally — i.e. the target node is in the expected set (voter / learner /
// neither) AND the joint transition has completed (joint == false).
//
// Both maps are owned by the caller: Status() returns a Clone of the tracker
// config, so Voters.IDs() and the Learners map are fresh per call. learners is
// nil when there are none (a nil-map read is a safe miss).
func (n *Raft) memberStatus() (voters, learners map[uint64]struct{}, joint bool) {
	cfg := n.node.Status().Config
	return cfg.Voters.IDs(), cfg.Learners, len(cfg.Voters[1]) > 0
}

// memberView is one member's role and (leader-only) replication progress as
// observed by this node, for the GET /v1/membership read.
type memberView struct {
	id       uint64
	learner  bool
	match    uint64
	hasMatch bool
}

// membershipView returns a single consistent snapshot of this node's raft
// status for the membership read: the leader id, current term, commit index,
// whether this node is the leader, and one memberView per voter and learner.
//
// The per-member matched index is populated only when this node is the leader
// — etcd raft keeps a follower-progress tracker only on the leader (Status().
// Progress is empty elsewhere). The HTTP layer proxies GET /v1/membership to
// the leader so the answer always carries progress; a follower-built snapshot
// still reports roles correctly, just with hasMatch=false.
func (n *Raft) membershipView() (leaderID, term, commit uint64, isLeader bool, members []memberView) {
	if n.node == nil {
		return 0, 0, 0, false, nil
	}
	st := n.node.Status()

	add := func(id uint64, learner bool) {
		mv := memberView{id: id, learner: learner}
		if pr, ok := st.Progress[id]; ok {
			mv.match = pr.Match
			mv.hasMatch = true
		}
		members = append(members, mv)
	}
	for id := range st.Config.Voters.IDs() {
		add(id, false)
	}
	for id := range st.Config.Learners {
		add(id, true)
	}
	return st.Lead, st.Term, st.Commit, st.RaftState == raft.StateLeader, members
}

func (n *Raft) serveChannels() {
	defer close(n.serveChannelsDoneC)

	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	// raftStopOnce protects close(n.raftStopC) since both the inner proposeC
	// reader (when its channels close) and Close() (via closeC propagation)
	// can drive shutdown.
	var raftStopOnce sync.Once
	closeRaftStop := func() {
		raftStopOnce.Do(func() { close(n.raftStopC) })
	}

	go func() {
		for n.proposeC != nil && n.proposeConfC != nil {
			select {
			case prop, ok := <-n.proposeC:
				if !ok {
					n.proposeC = nil
				} else {
					n.logger.Debug("proposal being sent to state machine")
					// blocks until accepted by raft state machine
					err := n.node.Propose(context.TODO(), []byte(prop))
					if err != nil {
						n.raftErrorC <- err
					}
					n.logger.Debug("proposal accepted by state machine")
				}
			case cc, ok := <-n.proposeConfC:
				if !ok {
					n.proposeConfC = nil
				} else {
					// cc already carries Transition=JointImplicit and a
					// single add/remove change, built by db.AddMember /
					// db.RemoveMember. ProposeConfChange forwards it to the
					// leader (MsgProp) when this node is a follower, exactly
					// like a normal Propose. ConfChangeV2 has no ID field —
					// raft tracks the in-flight change by its entry index
					// (pendingConfIndex), so unlike the old v1 path there is
					// no application-assigned ID to set here.
					err := n.node.ProposeConfChange(context.Background(), cc)
					if err != nil {
						n.raftErrorC <- err
					}
				}
			case <-n.closeC:
				// Close() asked us to stop, even though proposeC is still
				// open. Drop our reference and let raftStopC be closed below.
				n.proposeC = nil
				n.proposeConfC = nil
			}
		}
		// client closed channel (or Close() asked us to stop); shutdown raft
		// if not already
		closeRaftStop()
	}()

	for {
		select {
		case <-ticker.C:
			n.node.Tick()
		case rd := <-n.node.Ready():
			status := n.node.Status()
			isLeader := status.RaftState == raft.StateLeader
			n.leaderState.SetLeader(isLeader)
			// Update the observed leader ID on every Ready. SetLeaderID
			// no-ops when the value is unchanged; when it changes it
			// dispatches to any subscribers (db.DB's leader-change
			// watcher uses this to fail-fast in-flight Propose waiters
			// stamped under the old leader).
			n.leaderState.SetLeaderID(status.Lead)

			if n.metrics != nil {
				n.metrics.SetLeader(isLeader)
			}

			// Hand each confirmed read state back to its waiting
			// ReadIndex caller. A ReadState carries the request-context
			// token we passed to node.ReadIndex plus the commit index at
			// which the leader confirmed quorum; the caller then waits
			// for AppliedIndex to reach that index before reading. This
			// is independent of Save/apply, so dispatch it up front.
			n.dispatchReadStates(rd.ReadStates)

			// Save persists the new entries durably. It does NOT apply them
			// to bucket state — that happens via ApplyCommitted below, only
			// on rd.CommittedEntries. Calling Save with rd.Entries and apply
			// with rd.CommittedEntries is required by the etcd raft contract;
			// rd.Entries may include uncommitted entries on a follower, so
			// applying them to application state would diverge the cluster.
			//
			// Save errors are unrecoverable. Raft's invariants require that
			// every entry returned from Ready be durably persisted before
			// Advance is called: continuing past a failed Save and still
			// calling Advance lets raft forget its unstable buffer while
			// Storage lacks the corresponding entries, which panics the
			// internal raft loop ("committed > lastIndex") the moment any
			// new commit notification arrives. Treating Save failure as
			// "this node is done" is the safe posture: stop the Ready loop
			// (no more Send, ApplyCommitted, Advance), let peers notice the
			// silence and re-elect around this node, and let Close tear
			// down the transport and raft.Node.
			//
			// The send to raftErrorC is non-blocking so an unread error
			// channel (raft_test's direct-constructed Raft) doesn't deadlock
			// the return path. Production callers (db.DB) drain ErrorC so
			// the send always lands immediately.
			err := n.storage.Save(rd.HardState, rd.Entries, rd.Snapshot)
			if err != nil {
				n.logger.Error("storage save", zap.Error(err))
				select {
				case n.raftErrorC <- err:
				default:
				}
				return
			}
			n.transport.Send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				n.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				// Apply MUST complete before Advance() per the etcd raft
				// contract. Apply errors are crash-fatal: continuing past a
				// half-applied entry diverges the state machine, retrying
				// risks masking programming bugs, and disk failures leave
				// the node inconsistent in a way that's worse to keep
				// running than to crash. (Same posture etcd's raftexample
				// takes.)
				applyStart := time.Now()
				if err := n.storage.ApplyCommitted(entry); err != nil {
					n.logger.Fatal("apply committed entry", zap.Uint64("index", entry.Index), zap.Error(err))
				}
				if n.metrics != nil {
					n.metrics.EntryApplied(entry.Index, time.Since(applyStart))
				}
				// Fire the apply notifier once the storage apply has
				// succeeded, so blocking db.Propose unblocks promptly. The
				// notifier is no-op if nil (raft_test path) or if the
				// proposal's RequestID is 0 (system-internal proposers
				// or pre-PR2 entries).
				if n.applyNotifier != nil && entry.Type == raftpb.EntryNormal && entry.Data != nil {
					n.applyNotifier(entry.Data)
				}
				switch entry.Type {
				case raftpb.EntryConfChangeV2:
					// The normal membership path. db.AddMember / db.RemoveMember
					// propose ConfChangeV2 entries (joint consensus), and raft
					// itself proposes a zero-change ConfChangeV2 to leave the
					// joint configuration once it commits (JointImplicit
					// auto-leave). Both flow through here.
					var cc raftpb.ConfChangeV2
					if err := cc.Unmarshal(entry.Data); err != nil {
						n.raftErrorC <- err
						break
					}
					n.applyConfChange(cc, cc.Context)
				case raftpb.EntryConfChange:
					// Backward compatibility: a v1 ConfChange can only appear
					// in a log written by a pre-joint-consensus binary, since
					// this node now proposes v2 exclusively. etcd raft still
					// applies v1 (ConfChangeI.AsV2 promotes it), so replaying it
					// keeps an upgraded node consistent with what the old leader
					// committed. See docs/operations/membership.md.
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						n.raftErrorC <- err
						break
					}
					n.applyConfChange(cc, cc.Context)
				}
			}

			// AppliedIndex advanced this iteration (every committed entry
			// bumps it, regardless of type), so wake any LinearizableRead
			// blocked on AppliedIndex >= its ReadIndex. Fired once per
			// Ready rather than per entry to keep the broadcast cheap.
			if n.appliedIndexNotifier != nil && len(rd.CommittedEntries) > 0 {
				n.appliedIndexNotifier()
			}

			if n.metrics != nil {
				fi, _ := n.storage.FirstIndex()
				li, _ := n.storage.LastIndex()
				n.metrics.SetIndexRange(fi, li)
				// Surface degraded configs (persisted but not buildable on
				// this node — usually a missing ${VAR} secret). Optional
				// interface so only real storage reports it; the in-memory
				// test double doesn't implement it.
				if r, ok := n.storage.(configBuildErrorReporter); ok {
					n.metrics.SetConfigBuildErrors(r.ConfigBuildErrorCount())
				}
			}

			// Storage invariant: P_local == R_local. See
			// docs/event-log-architecture.md § "The storage invariant".
			// On violation the cluster has compacted past this node's
			// recoverable window (almost always via an InstallSnapshot
			// that advanced the raft side without backfilling the event
			// log) and this node cannot safely keep serving reads or
			// voting — it must exit and be rebuilt. v1 is fail-fast per
			// the doc; v2 will enter a streaming catch-up mode in
			// place of the Fatal.
			n.checkStorageInvariant()

			// If configured, trim the raft log up to a safe point so we
			// don't retain consensus transport indefinitely. The
			// permanent event log keeps the events forever; the raft
			// log only exists to move entries between peers and to
			// satisfy the `appliedIndex` replay window.
			n.maybeCompact()

			n.node.Advance()
		case err := <-n.transport.GetErrorC():
			n.writeError(err)
			return
		case <-n.raftStopC:
			return
		case <-n.closeC:
			closeRaftStop()
			return
		}
	}
}

func (n *Raft) writeError(err error) {
	n.stopTransport()
	n.raftErrorC <- err
	close(n.raftErrorC)
	n.node.Stop()
}

func (n *Raft) stopTransport() {
	if n.transport != nil {
		n.transport.Stop()
	}
	close(n.transportStopC)
	<-n.transportDoneC
}

// Close fully tears down the Raft instance: it signals serveChannels and its
// inner proposeC reader to exit, waits for serveChannels to actually stop
// (which guarantees Storage.Save is no longer running), stops the transport,
// and stops the underlying etcd raft.Node. It is safe to call Close more than
// once.
//
// Stopping n.node is critical to prevent goroutine leaks: without it, every
// Raft we create leaks the etcd raft.Node's internal `(*node).run` goroutine.
// Across many test iterations the leaked goroutines consume enough CPU that
// subsequent tests time out.
func (n *Raft) Close() error {
	n.closeOnce.Do(func() {
		close(n.closeC)
		<-n.serveChannelsDoneC
		n.stopTransport()
		if n.node != nil {
			n.node.Stop()
		}
	})
	return nil
}

func (n *Raft) serveRaft() {
	err := n.transport.Start(n.transportStopC)
	select {
	case <-n.transportStopC:
	default:
		log.Fatalf("transport stopped: (%v)", err)
	}
	close(n.transportDoneC)
}

// Leader returns the raft node ID that this Raft believes is the current
// leader, or 0 if no leader is known. Reads through to etcd raft's Status()
// snapshot, which is concurrency-safe (served via raft.node's status channel).
//
// Used by multi-node tests as the cheapest available "is the cluster ready"
// signal: poll until every node reports the same non-zero leader ID, then
// proceed with proposes. Production code shouldn't need this.
func (n *Raft) Leader() uint64 {
	if n.node == nil {
		return 0
	}
	return n.node.Status().Lead
}

// setCompactionPressure records whether the node is under disk pressure. While
// true, maybeCompact treats it as a trigger limb (compact every Ready
// iteration, subject to the usual safety constraints) so the node frees
// raft-log disk sooner. Called from db's disk-usage watcher goroutine.
func (n *Raft) setCompactionPressure(on bool) {
	n.compactionPressure.Store(on)
}

// transferLeadership asks etcd raft to hand leadership to transferee. Only
// meaningful on the current leader: raft first lets the target catch up on
// the log, then sends it MsgTimeoutNow to start an immediate election; the
// attempt silently expires after an election timeout if the target can't
// catch up or is unreachable. Fire-and-forget by design — the caller
// (db.maybeTransferLeadership, moving leadership off a disk-constrained
// node) observes the outcome through the normal leader-change machinery and
// retries on a later cycle if leadership didn't move.
func (n *Raft) transferLeadership(transferee uint64) {
	if n.node == nil {
		return
	}
	n.node.TransferLeadership(context.Background(), n.node.Status().Lead, transferee)
}

// ReadIndex performs the etcd-raft ReadIndex protocol and returns the raft
// log index at which a linearizable read may be served. The leader confirms
// it still holds quorum (a heartbeat round-trip, coalesced across concurrent
// requests by etcd-raft) before replying; on a follower the request is
// forwarded to the leader transparently. The returned index is the commit
// index observed at confirmation time — the caller must wait for its local
// AppliedIndex to reach it before reading (see db.LinearizableRead).
//
// ReadIndex blocks until the matching ReadState surfaces on a Ready, ctx is
// canceled, or the Raft is closed. If no leader can be reached (e.g. this
// node is partitioned into a minority), no ReadState ever comes back and the
// call returns ctx.Err() once ctx fires — that is the mechanism by which a
// partitioned node refuses to serve a stale linearizable read.
//
// The request-context token handed to node.ReadIndex is a per-call monotonic
// counter, so concurrent ReadIndex calls each match only their own
// ReadState. etcd-raft may still coalesce the underlying heartbeat round-trip
// across them, so the per-read cost amortizes under load.
func (n *Raft) ReadIndex(ctx context.Context) (uint64, error) {
	rctx := make([]byte, 8)
	binary.BigEndian.PutUint64(rctx, n.nextReadReq.Add(1))
	key := string(rctx)

	// Buffered cap 1 so dispatchReadStates never blocks the Ready loop:
	// the single expected send always fits, and a late duplicate (raft
	// can echo a ReadState more than once) falls through its default.
	ack := make(chan uint64, 1)
	n.readMu.Lock()
	n.readWaiters[key] = ack
	n.readMu.Unlock()
	defer func() {
		n.readMu.Lock()
		delete(n.readWaiters, key)
		n.readMu.Unlock()
	}()

	// etcd-raft DROPS a MsgReadIndex that arrives while this node sees no
	// leader (startup before the first election, or the gap during a
	// re-election) rather than queuing it — the request is silently lost
	// and no ReadState ever comes back. So re-issue on a timer until the
	// ReadState surfaces or ctx fires. The same token is reused across
	// attempts, so a leader that answers more than once is harmless: the
	// buffered ack keeps the first index and drops the rest. This mirrors
	// etcd's own linearizableReadLoop retry.
	retry := time.NewTimer(0) // fire immediately for the first attempt
	defer retry.Stop()
	for {
		select {
		case <-retry.C:
			if err := n.node.ReadIndex(ctx, rctx); err != nil {
				return 0, err
			}
			retry.Reset(n.readIndexRetryInterval())
		case idx := <-ack:
			return idx, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-n.closeC:
			return 0, ErrClosed
		}
	}
}

// readIndexRetryInterval is how long ReadIndex waits between re-issuing a
// dropped read request. One election timeout (ElectionTick × tickInterval) is
// the natural cadence: that's the timescale on which a missing leader
// resolves, so retrying faster just spams the dropped-message path while
// retrying slower needlessly stalls reads during a brief election. Floored so
// a tiny test tick doesn't turn this into a hot loop.
func (n *Raft) readIndexRetryInterval() time.Duration {
	return max(raftElectionTicks*n.tickInterval, 5*time.Millisecond)
}

// dispatchReadStates routes each confirmed ReadState from a Ready back to the
// ReadIndex caller that registered the matching request-context token. A
// ReadState whose token has no waiter (the caller already returned via ctx,
// or raft echoed a duplicate) is dropped. Called from the Ready loop only.
func (n *Raft) dispatchReadStates(states []raft.ReadState) {
	if len(states) == 0 {
		return
	}
	n.readMu.Lock()
	defer n.readMu.Unlock()
	for _, rs := range states {
		ack, ok := n.readWaiters[string(rs.RequestCtx)]
		if !ok {
			continue
		}
		select {
		case ack <- rs.Index:
		default:
		}
	}
}

// checkStorageInvariant enforces P_local == R_local after every Ready
// iteration. See docs/event-log-architecture.md § "The storage
// invariant" for the full rationale. Briefly:
//
//   - P_local is storage.EventIndex() — the highest raft index this
//     node has durably written to its permanent event log.
//   - R_local is storage.AppliedIndex() — the highest raft index this
//     node has acknowledged as applied to raft.
//
// Under normal operation ApplyCommitted writes the event and bumps
// appliedIndex atomically per entry, so the two stay equal. The only
// way they diverge is an InstallSnapshot that advanced appliedIndex
// past the permanent event log highwatermark — i.e., the cluster's
// raft log has been compacted past a gap this node can't fill. v1
// handles this by fatal-exiting with a pointer to the rebuild
// runbook; see docs/operations/rebuild.md.
func (n *Raft) checkStorageInvariant() {
	p := n.storage.EventIndex()
	r := n.storage.AppliedIndex()
	if p == r {
		return
	}
	n.logger.Fatal(
		"storage invariant violation: permanent event log is behind raft applied index. "+
			"The cluster has compacted past this node's recovery point. "+
			"Run the rebuild procedure at docs/operations/rebuild.md.",
		zap.Uint64("eventIndex", p),
		zap.Uint64("appliedIndex", r),
		zap.Uint64("gap", r-p),
	)
}

// maybeCompact trims the raft log up to a safe point when either
// compactMaxSize or compactMaxAge is exceeded — the "10GB or 1hr"
// policy described in docs/event-log-architecture.md § "Compaction
// policy". Safety constraints from the same doc:
//
//   - Compact point must be ≤ this node's permanent event log
//     highwatermark (EventIndex), so we never forget a raft entry
//     whose corresponding event isn't already durable here.
//   - Compact point must be ≤ AppliedIndex, the normal raft contract.
//   - We leave a small buffer below appliedIndex so a follower that
//     briefly lagged can still catch up via AppendEntries instead of
//     a more expensive snapshot install.
//
// The quorum-graduated constraint named in the doc isn't implemented
// for v1 — etcd raft doesn't expose peer-wise applied indices
// uniformly across versions, and a leader-only compact-point check
// has the same safety envelope for single-leader writes. Followers
// that fall behind still receive an InstallSnapshot if the leader's
// log has moved past them; that's the intended shape.
func (n *Raft) maybeCompact() {
	// Disk pressure is an independent trigger limb: it fires even when both
	// the size and age limbs are disabled, so a node under disk pressure
	// still compacts whatever is safe to free space. The compact-point
	// safety constraints below are unchanged, so this can never compact past
	// applied/event-log highwater.
	pressure := n.compactionPressure.Load()
	if n.compactMaxSize == 0 && n.compactMaxAge == 0 && !pressure {
		return
	}

	triggered := false
	var reason string
	if pressure {
		triggered = true
		reason = "disk-pressure"
	}
	if !triggered && n.compactMaxSize > 0 {
		size, err := n.storage.RaftLogApproxSize()
		if err == nil && size >= n.compactMaxSize {
			triggered = true
			reason = "size"
		}
	}
	if !triggered && n.compactMaxAge > 0 {
		if time.Since(n.lastCompactTime) >= n.compactMaxAge {
			triggered = true
			reason = "age"
		}
	}
	if !triggered {
		return
	}

	applied := n.storage.AppliedIndex()
	if applied == 0 {
		return
	}

	// Hold back from the very tip of applied so brief-lag followers can
	// still use AppendEntries instead of the costlier InstallSnapshot
	// path. Eight entries is arbitrary — small enough that compaction
	// still buys real disk savings, large enough that a short network
	// blip doesn't force a snapshot install.
	const safetyBuffer = uint64(8)
	if applied <= safetyBuffer {
		return
	}
	compactTo := applied - safetyBuffer

	// Never compact past the permanent event log — if we do, a
	// follower could receive an InstallSnapshot advancing raft's
	// state past its event log and trip the storage invariant.
	if eventIdx := n.storage.EventIndex(); compactTo > eventIdx {
		compactTo = eventIdx
	}
	if compactTo <= n.lastCompactedIndex.Load() {
		return
	}

	// Take a metadata snapshot first so raft has something to ship to
	// followers that fall behind the new first index.
	if _, err := n.storage.CreateSnapshot(compactTo, nil); err != nil {
		n.logger.Warn("create snapshot for compaction", zap.Uint64("compactTo", compactTo), zap.Error(err))
		return
	}
	if err := n.storage.Compact(compactTo); err != nil {
		n.logger.Warn("compact raft log", zap.Uint64("compactTo", compactTo), zap.Error(err))
		return
	}
	n.lastCompactedIndex.Store(compactTo)
	n.lastCompactTime = time.Now()
	n.logger.Debug("raft log compacted", zap.Uint64("compactTo", compactTo), zap.String("reason", reason))
}

// processSnapshot installs a snapshot delivered by the raft Ready
// loop — i.e. a leader has told us via InstallSnapshot that we need
// to catch up on metadata state that the leader's raft log no longer
// carries. The restore only touches bbolt (metadata); the permanent
// event log is NOT in the snapshot because events are too large to
// ship through raft.
//
// A successful restore leaves appliedIndex at snap.Metadata.Index. If
// this node's permanent event log is behind that index, the Ready
// loop's checkStorageInvariant fatal-exits at the end of the current
// iteration — by design. See docs/event-log-architecture.md §
// "Severe lag — v1 manual rebuild". A RestoreSnapshot error here is
// also fatal; a half-applied snapshot leaves the node in a hybrid
// state that is worse than crashing and letting an operator rebuild.
func (n *Raft) processSnapshot(snap raftpb.Snapshot) {
	if err := n.storage.RestoreSnapshot(snap); err != nil {
		n.logger.Fatal("restore snapshot failed",
			zap.Uint64("snapIndex", snap.Metadata.Index),
			zap.Uint64("snapTerm", snap.Metadata.Term),
			zap.Error(err),
		)
	}
}

type httpTransportRaft struct {
	node raft.Node
}

// The next four methods implement the Raft interface in the rafthttp package needed for rafthttp.Transport
func (n *httpTransportRaft) Process(ctx context.Context, m raftpb.Message) error {
	return n.node.Step(ctx, m)
}
func (n *httpTransportRaft) IsIDRemoved(id uint64) bool  { return false }
func (n *httpTransportRaft) ReportUnreachable(id uint64) { n.node.ReportUnreachable(id) }
func (n *httpTransportRaft) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.node.ReportSnapshot(id, status)
}
