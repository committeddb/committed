package db

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/philborlin/committed/internal/cluster/db/httptransport"
	"github.com/philborlin/committed/internal/cluster/metrics"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type Raft struct {
	proposeC     <-chan []byte            // proposed messages
	proposeConfC <-chan raftpb.ConfChange // proposed cluster config changes
	// commitC is the bidirectional channel that committed proposal data is
	// sent to. Stored as a bidirectional chan (rather than chan<-) so Close
	// can close it after serveChannels has stopped, letting consumers like
	// EatCommitC exit cleanly instead of leaking forever.
	commitC      chan []byte
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
	// to re-compact to the same point.
	lastCompactedIndex uint64
	// lastCompactTime is the wall-clock moment of the most recent
	// compaction (or startRaft time if no compaction has happened).
	// The age limb of the policy fires when (time.Now() -
	// lastCompactTime) exceeds compactMaxAge. Mutated only from the
	// serveChannels goroutine.
	lastCompactTime time.Time

	node    raft.Node
	storage Storage

	// applyNotifier is invoked after each successful Storage.ApplyCommitted
	// call with the raw entry data. db.New supplies db.notifyApplied here
	// so blocking db.Propose can release waiters once their proposal has
	// been applied. nil disables the callback (used by raft_test which
	// constructs Raft directly without a db.DB).
	applyNotifier func(data []byte)

	transport      Transport
	transportStopC chan struct{} // signals http transport to shutdown
	transportDoneC chan struct{} // signals http transport shutdown complete

	// transportWrapper is captured from the options in newRaftWithOptions
	// so startRaft can apply it after constructing the HttpTransport. nil
	// in all production paths; set by the adversarial test suite via
	// WithTransportWrapperForTest to inject a fault-injection layer
	// around the real transport.
	transportWrapper func(Transport) Transport

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

func NewRaft(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange, opts ...Option) (<-chan []byte, <-chan error, *Raft) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newRaftWithOptions(id, ps, s, proposeC, proposeConfC, nil, cfg.logger, cfg)
}

func newRaftWithOptions(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange, applyNotifier func(data []byte), logger *zap.Logger, cfg options) (<-chan []byte, <-chan error, *Raft) {
	commitC := make(chan []byte)
	errorC := make(chan error)

	n := &Raft{
		id:                 id,
		proposeC:           proposeC,
		proposeConfC:       proposeConfC,
		commitC:            commitC,
		raftErrorC:         errorC,
		raftStopC:          make(chan struct{}),
		leaderState:        NewLeaderState(false),
		tickInterval:       cfg.tickInterval,
		compactMaxSize:     cfg.compactMaxSize,
		compactMaxAge:      cfg.compactMaxAge,
		lastCompactTime:    time.Now(),
		storage:            s,
		applyNotifier:      applyNotifier,
		transportStopC:     make(chan struct{}),
		transportDoneC:     make(chan struct{}),
		transportWrapper:   cfg.transportWrapper,
		closeC:             make(chan struct{}),
		serveChannelsDoneC: make(chan struct{}),

		logger:  logger,
		metrics: cfg.metrics,
	}
	// startRaft itself doesn't block — it sets up the raft.Node and transport
	// then spawns serveRaft/serveChannels as their own goroutines. Calling it
	// synchronously here guarantees that n.transport and n.node are non-nil
	// and the worker goroutines have been launched by the time NewRaft
	// returns, so a fast caller-side Close() can't race with goroutine
	// startup.
	n.startRaft(id, ps)

	return commitC, errorC, n
}

func (n *Raft) startRaft(id uint64, ps []raft.Peer) {
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              10,
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

	if hs.Term > 0 {
		n.logger.Info("restarting node", zap.Uint64("id", id))
		n.node = raft.RestartNode(c)
	} else {
		n.logger.Info("starting node", zap.Uint64("id", id))
		n.node = raft.StartNode(c, ps)
	}

	r := &httpTransportRaft{node: n.node}
	var t Transport = httptransport.New(id, ps, n.logger, r)
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
		confChangeCount := uint64(0)

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
					confChangeCount++
					cc.ID = confChangeCount
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
			isLeader := n.node.Status().RaftState == raft.StateLeader
			n.leaderState.SetLeader(isLeader)

			if n.metrics != nil {
				n.metrics.SetLeader(isLeader)
			}

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
				// Fire the apply notifier after the storage apply has
				// succeeded but before processCommittedEntry's send to
				// commitC, so blocking db.Propose unblocks promptly. The
				// notifier is no-op if nil (raft_test path) or if the
				// proposal's RequestID is 0 (system-internal proposers
				// or pre-PR2 entries).
				if n.applyNotifier != nil && entry.Type == raftpb.EntryNormal && entry.Data != nil {
					n.applyNotifier(entry.Data)
				}
				n.processCommittedEntry(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					err := cc.Unmarshal(entry.Data)
					if err != nil {
						n.raftErrorC <- err
					}
					n.node.ApplyConfChange(cc)
					// Do we need to update the confState or will a snapshop be sent above?
					// c := n.node.ApplyConfChange(cc)
					// n.storage.ConfState(c)
				}
			}

			if n.metrics != nil {
				fi, _ := n.storage.FirstIndex()
				li, _ := n.storage.LastIndex()
				n.metrics.SetIndexRange(fi, li)
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
		// case err := <-n.transport.ErrorC:
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
// stops the underlying etcd raft.Node, and closes commitC so any consumer
// exits cleanly. It is safe to call Close more than once.
//
// Stopping n.node and closing commitC are critical to prevent goroutine
// leaks: without them, every Raft we create leaks the etcd raft.Node's
// internal `(*node).run` goroutine and any consumer of commitC (e.g. tests
// using DB.EatCommitC). Across many test iterations the leaked goroutines
// consume enough CPU that subsequent tests time out.
func (n *Raft) Close() error {
	n.closeOnce.Do(func() {
		close(n.closeC)
		<-n.serveChannelsDoneC
		n.stopTransport()
		if n.node != nil {
			n.node.Stop()
		}
		close(n.commitC)
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
	if n.compactMaxSize == 0 && n.compactMaxAge == 0 {
		return
	}

	triggered := false
	var reason string
	if n.compactMaxSize > 0 {
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
	if compactTo <= n.lastCompactedIndex {
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
	n.lastCompactedIndex = compactTo
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

func (n *Raft) processCommittedEntry(e raftpb.Entry) {
	if e.Type == raftpb.EntryNormal && e.Data != nil {
		n.commitC <- e.Data
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
