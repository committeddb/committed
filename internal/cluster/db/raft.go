package db

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/philborlin/committed/internal/cluster/db/httptransport"
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
	commitC    chan []byte
	raftErrorC chan<- error
	raftStopC    chan struct{}
	id           uint64
	leaderState  *LeaderState
	tickInterval time.Duration

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

	logger *zap.Logger
}

func NewRaft(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange, opts ...Option) (<-chan []byte, <-chan error, *Raft) {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newRaftWithOptions(id, ps, s, proposeC, proposeConfC, nil, cfg)
}

func newRaftWithOptions(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange, applyNotifier func(data []byte), cfg options) (<-chan []byte, <-chan error, *Raft) {
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
		storage:            s,
		applyNotifier:      applyNotifier,
		transportStopC:     make(chan struct{}),
		transportDoneC:     make(chan struct{}),
		closeC:             make(chan struct{}),
		serveChannelsDoneC: make(chan struct{}),

		logger: zap.NewExample(),
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
	}

	hs, _, err := n.storage.InitialState()
	if err != nil {
		// Send to the error channel
		fmt.Printf("[raft] %v\n", err)
	}

	if hs.Term > 0 {
		fmt.Printf("[raft] Restarting Node %d\n", id)
		n.node = raft.RestartNode(c)
	} else {
		fmt.Printf("[raft] Starting Node %d\n", id)
		n.node = raft.StartNode(c, ps)
	}

	r := &httpTransportRaft{node: n.node}
	n.transport = httptransport.New(id, ps, n.logger, r)

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
					fmt.Printf("[raft] proposal being sent to state machine...\n")
					// blocks until accepted by raft state machine
					err := n.node.Propose(context.TODO(), []byte(prop))
					if err != nil {
						n.raftErrorC <- err
					}
					fmt.Printf("[raft] ...proposal accepted by state machine\n")
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
			// fmt.Printf("[raft] ready and about to save to storage\n")
			n.leaderState.SetLeader(n.node.Status().RaftState == raft.StateLeader)

			// Save persists the new entries durably. It does NOT apply them
			// to bucket state — that happens via ApplyCommitted below, only
			// on rd.CommittedEntries. Calling Save with rd.Entries and apply
			// with rd.CommittedEntries is required by the etcd raft contract;
			// rd.Entries may include uncommitted entries on a follower, so
			// applying them to application state would diverge the cluster.
			err := n.storage.Save(rd.HardState, rd.Entries, rd.Snapshot)
			if err != nil {
				fmt.Printf("[raft] storage save: %v\n", err)
				n.raftErrorC <- err
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
				if err := n.storage.ApplyCommitted(entry); err != nil {
					n.logger.Fatal("apply committed entry", zap.Uint64("index", entry.Index), zap.Error(err))
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

func (n *Raft) processSnapshot(ms raftpb.Snapshot) {
	// TODO: Snapshot install must:
	//   1. Restore application state (BoltDB buckets, time series) from
	//      snap.Data.
	//   2. Bump Storage's appliedIndex to snap.Metadata.Index — otherwise
	//      ApplyCommitted will re-process every committed entry from
	//      firstIndex up to that point on the next Ready cycle.
	// Today snapshots are a no-op (single-node, no follower bringup),
	// so we get away with leaving this empty. ApplyCommitted's
	// "skip if entry.Index <= appliedIndex" guard means once snapshot
	// install lands, the only correctness-critical step here is to set
	// appliedIndex to snap.Metadata.Index.
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
