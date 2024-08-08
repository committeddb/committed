package db

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/philborlin/committed/internal/cluster/db/httptransport"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type node struct {
	proposeC     <-chan []byte            // proposed messages
	proposeConfC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC      chan<- []byte            // when a message is committed it is sent here
	raftErrorC   chan<- error             // errors from raft session
	raftStopC    chan struct{}

	node    raft.Node
	storage Storage

	// transport      *rafthttp.Transport
	transport      Transport
	transportStopC chan struct{} // signals http transport to shutdown
	transportDoneC chan struct{} // signals http transport shutdown complete

	logger *zap.Logger
}

func NewRaft(id uint64, ps []raft.Peer, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange) (<-chan []byte, <-chan error, io.Closer) {
	commitC := make(chan []byte)
	errorC := make(chan error)

	n := &node{
		proposeC:       proposeC,
		proposeConfC:   proposeConfC,
		commitC:        commitC,
		raftErrorC:     errorC,
		raftStopC:      make(chan struct{}),
		storage:        s,
		transportStopC: make(chan struct{}),
		transportDoneC: make(chan struct{}),

		logger: zap.NewExample(),
	}
	go n.startRaft(id, ps)

	return commitC, errorC, n
}

func (n *node) startRaft(id uint64, ps []raft.Peer) {
	c := &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	hs, _, err := n.storage.InitialState()
	if err != nil {
		// Send to the error channel
		fmt.Println(err)
	}

	if hs.Term > 0 {
		fmt.Printf("Restarting Node %d\n", id)
		n.node = raft.RestartNode(c)
	} else {
		fmt.Printf("Starting Node %d\n", id)
		n.node = raft.StartNode(c, ps)
	}

	n.transport = httptransport.New(id, ps, n.logger, n)

	go n.serveRaft()
	go n.serveChannels()
}

func (n *node) serveChannels() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		confChangeCount := uint64(0)

		for n.proposeC != nil && n.proposeConfC != nil {
			select {
			case prop, ok := <-n.proposeC:
				if !ok {
					n.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					fmt.Printf("[raft] proposal being sent to state machine...\n")
					n.node.Propose(context.TODO(), []byte(prop))
					fmt.Printf("[raft] ...proposal accepted by state machine\n")
				}

			case cc, ok := <-n.proposeConfC:
				if !ok {
					n.proposeConfC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					n.node.ProposeConfChange(context.Background(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(n.raftStopC)
	}()

	for {
		select {
		case <-ticker.C:
			n.node.Tick()
		case rd := <-n.node.Ready():
			fmt.Printf("[raft] ready and about to save to storage\n")
			n.storage.Save(rd.HardState, rd.Entries, rd.Snapshot)
			n.transport.Send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				n.processSnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				n.processCommittedEntry(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
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
		}
	}
}

func (n *node) writeError(err error) {
	n.stopTransport()
	n.raftErrorC <- err
	close(n.raftErrorC)
	n.node.Stop()
}

func (n *node) stopTransport() {
	if n.transport != nil {
		n.transport.Stop()
	}
	close(n.transportStopC)
	<-n.transportDoneC
}

func (n *node) Close() error {
	n.stopTransport()
	return nil
}

func (n *node) serveRaft() {
	err := n.transport.Start(n.transportStopC)
	select {
	case <-n.transportStopC:
	default:
		log.Fatalf("transport stopped: (%v)", err)
	}
	close(n.transportDoneC)
}

func (n *node) processSnapshot(ms raftpb.Snapshot) {
	// Nothing to do yet
}

func (n *node) processCommittedEntry(e raftpb.Entry) {
	if e.Type == raftpb.EntryNormal && e.Data != nil {
		n.commitC <- e.Data
	}
}

// The next four methods implement the Raft interface in the rafthttp package needed for rafthttp.Transport
func (n *node) Process(ctx context.Context, m raftpb.Message) error {
	return n.node.Step(ctx, m)
}
func (n *node) IsIDRemoved(id uint64) bool  { return false }
func (n *node) ReportUnreachable(id uint64) { n.node.ReportUnreachable(id) }
func (n *node) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.node.ReportSnapshot(id, status)
}
