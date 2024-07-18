package db

import (
	"context"
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

type Peers map[uint64]string

func newRaft(id uint64, peers Peers, s Storage, proposeC <-chan []byte, proposeConfC <-chan raftpb.ConfChange) (<-chan []byte, <-chan error) {
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
	go n.startRaft(id, peers)

	return commitC, errorC
}

func (n *node) startRaft(id uint64, peers Peers) {
	rpeers := make([]raft.Peer, len(peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i)}
	}
	c := &raft.Config{
		ID:                        uint64(id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   n.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	snap, err := n.storage.Snapshot()
	if err != nil {
		// TODO These should publish to the errorC
		log.Fatalf("cannot get snapshot (%v)", err)
	}

	confState := snap.Metadata.ConfState
	if len(confState.Voters) == 0 {
		n.node = raft.RestartNode(c)
	} else {
		n.node = raft.StartNode(c, rpeers)
	}

	n.transport = httptransport.New(uint64(id), httptransport.Peers(peers), n.logger, n)

	// n.transport.Start()
	// for i := range peers {
	// 	if i+1 != id {
	// 		n.transport.AddPeer(types.ID(i+1), []string{peers[i]})
	// 	}
	// }

	// go n.serveRaft(id, peers)
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
					n.node.Propose(context.TODO(), []byte(prop))
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
	n.transport.Stop()
	close(n.transportStopC)
	<-n.transportDoneC
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

// func (n *node) serveRaft(id int, peers []string) {
// 	url, err := url.Parse(peers[id-1])
// 	if err != nil {
// 		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
// 	}

// 	ln, err := newStoppableListener(url.Host, n.transportStopC)
// 	if err != nil {
// 		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
// 	}

// 	err = (&http.Server{Handler: n.transport.Handler()}).Serve(ln)
// 	select {
// 	case <-n.transportStopC:
// 	default:
// 		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
// 	}
// 	close(n.transportDoneC)
// }

func (n *node) processSnapshot(ms raftpb.Snapshot) {
	// Nothing to do yet
}

func (n *node) processCommittedEntry(e raftpb.Entry) {
	if e.Type == raftpb.EntryNormal {
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
