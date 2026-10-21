package db

import (
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// The local-storage side of raft's AsyncStorageWrites. The Ready loop
// (serveChannels) routes raft's local messages here and keeps ticking and
// sending; the append goroutine writes to disk, and the apply goroutine
// applies. Each local message carries responses that raft must receive only
// once its write is done: deliver hands them on.

// runAppend is the append goroutine: it persists raft's MsgStorageAppends in
// order — every one queued since the last write as one write — then
// delivers their responses, in order. Between writes it makes any raft-log
// cut a compaction prepared; the cut and Save's appends share the entry log
// lock-free, so both run here.
func (n *Raft) runAppend() {
	defer close(n.appendDoneC)
	for {
		msgs, ok := n.appendQ.pop()
		if !ok {
			return
		}
		if len(msgs) > 0 {
			if !n.persist(msgs) {
				return
			}
			for _, m := range msgs {
				n.deliver(m.GetResponses())
			}
		}
		n.finishCompaction()
	}
}

// persist writes MsgStorageAppends durably: their entries, their latest
// HardState (raft sets the fields only when the HardState changed) and a
// snapshot (whose message pop takes alone). It returns false when this node
// must stop: the write failed, or the node is closing.
//
// Save errors are unrecoverable. Raft must never receive the responses of a
// write that did not happen: a follower would acknowledge entries it lacks,
// and raft would drop entries from its unstable log that Storage does not
// hold. Treating a failed Save as "this node is done" is the safe posture
// (failStorage).
func (n *Raft) persist(msgs []*raftpb.Message) bool {
	hs, ents := mergeAppends(msgs)
	if snap := msgs[0].GetSnapshot(); hasSnapshot(msgs[0]) {
		installed := false
		// A snapshot install replaces bbolt, and its catch-up appends to the
		// event log: neither may race an apply. Raft hands out no committed
		// entries while a snapshot is in progress, so this waits only for
		// batches queued before it.
		if !n.applyQ.exclusive(func() { installed = n.installSnapshot(hs, ents, snap) }) || !installed {
			return false
		}
	} else if err := n.storage.Save(hs, ents, &raftpb.Snapshot{}); err != nil {
		n.failStorage(err)
		return false
	}
	if !raft.IsEmptyHardState(hs) {
		n.durable.advance(hs.GetCommit())
	}
	return true
}

// installSnapshot saves and installs a snapshot raft received. A snapshot past
// this node's event log cannot be saved (the event log would be left with a
// permanent gap): the log is filled from a peer first — the automatic
// catch-up, catchup.go — and only then saved and installed. Raft keeps
// ticking meanwhile; the node is not-ready (CatchUp reports it). A false
// catch-up return is a storage without an event log (the fail-fast path in
// Save and processSnapshot says so) or the node closing.
func (n *Raft) installSnapshot(hs *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot) bool {
	releaseCatchUp := func() {}
	if needIndex, needGen, needed := n.snapshotNeedsCatchUp(snap); needed {
		release, ok := n.catchUpEventLog(snap, needIndex, needGen)
		releaseCatchUp = release
		if !ok {
			select {
			case <-n.closeC:
				release()
				return false
			default:
			}
		}
	}
	if err := n.storage.Save(hs, ents, snap); err != nil {
		releaseCatchUp()
		n.failStorage(err)
		return false
	}
	n.processSnapshot(snap)
	if idx := n.storage.AppliedIndex(); idx > n.appliedTo.Load() {
		n.appliedTo.Store(idx)
		n.queuedTo.Store(max(n.queuedTo.Load(), idx))
	}
	// The storage's own maintenance held off during a catch-up (a pending
	// scrub) may run now, over the finished log and the installed metadata.
	releaseCatchUp()
	// An installed snapshot past this node's event log must stop it.
	n.checkStorageInvariant()
	return true
}

// failStorage stops this node after a failed write: the error goes to ErrorC,
// and the Ready loop stops, so no more messages leave and peers re-elect
// around this node. Close tears down the rest.
//
// The send to raftErrorC is non-blocking so an unread error channel
// (raft_test's direct-constructed Raft) doesn't deadlock this goroutine.
// Production callers (db.DB) drain ErrorC so the send always lands.
func (n *Raft) failStorage(err error) {
	n.logger.Error("storage save", zap.Error(err))
	select {
	case n.raftErrorC <- err:
	default:
	}
	n.durable.stop()
	n.storageFailOnce.Do(func() { close(n.storageFailedC) })
}

// deliver hands on the responses of a finished local write: those addressed
// to this node (MsgStorageAppendResp, MsgStorageApplyResp, a leader's
// acknowledgement of its own append) are stepped into raft, in order; the
// rest go to their peers.
func (n *Raft) deliver(responses []*raftpb.Message) {
	var out []*raftpb.Message
	for _, m := range responses {
		if m.GetTo() != n.id {
			out = append(out, m)
			continue
		}
		if err := n.node.Step(n.stepCtx, m); err != nil && n.stepCtx.Err() == nil {
			n.logger.Warn("step local storage response", zap.Stringer("type", m.GetType()), zap.Error(err))
		}
	}
	if len(out) > 0 {
		n.transport.Send(out)
	}
}

// applyConfChanges applies the membership changes in a batch of committed
// entries, BEFORE ApplyCommittedBatch advances the durable applied index.
// applyConfChange calls node.ApplyConfChange (which computes the
// authoritative ConfState) and stages it via storage.ConfState;
// ApplyCommittedBatch then writes that ConfState ATOMICALLY with the applied
// index. If the applied index were persisted first, a crash before the
// ConfState became durable would — with c.Applied suppressing re-delivery —
// restart raft on a stale voter set (confstate-lost-in-crash-window).
func (n *Raft) applyConfChanges(entries []*raftpb.Entry) {
	for _, entry := range entries {
		switch entry.GetType() {
		case raftpb.EntryConfChangeV2:
			// The normal membership path. db.AddMember / db.RemoveMember
			// propose ConfChangeV2 entries (joint consensus), and raft
			// itself proposes a zero-change ConfChangeV2 to leave the joint
			// configuration once it commits (JointImplicit auto-leave). Both
			// flow through here.
			var cc raftpb.ConfChangeV2
			if err := proto.Unmarshal(entry.Data, &cc); err != nil {
				// Guarded send (sendRaftError): the one-shot ErrorC consumer
				// may already be gone (SIGTERM path reads it zero times), and
				// a raw send here parks the apply goroutine forever —
				// deadlocking Close into SIGKILL.
				n.sendRaftError(err)
				continue
			}
			n.lastMembershipIndex.Store(entry.GetIndex())
			n.applyConfChange(&cc, entry.GetIndex())
		case raftpb.EntryConfChange:
			// Backward compatibility: a v1 ConfChange can only appear in a
			// log written by a pre-joint-consensus binary, since this node
			// now proposes v2 exclusively. etcd raft still applies v1
			// (ConfChangeI.AsV2 promotes it), so replaying it keeps an
			// upgraded node consistent with what the old leader committed.
			var cc raftpb.ConfChange
			if err := proto.Unmarshal(entry.Data, &cc); err != nil {
				n.sendRaftError(err) // guarded — see the v2 twin above
				continue
			}
			n.lastMembershipIndex.Store(entry.GetIndex())
			n.applyConfChange(&cc, entry.GetIndex())
		}
	}
}
