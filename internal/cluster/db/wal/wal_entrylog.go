package wal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"

	"github.com/tidwall/wal"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
)

func (s *Storage) getLastStates(li uint64) (*pb.HardState, *pb.Snapshot, error) {
	st := &pb.HardState{}
	snap := &pb.Snapshot{
		Data: nil,
		Metadata: &pb.SnapshotMetadata{
			ConfState: &pb.ConfState{
				Voters:         []uint64{},
				Learners:       []uint64{},
				VotersOutgoing: []uint64{},
				LearnersNext:   []uint64{},
			},
		},
	}

	if li > 0 {
		stDone := false
		snapDone := false

		fi, err := s.StateLog.FirstIndex()
		if err != nil {
			return nil, nil, err
		}

		for i := li; i >= fi; i-- {
			e, err := s.state(i)
			if err != nil {
				return nil, nil, err
			}

			if e.Type == HardState && !stDone {
				hs := &pb.HardState{}
				if err := proto.Unmarshal(e.Data, hs); err != nil {
					return nil, nil, err
				}
				// Skip empty records: raft hands Save an empty HardState on
				// every Ready where it didn't change, and the code before
				// the per-Ready write fix persisted those as-is — so on a
				// legacy log the newest HardState record is usually empty
				// and the real term/vote lives further back. Adopting the
				// empty one would restart the node at Term 0 (taking the
				// StartNode bootstrap path) and forget its vote.
				if !raft.IsEmptyHardState(hs) {
					st = hs
					stDone = true
				}
			} else if e.Type == Snapshot && !snapDone {
				err = proto.Unmarshal(e.Data, snap)
				if err != nil {
					return nil, nil, err
				}
				snapDone = true
			}

			if stDone && snapDone {
				break
			}
		}
	}

	return st, snap, nil
}

// Save persists raft state and entries durably. It does NOT apply entities
// to BoltDB / time series — that happens in ApplyCommitted, which raft.go
// calls separately on rd.CommittedEntries. Splitting these is important
// because the raft Ready loop hands Save the *to-persist* slice (rd.Entries),
// which on a multi-node follower may include uncommitted entries; applying
// them to bucket state before commit would diverge the cluster.
//
// Empty-value handling: per the etcd raft contract, rd.HardState and
// rd.Snapshot are EMPTY on every Ready where they didn't change — which is
// most of them (message-only Readys for heartbeats and elections carry
// neither). Neither empty value is adopted or persisted. Adopting an empty
// snap would destroy the ConfState that the conf-change apply path wrote via
// (*Storage).ConfState, which breaks raft.RestartNode's voter-set recovery;
// adopting an empty HardState would zero the in-memory term/vote, and
// persisting it would shadow the real record on recovery (see
// getLastStates). The snapshot is persisted only when snapDirty says it
// actually changed, so an unchanged-state Ready writes nothing at all —
// persisting the full snapshot (the entire serialized bbolt database) on
// every Ready is the write amplification that could fill a disk in hours
// under a crash loop.
//
// A Ready carrying a non-empty snapshot (an InstallSnapshot) is a log
// REPLACEMENT, not an append, and is handled by saveWithSnapshot: the
// entries that accompany it follow the snapshot index, not this node's
// existing entry log, so appending them here would break the seq mapping.
func (s *Storage) Save(st *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if !raft.IsEmptySnap(snap) {
		return s.saveWithSnapshot(st, ents, snap)
	}

	s.snapMu.Lock()
	if !raft.IsEmptyHardState(st) {
		// Store raft's HardState pointer directly: raft builds a fresh
		// HardState value per Ready and never mutates it after return, and
		// this code never mutates *s.hardState in place — so aliasing is safe
		// and avoids copying the (lock-bearing) protobuf struct by value.
		s.hardState = st
	}
	hardCopy := s.hardState
	snapCopy := s.snapshot
	snapDirty := s.snapDirty
	// Consumed here rather than after the write: a failed appendState stops
	// the node (see the Save error policy in raft.go's Ready loop), so
	// clearing early can't lose a snapshot.
	s.snapDirty = false
	s.snapMu.Unlock()

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	if err := s.appendState(hardCopy, snapCopy, !raft.IsEmptyHardState(st), snapDirty); err != nil {
		return fmt.Errorf("[wal.storage] appendState: %w", err)
	}

	return nil
}

// saveWithSnapshot handles the Ready that carries an InstallSnapshot. raft
// has already reset its in-memory log to the snapshot point; the entries in
// ents (if any — the leader's follow-up MsgApp can coalesce into the same
// Ready) start at snap index+1 and connect to NOTHING in this node's
// existing entry log. Before this path existed, appendEntries hit its
// contiguity guard on those entries and the node silently stopped
// replicating — the in-place snapshot catch-up path was broken.
//
// Severe lag is rejected here WITHOUT persisting or mutating anything: if
// the snapshot leaps past the permanent event log, this node must be
// rebuilt (processSnapshot fatal-exits with the rebuild message right after
// this returns). Persisting any of the Ready first would let a restart
// without the rsync rebuild come up with raft state ahead of the event log
// and silently leave a permanent gap in it; restarting with the
// pre-snapshot state instead makes the leader re-send the snapshot and the
// fatal re-fire until the operator runs the rebuild.
//
// On accept the order is: persist the snapshot+hard state (durable intent),
// cut the entry log over to a dummy at the snapshot point, then append the
// follow-up entries. A crash between the persist and the cut-over is
// completed at the next Open by reconcileEntryLogWithSnapshot.
func (s *Storage) saveWithSnapshot(st *pb.HardState, ents []*pb.Entry, snap *pb.Snapshot) error {
	if snap.Metadata.GetIndex() > s.eventIndex.Load() {
		return nil
	}

	s.snapMu.Lock()
	if !raft.IsEmptyHardState(st) {
		s.hardState = st
	}
	// Clone, don't alias: raft's rd.Snapshot may point at its internal
	// unstable snapshot, and ConfState() mutates s.snapshot.Metadata in
	// place — so we must own this copy.
	s.snapshot = proto.Clone(snap).(*pb.Snapshot)
	// Consumed by the appendState call below, which always persists this
	// (newer) snapshot.
	s.snapDirty = false
	hardCopy := s.hardState
	s.snapMu.Unlock()

	if err := s.appendState(hardCopy, snap, !raft.IsEmptyHardState(st), true); err != nil {
		return fmt.Errorf("[wal.storage] appendState: %w", err)
	}

	if err := s.resetEntryLogToSnapshot(snap.Metadata.GetIndex(), snap.Metadata.GetTerm()); err != nil {
		return fmt.Errorf("[wal.storage] reset entry log: %w", err)
	}

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	return nil
}

// entryLogDiscardDir is where resetEntryLogToSnapshot renames the
// superseded entry log before recreating a fresh one. A crash can strand
// it; Open removes it before opening the live dir.
func entryLogDiscardDir(entryLogDir string) string {
	return entryLogDir + ".discarded"
}

// resetEntryLogToSnapshot replaces the entry log with a single dummy entry
// at the snapshot point — the durable analogue of etcd
// MemoryStorage.ApplySnapshot's ents = [{Index, Term}]. The dummy sits at
// the compaction boundary: unreadable through Entries, but it supplies
// Term(index) for raft's log-matching probe, and the seq mapping is rebased
// (firstIndex = index) so post-snapshot entries append contiguously after
// it. Discarding the old entries loses nothing durable: the entry log is
// consensus transport; the permanent event log is a separate store this
// function never touches.
//
// Not crash-atomic — close/rename/recreate are separate steps. The
// preceding appendState persisted the snapshot record, which Open treats as
// the durable intent: reconcileEntryLogWithSnapshot re-runs this cut-over
// if a crash lands anywhere inside it, and Open removes a stranded
// .discarded dir. An error mid-way leaves the handle unusable and is
// returned, which stops the node (the Save error posture); the next boot
// heals.
func (s *Storage) resetEntryLogToSnapshot(index, term uint64) error {
	dummy := pb.Entry{Index: &index, Term: &term}
	data, err := proto.Marshal(&dummy)
	if err != nil {
		return err
	}

	discard := entryLogDiscardDir(s.raftLogDir)
	if err := os.RemoveAll(discard); err != nil {
		return fmt.Errorf("clear stale discard dir: %w", err)
	}

	s.entryMu.Lock()
	defer s.entryMu.Unlock()

	if err := s.EntryLog.Close(); err != nil {
		return fmt.Errorf("close entry log: %w", err)
	}
	if err := os.Rename(s.raftLogDir, discard); err != nil {
		return fmt.Errorf("rename entry log dir: %w", err)
	}
	if err := os.MkdirAll(s.raftLogDir, 0o700); err != nil {
		return err
	}
	fresh, err := wal.Open(s.raftLogDir, nil)
	if err != nil {
		return fmt.Errorf("reopen entry log: %w", err)
	}
	if err := fresh.Write(1, frame(data)); err != nil {
		return fmt.Errorf("write snapshot dummy: %w", err)
	}
	s.EntryLog = fresh
	// Best effort — Open clears a leftover before the next boot's open.
	_ = os.RemoveAll(discard)

	s.firstIndex.Store(index)
	s.compactedUpTo.Store(index)
	s.lastIndex.Store(index)
	return nil
}

// reconcileEntryLogWithSnapshot completes an in-place snapshot install that
// crashed between saveWithSnapshot's appendState (snapshot persisted) and
// its resetEntryLogToSnapshot (entry log cut over). Called from Open after
// both logs are recovered. A node whose entry log already reaches the
// snapshot point — every normal node, since compaction keeps the log's tail
// well past the last snapshot — is left alone.
func (s *Storage) reconcileEntryLogWithSnapshot() error {
	snapIdx := s.snapshot.Metadata.GetIndex()
	if snapIdx == 0 || s.lastIndex.Load() >= snapIdx {
		return nil
	}
	return s.resetEntryLogToSnapshot(snapIdx, s.snapshot.Metadata.GetTerm())
}

// SetLostNotifier installs the truncation lost-callback. db.New calls it
// (via the lostNotifierSetter optional interface) with db.notifyLost
// because the callback can't be passed at Open time — the DB it closes
// over doesn't exist yet. It MUST be called before the raft serveChannels
// goroutine starts (db wires it inside newRaftWithOptions, before
// startRaft), since appendEntries — the sole reader — runs only on that
// goroutine; setting it after would race. See the lostCallback field doc
// and WithLostCallback (the test-only construction-time equivalent).
func (s *Storage) SetLostNotifier(fn func([]uint64)) {
	s.lostCallback = fn
}

// collectTruncatedRequestIDs reads the entries about to be overwritten by a
// conflicting append — wal seqs [firstSeq, lastSeq] inclusive — and returns
// the set of non-zero cluster.Proposal RequestIDs they carry. These are the
// proposals physically present on this node that a higher-term leader is
// about to truncate before they committed; their waiters (if this node is
// also where Propose ran) get the definitive ErrProposalLost.
//
// Best-effort by design: an entry that fails to read or decode is logged and
// skipped — the truncation is already happening, so signalling the IDs we
// could decode beats signalling none. Config-change and empty (post-election
// no-op) entries carry no RequestID and contribute nothing.
func (s *Storage) collectTruncatedRequestIDs(firstSeq, lastSeq uint64) []uint64 {
	var ids []uint64
	for seq := firstSeq; seq <= lastSeq; seq++ {
		e, _, err := s.entry(seq)
		if err != nil {
			s.logger.Warn("truncation detection: read entry failed; skipping",
				zap.Uint64("seq", seq), zap.Error(err))
			continue
		}
		if e.GetType() != pb.EntryNormal || len(e.Data) == 0 {
			continue
		}
		rid, err := cluster.RequestIDFromProposal(e.Data)
		if err != nil {
			s.logger.Warn("truncation detection: decode proposal failed; skipping",
				zap.Uint64("seq", seq), zap.Uint64("index", e.GetIndex()), zap.Error(err))
			continue
		}
		if rid != 0 {
			ids = append(ids, rid)
		}
	}
	return ids
}

func (s *Storage) appendEntries(ents []*pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	// Snapshot the current bounds. appendEntries is only ever invoked from
	// the raft serveChannels goroutine (the sole writer), so observing them
	// once at the top is sufficient — no other writer can race us.
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	// `first` is the lowest raft index the wal can accept an append at
	// — one past the current compacted-dummy (boundary()). Entries at
	// or below that are already compacted away and must be skipped, not
	// rewritten.
	first := s.boundary() + 1
	last := ents[0].GetIndex() + uint64(len(ents)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > ents[0].GetIndex() {
		ents = ents[first-ents[0].GetIndex():]
	}

	offset := ents[0].GetIndex() - firstIndex
	l := lastIndex - firstIndex + 1

	// Don't error when this is the first write
	if firstIndex > 0 && l < offset {
		return fmt.Errorf("missing log entry [last: %d, append at: %d]", lastIndex, firstIndex)
	}

	// We have received previous log entries a second time and/or have log entries newer than the ones being received
	// This can happen during leadership changes or because we wrote data that was later not accepted by consensus
	// Trust the new data over the old data
	if l > offset {
		// Before overwriting the conflicting tail, capture the RequestIDs
		// of any proposals it carries so the lost-callback can give their
		// blocking-Propose waiters the definitive ErrProposalLost. The
		// truncated entries occupy wal seqs offset+1..l (raft indexes
		// ents[0].GetIndex()..lastIndex) — the very entries TruncateBack drops.
		// Skipped unless a callback is registered (the only case that
		// cares) and there is a prior log to truncate, so the happy path
		// pays nothing.
		var lostIDs []uint64
		if s.lostCallback != nil && firstIndex > 0 {
			lostIDs = s.collectTruncatedRequestIDs(offset+1, l)
		}

		err := s.EntryLog.TruncateBack(offset)
		if err != nil {
			return err
		}

		// Signal only after the truncation actually executed, so a waiter
		// never sees ErrProposalLost for an entry that survived. The
		// callback (db.notifyLost) only does map lookups and non-blocking
		// channel sends, so calling it inline on the serveChannels
		// goroutine — exactly as notifyApplied is — is safe.
		if len(lostIDs) > 0 {
			s.lostCallback(lostIDs)
		}
	}

	// case len > offset:
	// 	// NB: full slice expression protects ms.ents at index >= offset from
	// 	// rewrites, as they may still be referenced from outside MemoryStorage.
	// 	ms.ents = append(ms.ents[:offset:offset], entries...)
	// case len == offset:
	// 	ms.ents = append(ms.ents, entries...)
	// default:
	// 	return fmt.Errorf("missing log entry [last: %d, append at: %d]", s.lastIndex, s.firstIndex)
	// }

	if firstIndex == 0 && lastIndex == 0 && ents != nil {
		firstIndex = ents[0].GetIndex()
		s.firstIndex.Store(firstIndex)
	}

	for _, e := range ents {
		data, err := proto.Marshal(e)
		if err != nil {
			return err
		}

		i := e.GetIndex() - firstIndex + 1
		err = s.EntryLog.Write(i, frame(data))
		if err != nil {
			return fmt.Errorf("index %d to position %d: %w", e.GetIndex(), i, err)
		}
	}

	s.lastIndex.Store(ents[len(ents)-1].GetIndex())

	return nil
}

// stateLogReanchorFloor floors the re-anchor threshold in appendState so a
// small snapshot doesn't get re-appended every few HardState records. A var,
// not a const, only so tests can lower it to exercise re-anchoring without
// writing 64KB of records.
var stateLogReanchorFloor = 64 * 1024

// appendState persists raft state to the state log and truncates the log so
// it stays bounded. cur and snap are the CURRENT in-memory hard state and
// snapshot; hardChanged / snapChanged say whether this Ready actually changed
// them. Each is written only on change — recovery (getLastStates) needs only
// the newest Snapshot and the newest non-empty HardState record, and
// everything older than the older of those two is truncated away after the
// write. An unchanged-state call writes nothing.
//
// Two refinements keep the truncation cut moving:
//
//   - Writing the snapshot only on change lets HardState records pile up
//     behind an aging Snapshot record (the cut can't pass it). Once they
//     outgrow one snapshot copy, the snapshot is re-appended at the tail to
//     advance the cut — at most one extra snapshot write per snapshot-size
//     of hard-state churn, which matters when compaction (the usual
//     hourly snapshot refresher) is disabled.
//   - Symmetrically, a snapshot write re-appends the current hard state so
//     an old HardState record doesn't pin the cut the same way.
//
// The Snapshot record is written before the HardState record, matching the
// raft Ready contract's persist-the-snapshot-first ordering.
func (s *Storage) appendState(cur *pb.HardState, snap *pb.Snapshot, hardChanged, snapChanged bool) error {
	writeSnap := snapChanged
	if !writeSnap && hardChanged && s.lastSnapSeq > 0 &&
		s.hardStateBytesSinceSnap > max(s.lastSnapBytes, stateLogReanchorFloor) {
		writeSnap = true
	}
	writeHard := hardChanged || (writeSnap && !raft.IsEmptyHardState(cur))

	if !writeSnap && !writeHard {
		return nil
	}

	if writeSnap {
		data, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		n, err := s.writeStateRecord(Snapshot, data)
		if err != nil {
			return err
		}
		s.lastSnapSeq = s.stateIndex
		s.lastSnapBytes = n
		s.hardStateBytesSinceSnap = 0
	}

	if writeHard {
		data, err := proto.Marshal(cur)
		if err != nil {
			return err
		}
		n, err := s.writeStateRecord(HardState, data)
		if err != nil {
			return err
		}
		s.lastHardStateSeq = s.stateIndex
		s.hardStateBytesSinceSnap += n
	}

	cut := s.lastSnapSeq
	if s.lastHardStateSeq > 0 && (cut == 0 || s.lastHardStateSeq < cut) {
		cut = s.lastHardStateSeq
	}
	if cut > 0 {
		fi, err := s.StateLog.FirstIndex()
		if err == nil && fi > 0 && fi < cut {
			// Best-effort: a failed truncation costs disk, not correctness —
			// the records we just wrote are durable either way.
			if err := s.StateLog.TruncateFront(cut); err != nil {
				s.logger.Warn("truncate state log", zap.Uint64("cut", cut), zap.Error(err))
			}
		}
	}

	return nil
}

func (s *Storage) writeStateRecord(typ StateType, data []byte) (int, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(State{Type: typ, Data: data}); err != nil {
		return 0, err
	}

	framed := frame(buf.Bytes())
	s.stateIndex++
	if err := s.StateLog.Write(s.stateIndex, framed); err != nil {
		return 0, fmt.Errorf("position %d: %w", s.stateIndex, err)
	}
	return len(framed), nil
}
