package wal_test

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// These tests pin the state-log write/recovery semantics that prevent the
// disk-exhaustion incident: the pre-fix code appended the HardState AND the
// full snapshot (the entire serialized bbolt database) to the state log on
// EVERY raft Ready and never truncated, so a crash-looping node appended
// snapshot copies on each boot's pre-vote rounds until the disk filled.

func stateDirSize(t *testing.T, storageDir string) int64 {
	t.Helper()
	var total int64
	err := filepath.Walk(filepath.Join(storageDir, "raft", "state"),
		func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				total += info.Size()
			}
			return nil
		})
	require.NoError(t, err)
	return total
}

// gobStateRecord encodes a wal.State the way appendState does, so tests can
// forge legacy-format records directly in the state log.
func gobStateRecord(t *testing.T, typ wal.StateType, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(wal.State{Type: typ, Data: data}))
	return wal.Frame(buf.Bytes())
}

func metaSnapshot(idx uint64, data []byte) pb.Snapshot {
	return pb.Snapshot{
		Data: data,
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: []uint64{1}},
			Index:     idx,
			Term:      1,
		},
	}
}

// The term-loss half of the bug: raft hands Save an empty HardState on every
// Ready where it didn't change. Persisting those (as the pre-fix code did)
// shadows the real term/vote at recovery, so a restarted node came up at
// Term 0 and took the StartNode bootstrap path. Unchanged-state Saves must
// write nothing, and the real state must survive a reopen.
func TestStateLog_UnchangedReadysPreserveTerm(t *testing.T) {
	s := NewStorage(t, nil)
	require.NoError(t, s.Save(pb.HardState{Term: 7, Vote: 2, Commit: 1}, nil, defaultSnap))

	li, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	for range 25 {
		require.NoError(t, s.Save(defaultHardState, nil, defaultSnap))
	}
	li2, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	require.Equal(t, li, li2, "unchanged-state Saves must not grow the state log")

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	hs, _, err := s2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(7), hs.Term, "term must survive restart")
	require.Equal(t, uint64(2), hs.Vote, "vote must survive restart")
}

// Records superseded by a newer snapshot + hard state are truncated away:
// the state log holds the few records recovery needs, not history.
func TestStateLog_TruncatesSupersededRecords(t *testing.T) {
	s := NewStorage(t, nil)

	// Installed snapshots are gated on the permanent event log having
	// reached the snapshot point; apply through 21 first.
	for _, e := range index(1).terms(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1) {
		require.NoError(t, s.ApplyCommitted(e))
	}

	require.NoError(t, s.Save(pb.HardState{Term: 1, Vote: 1, Commit: 1}, nil, metaSnapshot(1, []byte("meta"))))
	for i := uint64(2); i <= 20; i++ {
		require.NoError(t, s.Save(pb.HardState{Term: 1, Vote: 1, Commit: i}, nil, defaultSnap))
	}

	// The old snapshot record pins the truncation cut, so nothing is
	// reclaimable yet.
	fi, err := s.StateLog.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), fi)

	// A new snapshot at the tail advances the cut past every older record.
	require.NoError(t, s.Save(pb.HardState{Term: 1, Vote: 1, Commit: 21}, nil, metaSnapshot(21, []byte("meta"))))

	fi, err = s.StateLog.FirstIndex()
	require.NoError(t, err)
	li, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	require.Equal(t, li-1, fi, "only the latest snapshot + hard state records should remain")

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	hs, cs, err := s2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(21), hs.Commit)
	require.Equal(t, []uint64{1}, cs.Voters)

	snap, err := s2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(21), snap.Metadata.Index)
}

// With compaction disabled no new snapshots arrive, so HardState records
// accumulate behind the last snapshot record and the truncation cut can't
// advance. Once they outgrow one snapshot copy, appendState re-appends the
// snapshot at the tail to move the cut — the log stays bounded.
func TestStateLog_ReanchorBoundsHardStateChurn(t *testing.T) {
	restore := wal.SetStateLogReanchorFloorForTest(256)
	defer restore()

	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Event log must reach the snapshot point for the install to be accepted.
	require.NoError(t, s.ApplyCommitted(index(1).terms(1)[0]))
	require.NoError(t, s.Save(pb.HardState{Term: 1, Vote: 1, Commit: 1}, nil, metaSnapshot(1, []byte("meta"))))
	for i := uint64(2); i <= 100; i++ {
		require.NoError(t, s.Save(pb.HardState{Term: 1, Vote: 1, Commit: i}, nil, defaultSnap))
	}

	fi, err := s.StateLog.FirstIndex()
	require.NoError(t, err)
	li, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	require.Less(t, li-fi+1, uint64(12),
		"re-anchoring must keep the state log bounded under hard-state churn")
}

// Open must recover from — and reclaim — a legacy state log written by the
// pre-fix code: full-snapshot records on every Ready, interleaved with empty
// HardState records that shadow the real term/vote. This is the on-disk
// shape of the 48GB incident.
func TestStateLog_OpenReclaimsLegacyBloat(t *testing.T) {
	s := NewStorage(t, nil)

	// The real, current hard state — about to be buried under garbage.
	require.NoError(t, s.Save(pb.HardState{Term: 7, Vote: 2, Commit: 3}, nil, defaultSnap))

	// Forge the legacy tail: one full-snapshot record plus one empty
	// HardState record per Ready, as written during a crash loop's pre-vote
	// rounds (no quorum, so the hard state never changed).
	blob := bytes.Repeat([]byte("x"), 200*1024)
	legacySnap := pb.Snapshot{
		Data: blob,
		Metadata: pb.SnapshotMetadata{
			ConfState: pb.ConfState{Voters: []uint64{1, 2}},
			Index:     3,
			Term:      7,
		},
	}
	snapb, err := legacySnap.Marshal()
	require.NoError(t, err)
	emptyHS, err := (&pb.HardState{}).Marshal()
	require.NoError(t, err)

	li, err := s.StateLog.LastIndex()
	require.NoError(t, err)
	for range 30 {
		li++
		require.NoError(t, s.StateLog.Write(li, gobStateRecord(t, wal.Snapshot, snapb)))
		li++
		require.NoError(t, s.StateLog.Write(li, gobStateRecord(t, wal.HardState, emptyHS)))
	}
	sizeBefore := stateDirSize(t, s.path)

	s2 := s.CloseAndReopenStorage(t)
	defer s2.Cleanup()

	hs, cs, err := s2.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(7), hs.Term, "recovery must skip the empty records and find the real term")
	require.Equal(t, uint64(2), hs.Vote)
	require.Equal(t, []uint64{1, 2}, cs.Voters, "newest snapshot's ConfState must be adopted")

	snap, err := s2.Snapshot()
	require.NoError(t, err)
	require.Equal(t, blob, snap.Data)

	sizeAfter := stateDirSize(t, s.path)
	require.Less(t, sizeAfter, sizeBefore/5,
		"Open must truncate the legacy bloat down to the records recovery needs")
}

// The crash-loop regression proper: repeated open/close cycles (each Open
// rewrites the recovered state at the tail) must not accumulate snapshot
// copies — the pre-fix code grew the state log on every boot and never gave
// a byte back.
func TestStateLog_RebootsDoNotAccumulate(t *testing.T) {
	s := NewStorage(t, nil)

	snap, err := s.CreateSnapshot(0, &pb.ConfState{Voters: []uint64{1}})
	require.NoError(t, err)
	require.NotEmpty(t, snap.Data)
	require.NoError(t, s.Save(pb.HardState{Term: 2, Vote: 1, Commit: 0}, nil, defaultSnap))

	cur := s
	for range 5 {
		cur = cur.CloseAndReopenStorage(t)
	}
	defer cur.Cleanup()

	size := stateDirSize(t, s.path)
	require.Less(t, size, int64(3*len(snap.Data)+64*1024),
		"state log must stay ~one snapshot copy across reboots")
}

// Stronger than the accumulation bound above: a restart with no writes in
// between must be a fixed point — byte-identical state log, not merely
// bounded. A bound tolerates a slow leak (one small record per boot grows
// for months before tripping it); equality catches the first leaked byte.
//
// The first boot is excluded from the comparison: its log layout is whatever
// Save left behind, and Open's rewrite-and-truncate (which produces the
// steady state) only runs on reopen. So: create state, restart once to
// settle, then every further restart must leave the log unchanged.
func TestStateLog_RestartWithoutWritesIsFixedPoint(t *testing.T) {
	s := NewStorage(t, nil)

	snap, err := s.CreateSnapshot(0, &pb.ConfState{Voters: []uint64{1}})
	require.NoError(t, err)
	require.NotEmpty(t, snap.Data)
	require.NoError(t, s.Save(pb.HardState{Term: 2, Vote: 1, Commit: 0}, nil, defaultSnap))

	cur := s.CloseAndReopenStorage(t)
	defer func() { cur.Cleanup() }()
	steady := stateDirSize(t, s.path)

	for restart := 2; restart <= 4; restart++ {
		cur = cur.CloseAndReopenStorage(t)
		require.Equal(t, steady, stateDirSize(t, s.path),
			"restart %d with no writes must leave the state log byte-identical", restart)
	}

	// Size parity with corrupted content would be no fix at all — the
	// recovered state must still be the one we wrote.
	hs, cs, err := cur.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(2), hs.Term)
	require.Equal(t, uint64(1), hs.Vote)
	require.Equal(t, []uint64{1}, cs.Voters)

	got, err := cur.Snapshot()
	require.NoError(t, err)
	require.Equal(t, snap.Data, got.Data)
}
