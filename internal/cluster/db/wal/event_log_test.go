package wal_test

import (
	"io"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"

	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TestEventLog_AppliedEntriesMirrored verifies that ApplyCommitted mirrors
// every committed entry into the permanent event log, keeping
// EventIndex() == AppliedIndex() after each apply. This is the Phase 1
// storage invariant: P_local == R_local.
func TestEventLog_AppliedEntriesMirrored(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	// Before any apply, both indexes are 0 and the invariant holds
	// trivially.
	require.Equal(t, uint64(0), s.EventIndex())
	require.Equal(t, uint64(0), s.AppliedIndex())

	// Apply three user-defined entity proposals at ascending raft indexes.
	// Each apply should advance both EventIndex and AppliedIndex by 1.
	for _, idx := range []uint64{1, 2, 3} {
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "type-x"},
			Key:  []byte("k"),
			Data: []byte("payload"),
		}}}
		saveProposal(t, p, s, 1, idx)

		require.Equal(t, idx, s.EventIndex(), "EventIndex after apply")
		require.Equal(t, idx, s.AppliedIndex(), "AppliedIndex after apply")
	}

	// The event log should now contain exactly three entries.
	li, err := s.EventLogLastSeq()
	require.Nil(t, err)
	require.Equal(t, uint64(3), li)
}

// TestEventLog_SurvivesRestart verifies the event log's contents and
// EventIndex persist across a close/reopen cycle — the crash-recovery
// shape that a clean restart is the easy case of.
func TestEventLog_SurvivesRestart(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	for _, idx := range []uint64{1, 2, 3} {
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "type-x"},
			Key:  []byte("k"),
			Data: []byte("payload"),
		}}}
		saveProposal(t, p, s, 1, idx)
	}

	s, err := s.CloseAndReopen()
	require.Nil(t, err)

	require.Equal(t, uint64(3), s.EventIndex(), "EventIndex restored from event log")
	require.Equal(t, uint64(3), s.AppliedIndex(), "AppliedIndex restored from bbolt")

	li, err := s.EventLogLastSeq()
	require.Nil(t, err)
	require.Equal(t, uint64(3), li, "event log entries persist")

	// Verify the stored entries round-trip back to their raft indexes.
	for seq := uint64(1); seq <= li; seq++ {
		data, err := s.ReadEventAt(seq)
		require.Nil(t, err)
		e := &pb.Entry{}
		require.Nil(t, e.Unmarshal(data))
		require.Equal(t, seq, e.Index, "raft index embedded in event log entry")
	}
}

// TestEventLog_ReaderBootstrapFromIndex1 exercises the Phase 2 CQRS
// bootstrap path: a brand-new syncable (no stored position) created
// against a storage that already has events must read every applied
// event from the beginning, in order.
func TestEventLog_ReaderBootstrapFromIndex1(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	// Type at index 1; data proposals at 2, 3, 4.
	s.RegisterType(t, "type-x", 1, 1)

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i, data := range want {
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "type-x"},
			Key:  []byte{byte(i)},
			Data: data,
		}}}
		saveProposal(t, p, s, 1, uint64(i+2))
	}

	r := s.Reader("brand-new-syncable")
	// The reader also surfaces the type-registration entry — drain it
	// first so the assertions below focus on the user-data entries.
	_, typeEntry, err := r.Read()
	require.Nil(t, err)
	require.Equal(t, "type-x", string(typeEntry.Entities[0].Key))

	for i, w := range want {
		_, got, err := r.Read()
		require.Nil(t, err, "read %d", i)
		require.Equal(t, 1, len(got.Entities))
		require.Equal(t, w, got.Entities[0].Data)
	}
	_, _, err = r.Read()
	require.Equal(t, io.EOF, err, "EOF after draining")
}

// TestEventLog_ReaderResumesFromPosition verifies that a syncable with
// a stored raft-index position skips already-processed entries and
// picks up from the next one, even when the event log's raft indices
// aren't contiguous (the test harness does Save-without-Apply for
// conf-change entries, which mirrors a future-compacted log).
func TestEventLog_ReaderResumesFromPosition(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()
	// Register type-x at index 1; user-data proposals follow at
	// indices 3, 5, 7 (with conf-change filler at 2, 4, 6). The
	// appendEntries path in Save requires contiguous indices, so the
	// gaps between raft indices must be filled by actual entries.
	s.RegisterType(t, "type-x", 1, 1)

	// Apply at raft indices 3, 5, 7 (gaps between).
	for i, idx := range []uint64{3, 5, 7} {
		p := &cluster.Proposal{Entities: []*cluster.Entity{{
			Type: &cluster.Type{ID: "type-x"},
			Key:  []byte{byte(i)},
			Data: []byte{byte(i)},
		}}}
		// Save a no-op EntryConfChange at idx-1 to mimic how raft
		// interleaves conf changes with user entries, but don't Apply
		// it — so only the EntryNormal lands in EventLog.
		cc := pb.Entry{Term: 1, Index: idx - 1, Type: pb.EntryConfChange}
		require.Nil(t, s.Save(defaultHardState, []pb.Entry{cc}, defaultSnap))
		saveProposal(t, p, s, 1, idx)
	}

	// Persist a syncable position of raft index 3 (already processed).
	id := "partly-done"
	posEntity, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: 3})
	require.Nil(t, err)
	saveEntity(t, posEntity, s, 1, 8)

	// Reopen so the reader picks up the persisted position.
	s = s.CloseAndReopenStorage(t)
	defer s.Cleanup()

	r := s.Reader(id)
	// Should skip the entry at raft index 3 and return 5, then 7.
	for _, want := range []uint64{5, 7} {
		idx, _, err := r.Read()
		require.Nil(t, err)
		require.Equal(t, want, idx)
	}
	_, _, err = r.Read()
	require.Equal(t, io.EOF, err)
}

// TestEventLog_ReplaySkipsAlreadyApplied verifies that re-applying an
// already-applied entry (the replay-on-restart shape) doesn't double-
// write the event log, doesn't fail with ErrOutOfOrder, and leaves the
// storage invariant intact.
func TestEventLog_ReplaySkipsAlreadyApplied(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	p := &cluster.Proposal{Entities: []*cluster.Entity{{
		Type: &cluster.Type{ID: "type-x"},
		Key:  []byte("k"),
		Data: []byte("payload"),
	}}}
	bs, err := p.Marshal()
	require.Nil(t, err)

	ent := pb.Entry{Term: 1, Index: 1, Type: pb.EntryNormal, Data: bs}
	require.Nil(t, s.Save(defaultHardState, []pb.Entry{ent}, defaultSnap))
	require.Nil(t, s.ApplyCommitted(ent))
	// Second apply of the same entry is a no-op (replay-on-restart safety).
	require.Nil(t, s.ApplyCommitted(ent))

	require.Equal(t, uint64(1), s.EventIndex())
	require.Equal(t, uint64(1), s.AppliedIndex())

	li, err := s.EventLogLastSeq()
	require.Nil(t, err)
	require.Equal(t, uint64(1), li, "event log not double-written on replay")
}
