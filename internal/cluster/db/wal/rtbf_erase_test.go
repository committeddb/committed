package wal_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	parser "github.com/committeddb/committed/internal/cluster/db/parser"
)

// These tests pin the RTBF delete-key erasure pass (rtbf_erase.go): the gated
// rewrite of a retained user-delete's raw subject key to cluster.ErasedKey.
// They drive committed Scrub commands through the real apply path (which
// records the scrub history the gate depends on) and poll the background
// worker to completion, the way a live node runs.

// eraseHarness applies the shared prologue on a parser-backed storage:
//
//	idx 1: type "u" registration
//	idx 2: syncable config "sink" (create)
//	idx 3: upsert  u/alice   (the PII original)
//	idx 4: delete  u/alice   (the retained tombstone; D = 4)
//	idx 5: upsert  u/bob     (an innocent bystander)
//
// Callers append checkpoint/config/scrub entries from idx 6 up.
func eraseHarness(t *testing.T) *StorageWrapper {
	t.Helper()
	s := NewStorageWithParser(t, nil, parser.New())
	s.RegisterType(t, "u", 1, 1)
	seedSyncableConfig(t, s, "sink", 2)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4)
	saveEntity(t, userUpsert("u", "bob", `{"ok":1}`), s, 1, 5)
	return s
}

// applyCheckpoint commits a SyncableIndex bump for id at raft index idx.
func applyCheckpoint(t *testing.T, s *StorageWrapper, id string, consumed, idx uint64) {
	t.Helper()
	e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: consumed})
	require.NoError(t, err)
	saveEntity(t, e, s, 1, idx)
}

// applyScrub commits a Scrub command (bound, hashDeleteKeys) at raft index idx
// and waits for the background worker to complete it.
func applyScrub(t *testing.T, s *StorageWrapper, bound, idx uint64, hash bool) {
	t.Helper()
	e, err := cluster.NewScrubEntity(bound, hash)
	require.NoError(t, err)
	saveEntity(t, e, s, 1, idx)
	require.Eventually(t, func() bool { return s.ScrubCompletedBound() >= bound },
		10*time.Second, 5*time.Millisecond, "the scrub worker never completed bound %d", bound)
}

// logContainsBytes scans every surviving event-log record for needle — the
// "is the raw subject identifier still on disk in the log" oracle.
func logContainsBytes(t *testing.T, s *StorageWrapper, needle string) bool {
	t.Helper()
	last, err := s.EventLogLastSeq()
	require.NoError(t, err)
	for seq := uint64(1); seq <= last; seq++ {
		raw, err := s.ReadEventAt(seq)
		if err != nil {
			continue // seqs below the log's first are absent after densify
		}
		if bytes.Contains(raw, []byte(needle)) {
			return true
		}
	}
	return false
}

// deleteEntryFor returns the (single) entity of the retained delete entry at
// raft index idx.
func deleteEntryFor(t *testing.T, s *StorageWrapper, idx uint64) *cluster.Entity {
	t.Helper()
	a, err := s.ActualAt(idx)
	require.NoError(t, err)
	require.Len(t, a.Entities, 1)
	require.True(t, a.Entities[0].IsDelete())
	return a.Entities[0]
}

// TestDeleteKeyErase_ErasesOnceConsumed is the headline: once the syncable's
// committed checkpoint passes the delete, an authorized scrub rewrites the
// retained tombstone's key to the sentinel and the raw subject identifier is
// gone from every surviving log record.
func TestDeleteKeyErase_ErasesOnceConsumed(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 5, 6) // consumed through bob — past D=4
	applyScrub(t, s, 6, 7, true)

	e := deleteEntryFor(t, s, 4)
	require.True(t, cluster.IsErasedKey(e.Key), "the consumed delete's key must be the erased sentinel")
	require.False(t, logContainsBytes(t, s, "alice"),
		"the raw subject identifier must be gone from every surviving log record")
	// The bystander is untouched.
	bob, err := s.ActualAt(5)
	require.NoError(t, err)
	require.Equal(t, []byte("bob"), bob.Entities[0].Key)
}

// TestDeleteKeyErase_LaggingSyncableBlocksThenErases pins the gate's blocking
// half and its convergence: a checkpoint below the delete keeps the raw key
// (the lagging sink still needs it to erase its row), and the erase happens on
// the next authorized scrub after the checkpoint passes.
func TestDeleteKeyErase_LaggingSyncableBlocksThenErases(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 3, 6) // consumed only through the upsert — NOT the delete
	applyScrub(t, s, 6, 7, true)

	e := deleteEntryFor(t, s, 4)
	require.Equal(t, []byte("alice"), e.Key,
		"a delete an alive syncable has not consumed must keep its raw key")

	applyCheckpoint(t, s, "sink", 7, 8) // now past D
	applyScrub(t, s, 8, 9, true)

	e = deleteEntryFor(t, s, 4)
	require.True(t, cluster.IsErasedKey(e.Key), "the erase must land once the checkpoint passes the delete")
	require.False(t, logContainsBytes(t, s, "alice"))
}

// TestDeleteKeyErase_UnauthorizedScrubNeverErases pins the feature gate's
// deterministic carrier: a Scrub command without HashDeleteKeys (an older
// proposer, or a cluster below feature level 4) removes upserts but never
// touches a delete's key — every replica, old or new, computes that same
// rewrite.
func TestDeleteKeyErase_UnauthorizedScrubNeverErases(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 5, 6)
	applyScrub(t, s, 6, 7, false)

	e := deleteEntryFor(t, s, 4)
	require.Equal(t, []byte("alice"), e.Key, "an unauthorized scrub must not erase delete keys")
	// The upsert removal still happened.
	_, err := s.ActualAt(3)
	require.Error(t, err, "the PII original is still removed by an unauthorized scrub")
}

// TestDeleteKeyErase_DeletedSyncableStopsBlocking pins the orphaned-sink rule:
// a deleted syncable can never consume anything again, so it must not hold
// the erase hostage (its sink's erasure obligations pass to the operator with
// the deletion).
func TestDeleteKeyErase_DeletedSyncableStopsBlocking(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 3, 6) // lagging — blocks
	// Delete the syncable (config + checkpoint die atomically in one proposal).
	p := &cluster.Proposal{Entities: cluster.NewDeleteSyncableEntities("sink", false)}
	saveProposal(t, p, s, 1, 7)
	applyScrub(t, s, 7, 8, true)

	e := deleteEntryFor(t, s, 4)
	require.True(t, cluster.IsErasedKey(e.Key), "a deleted syncable must not block the erase")
}

// TestDeleteKeyErase_NoSyncablesErasesFreely: with no syncable registered at
// all, nothing can have written a raw-keyed downstream row, so the erase gates
// on nothing.
func TestDeleteKeyErase_NoSyncablesErasesFreely(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	s.RegisterType(t, "u", 1, 1)
	saveEntity(t, userUpsert("u", "alice", `{"pii":true}`), s, 1, 2)
	saveEntity(t, userDelete("u", "alice"), s, 1, 3)

	applyScrub(t, s, 3, 4, true)

	e := deleteEntryFor(t, s, 3)
	require.True(t, cluster.IsErasedKey(e.Key))
	require.False(t, logContainsBytes(t, s, "alice"))
}

// TestDeleteKeyErase_PostScrubSyncableIsExempt pins the liveness half the
// ticket demanded ("no stall under syncable create/rebuild churn"): a
// syncable CREATED after a scrub command whose bound covers the delete never
// saw the removed upsert — its checkpoint sitting at 0 must not block the
// erase, while a pre-existing lagging syncable still does until it consumes.
func TestDeleteKeyErase_PostScrubSyncableIsExempt(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 3, 6) // "sink" lags — blocks the erase
	applyScrub(t, s, 6, 7, true)        // removal runs; erase blocked by "sink"
	require.Equal(t, []byte("alice"), deleteEntryFor(t, s, 4).Key)

	// A NEW syncable created AFTER that scrub command (index 8 > 7): its
	// checkpoint is absent (0), but the covering scrub at index 7 (bound 6 >=
	// D=4) exempts it.
	seedSyncableConfig(t, s, "late", 8)
	// The original sink finally consumes past D.
	applyCheckpoint(t, s, "sink", 8, 9)
	applyScrub(t, s, 9, 10, true)

	e := deleteEntryFor(t, s, 4)
	require.True(t, cluster.IsErasedKey(e.Key),
		"a post-scrub syncable at checkpoint 0 must not stall the erase (exemption)")
}

// TestDeleteKeyErase_CheckpointResetBlocks pins the reset visibility rule: a
// rebuild's checkpoint reset (SyncableIndex delete) revokes the consumed-past-D
// evidence, including a reset committed AFTER the freeze line but BEFORE the
// scrub command — the gate harvests to the command's own index precisely so
// this reset is seen.
func TestDeleteKeyErase_CheckpointResetBlocks(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 5, 6) // consumed past D...
	// ...but a rebuild resets the checkpoint at index 7 — ABOVE the bound (6)
	// the next scrub will run at, below the command itself (8).
	saveEntity(t, cluster.NewDeleteSyncableIndexEntity("sink"), s, 1, 7)
	applyScrub(t, s, 6, 8, true)

	e := deleteEntryFor(t, s, 4)
	require.Equal(t, []byte("alice"), e.Key,
		"a checkpoint reset between the freeze line and the command must block the erase")
}

// TestDeleteKeyErase_IdempotentAcrossRescrubs: an erased delete stays a stable
// sentinel through later authorized scrubs (no re-rewrite churn, no error).
func TestDeleteKeyErase_IdempotentAcrossRescrubs(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	applyCheckpoint(t, s, "sink", 5, 6)
	applyScrub(t, s, 6, 7, true)
	require.True(t, cluster.IsErasedKey(deleteEntryFor(t, s, 4).Key))

	snapBefore, err := s.EventLogSnapshot()
	require.NoError(t, err)

	applyCheckpoint(t, s, "sink", 7, 8)
	applyScrub(t, s, 8, 9, true)

	require.True(t, cluster.IsErasedKey(deleteEntryFor(t, s, 4).Key))
	// The erased entry's record is byte-stable: compare the idx=4 line across
	// snapshots (later lines differ — new entries were appended).
	snapAfter, err := s.EventLogSnapshot()
	require.NoError(t, err)
	require.Equal(t, lineForIdx(t, snapBefore, 4), lineForIdx(t, snapAfter, 4),
		"an already-erased delete must re-marshal byte-identically on re-scrub")
}

// lineForIdx picks the EventLogSnapshot line for a raft index, minus the seq
// prefix (densify renumbers seqs; the content hash is what must be stable).
func lineForIdx(t *testing.T, lines []string, idx uint64) string {
	t.Helper()
	for _, l := range lines {
		if bytes.Contains([]byte(l), []byte("idx=4 ")) && idx == 4 {
			return l[bytes.Index([]byte(l), []byte("idx=")):]
		}
	}
	t.Fatalf("no snapshot line for idx %d in %v", idx, lines)
	return ""
}

// TestDeleteKeyErase_DeterministicAcrossReplicas: two storages fed the same
// committed sequence — including the authorized Scrub command — end with
// byte-identical event logs, the invariant every rewrite must preserve.
func TestDeleteKeyErase_DeterministicAcrossReplicas(t *testing.T) {
	run := func() ([]string, func()) {
		s := eraseHarness(t)
		applyCheckpoint(t, s, "sink", 5, 6)
		applyScrub(t, s, 6, 7, true)
		snap, err := s.EventLogSnapshot()
		require.NoError(t, err)
		return snap, s.Cleanup
	}
	a, cleanupA := run()
	defer cleanupA()
	b, cleanupB := run()
	defer cleanupB()
	require.Equal(t, a, b, "replicas must produce byte-identical logs from the same committed sequence")
}

// TestDeleteKeyErase_BacklogDrivesAndSettles pins the cadence loop:
// HasDeleteKeyEraseBacklog is false with nothing to do, true once a consumed
// un-erased delete exists, and false again after the erase completes — so the
// scheduler proposes exactly while there is work.
func TestDeleteKeyErase_BacklogDrivesAndSettles(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()

	require.False(t, s.HasDeleteKeyEraseBacklog(),
		"a delete no alive syncable has consumed is not yet eligible — no backlog")

	applyCheckpoint(t, s, "sink", 5, 6)
	require.True(t, s.HasDeleteKeyEraseBacklog(),
		"a consumed, un-erased delete is exactly the backlog")

	applyScrub(t, s, 6, 7, true)
	require.False(t, s.HasDeleteKeyEraseBacklog(),
		"after the erase completes the backlog settles")
}

// TestDeleteKeyErase_PartialThresholdKeepsLaterDeleteRaw: two subjects, the
// checkpoint between their deletes — only the consumed one erases, and the
// still-raw one remains in the backlog for the next pass.
func TestDeleteKeyErase_PartialThresholdKeepsLaterDeleteRaw(t *testing.T) {
	s := NewStorageWithParser(t, nil, parser.New())
	defer s.Cleanup()
	s.RegisterType(t, "u", 1, 1)
	seedSyncableConfig(t, s, "sink", 2)
	saveEntity(t, userUpsert("u", "alice", `{"pii":1}`), s, 1, 3)
	saveEntity(t, userDelete("u", "alice"), s, 1, 4) // D1
	saveEntity(t, userUpsert("u", "carol", `{"pii":2}`), s, 1, 5)
	saveEntity(t, userDelete("u", "carol"), s, 1, 6) // D2
	applyCheckpoint(t, s, "sink", 5, 7)              // consumed D1, not D2

	applyScrub(t, s, 7, 8, true)

	require.True(t, cluster.IsErasedKey(deleteEntryFor(t, s, 4).Key), "the consumed delete erases")
	require.Equal(t, []byte("carol"), deleteEntryFor(t, s, 6).Key, "the unconsumed delete stays raw")
	require.False(t, logContainsBytes(t, s, "alice"))
	require.False(t, s.HasDeleteKeyEraseBacklog(),
		"the still-raw delete is not yet eligible (checkpoint below it) — no backlog")
	// The OPERATOR number disagrees with the scheduler flag here, on purpose:
	// a raw key remains on disk, so completion must not be reported. This
	// split is exactly why /node/status exposes the count, not the flag.
	require.Equal(t, 1, s.PendingDeleteKeyErasures(),
		"the operator-facing count reports the still-raw delete even while the scheduler sees no eligible work")

	// The raw delete was retained for the next pass: once the checkpoint
	// passes it, it re-becomes backlog and the next scrub erases it.
	applyCheckpoint(t, s, "sink", 8, 9)
	require.True(t, s.HasDeleteKeyEraseBacklog(),
		"the retained raw delete becomes backlog when the checkpoint passes it")
	applyScrub(t, s, 9, 10, true)
	require.True(t, cluster.IsErasedKey(deleteEntryFor(t, s, 6).Key))
	require.False(t, logContainsBytes(t, s, "carol"))
	require.False(t, s.HasDeleteKeyEraseBacklog())
	require.Zero(t, s.PendingDeleteKeyErasures(), "all keys erased — the completion count reaches zero")
}

// TestDeleteKeyErase_FromZeroReadPinBlocksSwap pins the reader-side invariant:
// while a from-0 read is registered, a scrub does not swap (its completion
// stalls); releasing the pin lets it finish. This is what keeps a fresh
// replay from observing a raw upsert from one log state and an erased delete
// from the next.
func TestDeleteKeyErase_FromZeroReadPinBlocksSwap(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()
	applyCheckpoint(t, s, "sink", 5, 6)

	release := s.BeginFromZeroRead()
	e, err := cluster.NewScrubEntity(6, true)
	require.NoError(t, err)
	saveEntity(t, e, s, 1, 7)

	require.Never(t, func() bool { return s.ScrubCompletedBound() >= 6 },
		500*time.Millisecond, 25*time.Millisecond,
		"the swap must wait while a from-0 read is in flight")

	release()
	require.Eventually(t, func() bool { return s.ScrubCompletedBound() >= 6 },
		10*time.Second, 5*time.Millisecond, "releasing the pin must let the scrub complete")
	require.True(t, cluster.IsErasedKey(deleteEntryFor(t, s, 4).Key))
}

// TestDeleteKeyErase_WaitScrubCurrent pins the from-0 start ordering: the wait
// returns immediately when the local log is current, blocks while a pending
// scrub has not executed locally, and unblocks when it completes.
func TestDeleteKeyErase_WaitScrubCurrent(t *testing.T) {
	s := eraseHarness(t)
	defer s.Cleanup()
	applyCheckpoint(t, s, "sink", 5, 6)

	// Current log: no wait.
	done := make(chan struct{})
	require.NoError(t, s.WaitScrubCurrent(done))

	// Hold the swap with a pin so the pending scrub cannot complete, then
	// assert WaitScrubCurrent blocks — and unblocks once the pin releases.
	release := s.BeginFromZeroRead()
	e, err := cluster.NewScrubEntity(6, true)
	require.NoError(t, err)
	saveEntity(t, e, s, 1, 7)

	waitDone := make(chan error, 1)
	go func() { waitDone <- s.WaitScrubCurrent(done) }()
	select {
	case <-waitDone:
		t.Fatal("WaitScrubCurrent must block while the local scrub is behind")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("WaitScrubCurrent never unblocked after the scrub completed")
	}
}
