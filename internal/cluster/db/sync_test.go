package db_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

func TestSync(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	id := "foo"

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStorage()
			db := createDBWithStorage(s)
			defer db.Close()

			size := len(tc.inputs)

			ctx, cancel := context.WithCancel(context.Background())
			ps := createProposals(tc.inputs)
			for _, p := range ps {
				require.Equal(t, nil, db.Propose(testCtx(t), p))
			}

			syncable := NewSyncable(size, cancel)
			err := db.Sync(ctx, id, syncable)
			require.Nil(t, err)
			// The sync goroutine cancels ctx once it has consumed `size`
			// proposals; wait on that.
			<-ctx.Done()
			checkSyncs(t, db, syncable, ps)
		})
	}
}

func TestResumeSync(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	id := "foo"

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			storage := NewMemoryStorage()
			db := createDBWithStorage(storage)
			defer db.Close()

			size := len(tc.inputs)

			// First sync round: propose ps and let syncable1 consume them.
			// MemorySyncable cancels the test's local ctx when count == size,
			// which is the test's signal to advance — NOT a worker stop. After
			// PR3 the worker context is db.ctx-derived so the caller's ctx is
			// just an out-of-band wakeup channel for the test.
			ps := createProposalsAndProposeThem(t, db, tc.inputs)
			ctx1, cancel1 := context.WithCancel(context.Background())
			syncable1 := NewSyncable(size, cancel1)
			_ = db.Sync(ctx1, id, syncable1)
			<-ctx1.Done()

			// Second sync round: install syncable2 BEFORE proposing the new
			// inputs. Because db.Sync now uses replace semantics, this call
			// cancels the first worker, waits for it to drain, and starts a
			// fresh worker that begins reading at storage.indexes[id]+1.
			// We bump indexes[id] to lastIndex(ps) first so the replacement
			// worker starts past the original ps proposals, exactly as the
			// pre-PR3 version did via worker-context cancellation.
			storage.indexes[id] = getLastIndex(storage, ps)
			ctx2, cancel2 := context.WithCancel(context.Background())
			syncable2 := NewSyncable(size, cancel2)
			_ = db.Sync(ctx2, id, syncable2)

			// Now propose the new inputs. The replacement worker is already
			// running and will pick them up from the persisted index.
			inputs2 := modifyInputs(tc.inputs)
			ps2 := createProposalsAndProposeThem(t, db, inputs2)
			<-ctx2.Done()

			require.Equal(t, size, syncable2.Count())

			// Verify the wal contains exactly the proposals we proposed
			// (ps + ps2) plus some number of system SyncableIndex entries.
			// The exact wal layout depends on raft Ready timing for the
			// fire-and-forget SyncableIndex bumps and is intentionally not
			// asserted: what matters is that ps and ps2 are present as
			// normal entries and that everything else is a system entry.
			normal := 0
			seenPs := 0
			seenPs2 := 0
			for _, p := range storage.Proposals() {
				if cluster.IsSystem(p.Entities[0].Type.ID) {
					continue
				}
				normal++
				switch {
				case containsProposal(ps, p):
					seenPs++
				case containsProposal(ps2, p):
					seenPs2++
				default:
					t.Fatalf("unexpected non-system proposal in wal: %v", p)
				}
			}
			require.Equal(t, len(ps), seenPs, "all ps proposals should appear in wal")
			require.Equal(t, len(ps2), seenPs2, "all ps2 proposals should appear in wal")
			require.Equal(t, len(ps)+len(ps2), normal, "no extra normal proposals")

			for i, got := range syncable2.Actuals() {
				require.False(t, cluster.IsSystem(got.Entities[0].Type.ID))
				require.Equal(t, ps2[i].Entities, got.Entities)
			}
		})
	}
}

// TestSyncReplaysRangeWhenBumpLost documents the replay behavior the
// cluster.Syncable contract now spells out (and that
// sync-fail-fast-bump-tracking.md decided to leave as the framework's
// behavior for non-two-phase syncables): when the post-Sync index bump does
// NOT durably apply — a worker replace/Close cancels it, or a leader flap
// orphans it and db.Propose returns ErrProposalUnknown/ErrProposalLost — the
// worker correctly does not advance the persisted SyncableIndex. A
// replacement worker then re-reads from the stale cursor and re-delivers the
// WHOLE already-synced range. An idempotent sink absorbs that; a
// non-idempotent one (HTTP webhook, event stream) double-emits.
//
// Modeling note: this mirrors TestResumeSync but inverts its key line.
// TestResumeSync advances storage.indexes[id] by hand to model a SUCCESSFUL
// bump (MemoryStorage's ApplyCommitted is a no-op, so a bump never moves the
// cursor on its own). Leaving the cursor un-advanced is therefore the
// faithful model of a LOST bump — the persisted state the worker is left in
// after the bump's Propose fails to apply.
func TestSyncReplaysRangeWhenBumpLost(t *testing.T) {
	inputs := [][]string{{"foo", "bar"}, {"baz"}}
	id := "foo"

	storage := NewMemoryStorage()
	db := createDBWithStorage(storage)
	defer db.Close()

	size := len(inputs)

	// First worker syncs the full range.
	ps := createProposalsAndProposeThem(t, db, inputs)
	ctx1, cancel1 := context.WithCancel(context.Background())
	syncable1 := NewSyncable(size, cancel1)
	_ = db.Sync(ctx1, id, syncable1)
	<-ctx1.Done()
	require.Equal(t, size, syncable1.Count(), "first worker should sync the whole range")

	// LOST bump: the persisted cursor stays put (NOT advanced the way
	// TestResumeSync does to model a successful bump). This is the state the
	// worker is left in when proposeSyncableIndex's Propose returns
	// ErrProposalUnknown/ErrProposalLost during a flap.
	require.Equal(t, uint64(0), storage.indexes[id], "precondition: lost bump leaves the cursor un-advanced")

	// Replacement worker starts from the stale cursor and MUST re-deliver the
	// same range — the duplicate a non-idempotent sink would re-emit.
	ctx2, cancel2 := context.WithCancel(context.Background())
	syncable2 := NewSyncable(size, cancel2)
	_ = db.Sync(ctx2, id, syncable2)
	<-ctx2.Done()

	require.Equal(t, size, syncable2.Count(),
		"replacement worker re-syncs the replayed range when the bump was lost")

	// Both workers saw the SAME proposals: the range was delivered twice.
	for i, p := range ps {
		require.Equal(t, p.Entities, syncable1.Actuals()[i].Entities,
			"first worker delivered proposal %d", i)
		require.Equal(t, p.Entities, syncable2.Actuals()[i].Entities,
			"replacement re-delivered the SAME proposal %d (the replay duplicate)", i)
	}
}

// containsProposal reports whether `target` is byte-equivalent to any
// element of `ps`. Used by TestResumeSync to verify the wal contains
// the expected proposals without depending on their exact wal positions.
func containsProposal(ps []*cluster.Proposal, target *cluster.Proposal) bool {
	for _, p := range ps {
		if proposalEqual(p, target) {
			return true
		}
	}
	return false
}

func proposalEqual(a, b *cluster.Proposal) bool {
	if len(a.Entities) != len(b.Entities) {
		return false
	}
	for i := range a.Entities {
		if string(a.Entities[i].Data) != string(b.Entities[i].Data) {
			return false
		}
		if string(a.Entities[i].Key) != string(b.Entities[i].Key) {
			return false
		}
	}
	return true
}

func TestSyncWithStateChanges(t *testing.T) {
	tests := map[string]struct {
		input1 [][]string
		input2 [][]string
	}{
		"simple":  {input1: [][]string{{"foo"}}, input2: [][]string{{"qux"}}},
		"two":     {input1: [][]string{{"foo", "bar"}}, input2: [][]string{{"qux", "quux"}}},
		"two-one": {input1: [][]string{{"foo", "bar"}, {"baz"}}, input2: [][]string{{"qux", "quux"}, {"corge"}}},
	}

	id := "foo"
	// quietWait is how long we wait when asserting "nothing should happen".
	// We can't poll for absence, so we have to wait a fixed period. This
	// also has to be long enough for the sync goroutine to actually notice
	// a leader-state change before any new proposals are committed —
	// otherwise an in-flight propose can sneak in and be synced before the
	// goroutine transitions to not-leader. Under -race the sync state
	// machine is slow enough that 100ms isn't reliable.
	const quietWait = 500 * time.Millisecond
	// activeWait is the upper bound for "expect N items to be synced". The
	// actual wait is typically a few ms because we poll, but under -race
	// the sync goroutine's busy-wait loop and the raft worker compete for
	// CPU, so we keep this generous to avoid flakes.
	const activeWait = 10 * time.Second

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStorage()
			db := createDBWithStorage(s)
			defer db.Close()

			size := len(tc.input1)

			// Start as not leader
			s.SetNode(math.MaxUint64)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			proposeCtx := testCtx(t)
			ps := createProposals(tc.input1)
			for _, p := range ps {
				require.Equal(t, nil, db.Propose(proposeCtx, p))
			}

			syncable := NewSyncable(len(tc.input1)+len(tc.input2)+1, cancel)
			err := db.Sync(ctx, id, syncable)
			require.Nil(t, err)
			require.Equal(t, 0, syncable.Count())

			// not-leader -> not-leader - keep not-ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(quietWait)
			require.Equal(t, 0, syncable.Count())

			// not-leader -> leader - start ingesting
			s.SetNode(db.ID())
			require.Eventually(t, func() bool { return syncable.Count() == size }, activeWait, 5*time.Millisecond)

			// leader -> leader - keep ingesting
			s.SetNode(db.ID())
			ps2 := createProposals(tc.input2)
			for _, p := range ps2 {
				require.Equal(t, nil, db.Propose(proposeCtx, p))
			}
			newSize := size + len(tc.input2)
			require.Eventually(t, func() bool { return syncable.Count() == newSize }, activeWait, 5*time.Millisecond)

			// leader -> not-leader - stop ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(quietWait)
			ps3 := createProposals(tc.input2)
			for _, p := range ps3 {
				require.Equal(t, nil, db.Propose(proposeCtx, p))
			}
			time.Sleep(quietWait)
			require.Equal(t, newSize, syncable.Count())

			fmt.Printf("Got here\n")
		})
	}
}

func checkSyncs(t *testing.T, db *DB, syncable *MemorySyncable, ps []*cluster.Proposal) {
	size := len(ps)
	require.Equal(t, size, syncable.Count())

	// The sync worker fires proposeSyncableIndex after each successful
	// s.Sync, but it's fire-and-forget — by the time the syncable's
	// internal cancel() unblocks the test, the SyncableIndex propose may
	// not yet have been committed and applied. Poll until both halves
	// (original proposals + syncable indexes) are visible.
	expected := len(ps) * 2
	var ents []*cluster.Proposal
	var err error
	require.Eventually(t, func() bool {
		ents, err = db.ents()
		return err == nil && len(ents) == expected
	}, 5*time.Second, 5*time.Millisecond)
	require.Nil(t, err)
	require.Equal(t, expected, len(ents))

	for i, p := range ents {
		got := cluster.IsSyncableIndex(p.Entities[0].Type.ID)
		expected := i >= len(ps)
		require.Equal(t, expected, got)

		if !got {
			for ei, e := range ents[i].Entities {
				require.Equal(t, e.Data, ents[i].Entities[ei].Data)
			}
		}
	}
}
