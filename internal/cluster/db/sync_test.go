package db_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
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
				require.Equal(t, nil, db.Propose(p))
				<-db.CommitC
			}

			syncable := NewSyncable(size, cancel)
			err := db.Sync(ctx, id, syncable)
			require.Nil(t, err)
			for i := 0; i < len(tc.inputs); i++ {
				<-db.CommitC
			}
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

			ps := createProposalsAndProposeThem(t, db, tc.inputs)
			ctx, cancel := context.WithCancel(context.Background())
			syncable := NewSyncable(size, cancel)

			_ = db.Sync(ctx, id, syncable)
			for i := 0; i < len(tc.inputs); i++ {
				<-db.CommitC
			}

			inputs2 := modifyInputs(tc.inputs)

			ps2 := createProposalsAndProposeThem(t, db, inputs2)
			ctx, cancel = context.WithCancel(context.Background())
			syncable = NewSyncable(size, cancel)

			storage.indexes[id] = getLastIndex(storage, ps)

			_ = db.Sync(ctx, id, syncable)
			for i := 0; i < len(inputs2); i++ {
				<-db.CommitC
			}

			require.Equal(t, size, syncable.Count())

			for i, p := range storage.Proposals() {
				fmt.Printf("Testing [%v] - %v", i, p)
				if i < size {
					require.False(t, cluster.IsSystem(p.Entities[0].Type.ID))
					require.Equal(t, ps[i%size], p)
				} else if i >= size*2 && i < size*3 {
					require.False(t, cluster.IsSystem(p.Entities[0].Type.ID))
					require.Equal(t, ps2[i%size], p)
				} else {
					require.True(t, cluster.IsSystem(p.Entities[0].Type.ID))
				}
			}

			for i, got := range syncable.Proposals() {
				require.False(t, cluster.IsSystem(got.Entities[0].Type.ID))
				require.Equal(t, ps2[i], got)
			}
		})
	}
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

			db.EatCommitC()

			// Start as not leader
			s.SetNode(math.MaxUint64)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ps := createProposals(tc.input1)
			for _, p := range ps {
				require.Equal(t, nil, db.Propose(p))
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
				require.Equal(t, nil, db.Propose(p))
			}
			newSize := size + len(tc.input2)
			require.Eventually(t, func() bool { return syncable.Count() == newSize }, activeWait, 5*time.Millisecond)

			// leader -> not-leader - stop ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(quietWait)
			ps3 := createProposals(tc.input2)
			for _, p := range ps3 {
				require.Equal(t, nil, db.Propose(p))
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

	ents, err := db.ents()
	require.Nil(t, err)
	require.Equal(t, len(ps)*2, len(ents))

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
