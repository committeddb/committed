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

			require.Equal(t, size, syncable.count)

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

			for i, got := range syncable.proposals {
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
	duration := 1 * time.Second

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
			require.Equal(t, 0, syncable.count)

			// not-leader -> not-leader - keep not-ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(duration)
			require.Equal(t, 0, syncable.count)

			// not-leader -> leader - start ingesting
			s.SetNode(db.ID())
			time.Sleep(duration)
			require.Equal(t, size, syncable.count)

			// leader -> leader - keep ingesting
			s.SetNode(db.ID())
			time.Sleep(duration)
			ps2 := createProposals(tc.input2)
			for _, p := range ps2 {
				require.Equal(t, nil, db.Propose(p))
			}
			newSize := size + len(tc.input2)
			time.Sleep(duration)
			require.Equal(t, newSize, syncable.count)

			// leader -> not-leader - stop ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(duration)
			ps3 := createProposals(tc.input2)
			for _, p := range ps3 {
				require.Equal(t, nil, db.Propose(p))
			}
			time.Sleep(duration)
			require.Equal(t, newSize, syncable.count)

			fmt.Printf("Got here\n")
		})
	}
}

func checkSyncs(t *testing.T, db *DB, syncable *MemorySyncable, ps []*cluster.Proposal) {
	size := len(ps)
	require.Equal(t, size, syncable.count)

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
