package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestIngest(t *testing.T) {
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

			ctx, cancel := context.WithCancel(context.Background())
			ps := createProposals(tc.inputs)
			positions := []cluster.Position{cluster.Position([]byte("foo"))}
			ingestable := NewIngestable(ps, positions, cancel)
			err := db.Ingest(ctx, id, ingestable)
			require.Nil(t, err)
			for i := 0; i < (len(ps) + len(positions)); i++ {
				<-db.CommitC
			}

			size := len(ps) + len(positions)
			checkCommits(t, db, ps, size-1)
		})
	}
}

func TestResumeIngest(t *testing.T) {
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

			ctx, cancel := context.WithCancel(context.Background())
			ps := createProposals(tc.inputs)
			positions := []cluster.Position{cluster.Position([]byte("foo"))}
			ingestable := NewIngestable(ps, positions, cancel)
			err := db.Ingest(ctx, id, ingestable)
			require.Nil(t, err)
			for i := 0; i < (len(ps) + len(positions)); i++ {
				<-db.CommitC
			}

			size := len(ps) + len(positions)
			checkCommits(t, db, ps, size-1)

			cancel()

			storage.positions[id] = &MemoryPosition{ProIndex: len(tc.inputs), PosIndex: 1}

			ctx, cancel = context.WithCancel(context.Background())
			inputs2 := modifyInputs(tc.inputs)
			ps2 := createProposals(inputs2)
			positions2 := []cluster.Position{cluster.Position([]byte("bar"))}
			ingestable2 := NewIngestable(append(ps, ps2...), append(positions, positions2...), cancel)
			err = db.Ingest(ctx, id, ingestable2)
			require.Nil(t, err)
			for i := 0; i < (len(ps) + len(positions)); i++ {
				<-db.CommitC
			}

			size2 := (len(ps) + len(positions)) * 2
			checkCommits(t, db, append(ps, ps2...), len(ps), size2-1)
		})
	}
}

func TestIngestWithStateChanges(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	id := "foo"
	duration := 1 * time.Second

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewMemoryStorage()
			db := createDBWithStorage(s)
			defer db.Close()

			db.EatCommitC()

			// Start as not leader
			s.SetNode(math.MaxUint64)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ps := createProposals(tc.inputs)
			positions := []cluster.Position{cluster.Position([]byte("foo"))}
			ingestable := NewIngestable(ps, positions, cancel)
			err := db.Ingest(ctx, id, ingestable)
			require.Nil(t, err)
			// for i := 0; i < (len(ps) + len(positions)); i++ {
			// 	<-db.CommitC
			// }

			size := len(ps) + len(positions)
			positionIndex := size - 1

			checkCommits(t, db, []*cluster.Proposal{})

			// not-leader -> not-leader - keep not-ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(duration)
			checkCommits(t, db, []*cluster.Proposal{})

			// not-leader -> leader - start ingesting
			s.SetNode(db.ID())
			time.Sleep(duration)
			checkCommits(t, db, ps, positionIndex)

			// leader -> leader - keep ingesting
			s.SetNode(db.ID())
			ps2 := createProposals([][]string{{"ps2"}})
			ingestable.proposals = append(ingestable.proposals, ps2[0])
			ingestable.positions = append(ingestable.positions, cluster.Position([]byte("bar")))
			size = size + 2
			time.Sleep(duration)
			checkCommits(t, db, append(ps, ps2...), positionIndex, size-1)

			// leader -> not-leader - stop ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(duration)
			ps3 := createProposals([][]string{{"ps3"}})
			ingestable.proposals = append(ingestable.proposals, ps3[0])
			checkCommits(t, db, append(ps, ps2...), positionIndex, size-1)

			fmt.Printf("Got here\n")
		})
	}
}

func checkCommits(t *testing.T, db *DB, ps []*cluster.Proposal, positions ...int) {
	size := len(ps) + len(positions)

	ents, err := db.ents()
	require.Nil(t, err)
	require.Equal(t, size, len(ents))

	positionsFound := 0
	for i, p := range ents {
		expected := slices.Contains(positions, i)
		got := cluster.IsIngestablePosition(p.Entities[0].Type.ID)
		require.Equal(t, expected, got)
		if got {
			positionsFound++
		} else {
			for ei, e := range ents[i].Entities {
				require.Equal(t, e.Data, ps[i-positionsFound].Entities[ei].Data)
			}
		}
	}

	require.Equal(t, len(positions), positionsFound)
}

type MemoryIngestable struct {
	proposals []*cluster.Proposal
	positions []cluster.Position
	cancel    func()
}

func NewIngestable(proposals []*cluster.Proposal, positions []cluster.Position, cancel func()) *MemoryIngestable {
	return &MemoryIngestable{
		proposals: proposals,
		positions: positions,
		cancel:    cancel,
	}
}

func (mi *MemoryIngestable) Ingest(ctx context.Context, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	position := &MemoryPosition{}
	if pos != nil {
		_ = json.Unmarshal(pos, position)
	}

	for {
		select {
		case <-ctx.Done():
			return nil // TODO Should this be an io.EOF?
		default:
			if len(mi.proposals) > position.ProIndex {
				fmt.Printf("Proposal Index: %v\n", position.ProIndex)
				pr <- mi.proposals[position.ProIndex]
				position.ProIndex++
			} else if len(mi.positions) > position.PosIndex {
				fmt.Printf("Position Index: %v\n", position.PosIndex)
				po <- mi.positions[position.PosIndex]
				position.PosIndex++
			}
		}
	}
}

func (mi *MemoryIngestable) Close() error {
	return nil
}
