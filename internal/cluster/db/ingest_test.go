package db_test

import (
	"context"
	"testing"

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
			checkCommits(t, db, size)
		})
	}
}

// func TestIngestWithStateChanges(t *testing.T) {
// 	tests := map[string]struct {
// 		inputs [][]string
// 	}{
// 		"simple":  {inputs: [][]string{{"foo"}}},
// 		"two":     {inputs: [][]string{{"foo", "bar"}}},
// 		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
// 	}

// 	id := "foo"

// 	for name, tc := range tests {
// 		t.Run(name, func(t *testing.T) {
// 			s := NewMemoryStorage()
// 			db := createDBWithStorage(s)
// 			defer db.Close()

// 			// Start as not leader
// 			leaderState := &cdb.LeaderState{}

// 			ctx, cancel := context.WithCancel(context.Background())
// 			ps := createProposals(tc.inputs)
// 			positions := []cluster.Position{cluster.Position([]byte("foo"))}
// 			ingestable := NewIngestable(ps, positions, cancel)
// 			// err := db.Ingest(ctx, id, ingestable, leaderState)
// 			err := db.Ingest(ctx, id, ingestable)
// 			require.Nil(t, err)
// 			for i := 0; i < (len(ps) + len(positions)); i++ {
// 				<-db.CommitC
// 			}

// 			size := len(ps) + len(positions)

// 			checkCommits(t, db, 0)

// 			// not-leader -> not-leader - keep not-ingesting
// 			leaderState.SetLeader(false)
// 			checkCommits(t, db, 0)

// 			// not-leader -> leader - start ingesting
// 			leaderState.SetLeader(true)
// 			checkCommits(t, db, size)

// 			// leader -> leader - keep ingesting
// 			leaderState.SetLeader(true)
// 			ps2 := createProposals([][]string{{"ps2"}})
// 			ingestable.proposals = append(ingestable.proposals, ps2[0])
// 			size++
// 			checkCommits(t, db, size)

// 			// leader -> not-leader - stop ingesting
// 			leaderState.SetLeader(false)
// 			ps3 := createProposals([][]string{{"ps3"}})
// 			ingestable.proposals = append(ingestable.proposals, ps3[0])
// 			checkCommits(t, db, size)
// 		})
// 	}
// }

func checkCommits(t *testing.T, db *DB, size int) {
	ents, err := db.ents()
	require.Nil(t, err)
	require.Equal(t, size, len(ents))

	for i, p := range ents {
		got := cluster.IsIngestablePosition(p.Entities[0].Type.ID)
		expected := i == size-1
		require.Equal(t, expected, got)
	}
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
	for _, p := range mi.proposals {
		select {
		case <-ctx.Done():
			return nil // TODO Should this be an io.EOF?
		default:
			pr <- p
		}
	}
	for _, p := range mi.positions {
		select {
		case <-ctx.Done():
			return nil // TODO Should this be an io.EOF?
		default:
			po <- p
		}
	}

	return nil
}

// func (mi *MemoryIngestable) Ingest(pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, error) {
// 	go func() {
// 		for _, p := range mi.proposals {
// 			mi.proposalChan <- p
// 		}
// 		for _, p := range mi.positions {
// 			mi.positionChan <- p
// 		}
// 	}()

// 	return mi.proposalChan, mi.positionChan, nil
// }

func (mi *MemoryIngestable) Close() error {
	return nil
}
