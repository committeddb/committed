package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sync"
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
	// quietWait gives the ingest goroutine a chance to fire when we expect
	// it NOT to. We can't poll for absence of work, so we have to wait
	// fixed. It also has to be long enough for the ingest state machine to
	// notice a leader-state change before any new proposals are committed.
	const quietWait = 500 * time.Millisecond
	// activeWait is the upper bound for "expect N items to be ingested". The
	// real wait is typically a few ms because we poll, but under -race the
	// raft and ingest goroutines compete for CPU so we keep this generous
	// to avoid flakes.
	const activeWait = 10 * time.Second

	waitForCommitCount := func(t *testing.T, d *DB, want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			ents, err := d.ents()
			return err == nil && len(ents) == want
		}, activeWait, 5*time.Millisecond)
	}

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

			size := len(ps) + len(positions)
			positionIndex := size - 1

			checkCommits(t, db, []*cluster.Proposal{})

			// not-leader -> not-leader - keep not-ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(quietWait)
			checkCommits(t, db, []*cluster.Proposal{})

			// not-leader -> leader - start ingesting
			s.SetNode(db.ID())
			waitForCommitCount(t, db, size)
			checkCommits(t, db, ps, positionIndex)

			// leader -> leader - keep ingesting
			s.SetNode(db.ID())
			ps2 := createProposals([][]string{{"ps2"}})
			ingestable.AppendProposal(ps2[0])
			ingestable.AppendPosition(cluster.Position([]byte("bar")))
			size = size + 2
			waitForCommitCount(t, db, size)
			checkCommits(t, db, append(ps, ps2...), positionIndex, size-1)

			// leader -> not-leader - stop ingesting
			s.SetNode(math.MaxUint64)
			time.Sleep(quietWait)
			ps3 := createProposals([][]string{{"ps3"}})
			ingestable.AppendProposal(ps3[0])
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

// MemoryIngestable is a test ingestable that streams a fixed list of
// proposals/positions to the DB. The slices may be appended to while the
// ingest goroutine is reading them, so all access is mutex-guarded.
type MemoryIngestable struct {
	cancel func()

	mu        sync.Mutex
	proposals []*cluster.Proposal
	positions []cluster.Position
}

func NewIngestable(proposals []*cluster.Proposal, positions []cluster.Position, cancel func()) *MemoryIngestable {
	return &MemoryIngestable{
		proposals: proposals,
		positions: positions,
		cancel:    cancel,
	}
}

// AppendProposal appends a proposal that the ingest goroutine will pick up
// on its next iteration. Safe to call from any goroutine.
func (mi *MemoryIngestable) AppendProposal(p *cluster.Proposal) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.proposals = append(mi.proposals, p)
}

// AppendPosition appends a position that the ingest goroutine will pick up
// on its next iteration. Safe to call from any goroutine.
func (mi *MemoryIngestable) AppendPosition(p cluster.Position) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	mi.positions = append(mi.positions, p)
}

// nextItem returns the next item the ingest loop should emit, given the
// current proposal and position indices. It checks proposal first then
// position under a single lock acquisition; this ordering must be atomic so
// the test can append both a proposal and a position concurrently without
// the loop observing the new position before the new proposal (which would
// reverse the emission order).
//
// Returns (proposal, true, false) if a proposal is ready, (nil, _, true)
// with the position if a position is ready, or zero values if neither.
func (mi *MemoryIngestable) nextItem(proIdx, posIdx int) (proposal *cluster.Proposal, isProposal bool, position cluster.Position, isPosition bool) {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	if proIdx < len(mi.proposals) {
		return mi.proposals[proIdx], true, nil, false
	}
	if posIdx < len(mi.positions) {
		return nil, false, mi.positions[posIdx], true
	}
	return nil, false, nil, false
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
			proposal, hasProposal, pp, hasPosition := mi.nextItem(position.ProIndex, position.PosIndex)
			if hasProposal {
				fmt.Printf("Proposal Index: %v\n", position.ProIndex)
				pr <- proposal
				position.ProIndex++
			} else if hasPosition {
				fmt.Printf("Position Index: %v\n", position.PosIndex)
				po <- pp
				position.PosIndex++
			}
		}
	}
}

func (mi *MemoryIngestable) Close() error {
	return nil
}
