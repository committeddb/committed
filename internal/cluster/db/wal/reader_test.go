package wal_test

import (
	"io"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TODO Test that we skip malformed Proposals (or pb.Entry structs that aren't proposals)
// TODO Test that we skip system Proposals (types, configurations, read indexes, etc.)

func TestReader(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)
			ps := pc.createAndSaveProposals(t, tt.inputs)

			r := s.Reader("qux")
			// Drain the "string" type-registration entry the reader
			// surfaces before user-data proposals.
			if _, _, err := r.Read(); err != nil {
				t.Fatalf("drain type entry: %v", err)
			}

			for _, expected := range ps {
				_, got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}
		})
	}
}

func TestReaderSkipsSyncableIndexes(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id := "foo"
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)

			var ps []*cluster.Proposal
			for _, input := range tt.inputs {
				ps = append(ps, createProposal(input))
				ps = append(ps, createSyncableIndexProposal(t, id))
			}
			pc.saveProposals(t, ps)

			psAssert := []*cluster.Proposal{}
			for _, p := range ps {
				if !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
					psAssert = append(psAssert, p)
				}
			}

			r := s.Reader(id)
			// Drain the "string" type-registration entry the reader
			// surfaces before user-data proposals.
			if _, _, err := r.Read(); err != nil {
				t.Fatalf("drain type entry: %v", err)
			}

			for _, expected := range psAssert {
				_, got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}
		})
	}
}

func TestEOF(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"empty":  {inputs: [][]string{}},
		"simple": {inputs: [][]string{{"foo"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()
			// Register the "string" type at index 1; entity proposals
			// follow starting at index 2.
			s.RegisterType(t, "string", 1, 1)

			r := s.Reader("qux")
			// The reader surfaces the type-registration entry first
			// (Reader only filters SyncableIndex entries, not Type
			// upserts). Drain it so the user-data assertions below are
			// positional.
			if _, _, err := r.Read(); err != nil {
				t.Fatalf("drain type entry: %v", err)
			}

			ps := createProposals(tt.inputs)
			for i, p := range ps {
				saveProposal(t, p, s, 1, 2+uint64(i))

				_, got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, p, got)
			}

			_, _, err := r.Read()
			require.Equal(t, io.EOF, err)
		})
	}
}

func TestResumeReader(t *testing.T) {
	tests := map[string]struct {
		firstRead  [][]string
		secondRead [][]string
	}{
		"one-zero": {firstRead: [][]string{{"foo"}}, secondRead: [][]string{}},
		"zero-one": {firstRead: [][]string{}, secondRead: [][]string{{"foo"}}},
		"one-one":  {firstRead: [][]string{{"foo"}}, secondRead: [][]string{{"bar"}}},
		"two-one":  {firstRead: [][]string{{"foo"}, {"bar"}}, secondRead: [][]string{{"baz"}}},
		"two-two":  {firstRead: [][]string{{"foo"}, {"bar"}}, secondRead: [][]string{{"baz"}, {"qux"}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id := "qux"
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := newProposalCreatorForString(t, s)
			ps := pc.createAndSaveProposals(t, tt.firstRead)

			r := s.Reader(id)
			// Drain the "string" type-registration entry the reader
			// surfaces before user-data proposals. Track its index so
			// `index` reflects the last user-proposal index at the end
			// of the loop (the persisted syncable position after this
			// first read pass).
			var index uint64
			if i, _, err := r.Read(); err != nil {
				t.Fatalf("drain type entry: %v", err)
			} else {
				index = i
			}

			for _, expected := range ps {
				i, got, err := r.Read()
				index = i
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			pc.saveProposals(t, []*cluster.Proposal{createSyncableIndexProposalWithIndex(t, id, index)})

			ps = pc.createAndSaveProposals(t, tt.secondRead)
			r = s.Reader(id)

			for _, expected := range ps {
				_, got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}
		})
	}
}

type ProposalCreator struct {
	currentIndex uint64
	currentTerm  uint64
	s            *StorageWrapper
}

func NewProposalCreator(s *StorageWrapper) *ProposalCreator {
	return &ProposalCreator{currentIndex: uint64(6), currentTerm: uint64(6), s: s}
}

// newProposalCreatorForString registers the "string" type used by
// createProposal via the real apply path (consuming one raft index),
// then returns a ProposalCreator ready to append entity proposals after
// it. Tests using createProposal/createProposals must go through this
// constructor so that ApplyCommitted can resolve the type when the
// entity proposals are applied.
func newProposalCreatorForString(t *testing.T, s *StorageWrapper) *ProposalCreator {
	pc := NewProposalCreator(s)
	pc.currentIndex++
	s.RegisterType(t, "string", pc.currentTerm, pc.currentIndex)
	return pc
}

func (c *ProposalCreator) saveProposals(t *testing.T, ps []*cluster.Proposal) []*cluster.Proposal {
	for _, p := range ps {
		c.currentIndex += 1
		configEntry := &pb.Entry{
			Term:  c.currentTerm,
			Index: c.currentIndex,
			Type:  pb.EntryConfChange,
		}
		err := c.s.Save(defaultHardState, []pb.Entry{*configEntry}, defaultSnap)
		require.Equal(t, nil, err)
		c.currentIndex += 1
		saveProposal(t, p, c.s, c.currentTerm, c.currentIndex)
	}

	return ps
}

func (c *ProposalCreator) createAndSaveProposals(t *testing.T, inputs [][]string) []*cluster.Proposal {
	ps := createProposals(inputs)
	return c.saveProposals(t, ps)
}

func createProposals(input [][]string) []*cluster.Proposal {
	var ps []*cluster.Proposal

	for _, entities := range input {
		ps = append(ps, createProposal(entities))
	}

	return ps
}

func createProposal(input []string) *cluster.Proposal {
	proposal := &cluster.Proposal{}
	for _, entity := range input {
		logEntity := &cluster.Entity{
			// Must match what StorageWrapper.RegisterType writes, so
			// that proposals produced by this helper compare equal to
			// proposals read back after apply-path hydration.
			Type: &cluster.Type{ID: "string", Name: "string", Version: 1},
			Key:  []byte(entity),
			Data: []byte(entity),
		}
		proposal.Entities = append(proposal.Entities, logEntity)
	}
	return proposal
}

func createSyncableIndexProposal(t *testing.T, id string) *cluster.Proposal {
	return createSyncableIndexProposalWithIndex(t, id, 0)
}

func createSyncableIndexProposalWithIndex(t *testing.T, id string, index uint64) *cluster.Proposal {
	e, err := cluster.NewUpsertSyncableIndexEntity(&cluster.SyncableIndex{ID: id, Index: index})
	require.Nil(t, err)
	return &cluster.Proposal{Entities: []*cluster.Entity{e}}
}
