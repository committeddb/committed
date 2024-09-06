package wal_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

// TODO Test that we skip malformed Proposals (or pb.Entry structs that aren't proposals)

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

			pc := NewProposalCreator(s)
			ps := pc.saveProposal(t, tt.inputs)

			r := s.Reader(0)

			for _, expected := range ps {
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

			r := s.Reader(0)

			ps := createProposals(tt.inputs)
			for i, p := range ps {
				saveProposal(t, p, s, 1, 1+uint64(i))

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
			s := NewStorage(t, nil)
			defer s.Cleanup()

			pc := NewProposalCreator(s)
			ps := pc.saveProposal(t, tt.firstRead)

			r := s.Reader(0)

			var index uint64
			for _, expected := range ps {
				i, got, err := r.Read()
				index = i
				require.Equal(t, nil, err)
				require.Equal(t, expected, got)
			}

			ps = pc.saveProposal(t, tt.secondRead)
			r = s.Reader(index)

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

func (c *ProposalCreator) saveProposal(t *testing.T, inputs [][]string) []*cluster.Proposal {
	ps := createProposals(inputs)
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

func createProposals(input [][]string) []*cluster.Proposal {
	var ps []*cluster.Proposal

	for fi, entities := range input {
		proposal := &cluster.Proposal{}
		for si, entity := range entities {
			logEntity := &cluster.Entity{
				Type: &cluster.Type{ID: "string"},
				Key:  []byte(fmt.Sprintf("%d-%d", fi, si)),
				Data: []byte(entity),
			}
			proposal.Entities = append(proposal.Entities, logEntity)
		}
		ps = append(ps, proposal)
	}

	return ps
}
