package wal_test

import (
	"fmt"
	"io"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

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

			currentIndex := uint64(6)
			currentTerm := uint64(6)
			ps := createProposals(tt.inputs)
			for i, p := range ps {
				configEntry := &pb.Entry{
					Term:  currentTerm,
					Index: currentIndex + uint64(i*2),
					Type:  pb.EntryConfChange,
				}
				err := s.Save(defaultHardState, []pb.Entry{*configEntry}, defaultSnap)
				require.Equal(t, nil, err)
				saveProposal(t, p, s, currentTerm, currentIndex+uint64(i*2+1))
			}

			r := s.Reader("foo")

			for _, expected := range ps {
				got, err := r.Read()
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

			r := s.Reader("foo")

			ps := createProposals(tt.inputs)
			for i, p := range ps {
				saveProposal(t, p, s, 1, 1+uint64(i))

				got, err := r.Read()
				require.Equal(t, nil, err)
				require.Equal(t, p, got)
			}

			_, err := r.Read()
			require.Equal(t, io.EOF, err)
		})
	}
}

// TODO Test that we skip malformed Proposals (or pb.Entry structs that aren't proposals)
