package db_test

import (
	"testing"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/stretchr/testify/require"
)

func TestProposals(t *testing.T) {
	one := createProposal("one", "foo")
	two := createProposal("two", "bar")
	t1 := createTypeProposal("t1")

	tests := map[string]struct {
		add                []*cluster.Proposal
		expect             []*cluster.Proposal
		amount             uint64
		typeIDsToSearchFor []string
		typeIDsToCreate    []string
	}{
		"simple": {
			add:                []*cluster.Proposal{one},
			expect:             []*cluster.Proposal{one},
			amount:             1,
			typeIDsToSearchFor: []string{"foo"},
			typeIDsToCreate:    []string{"foo"},
		},
		"first": {
			add:                []*cluster.Proposal{one, two},
			expect:             []*cluster.Proposal{two},
			amount:             10,
			typeIDsToSearchFor: []string{"bar"},
			typeIDsToCreate:    []string{"foo", "bar"},
		},
		"no type match": {
			add:                []*cluster.Proposal{one, two},
			expect:             []*cluster.Proposal{},
			amount:             0,
			typeIDsToSearchFor: []string{"foo"},
			typeIDsToCreate:    []string{"foo", "bar"},
		},
		"two": {
			add:                []*cluster.Proposal{one, two},
			expect:             []*cluster.Proposal{two, one},
			amount:             2,
			typeIDsToSearchFor: []string{"foo", "bar"},
			typeIDsToCreate:    []string{"foo", "bar"},
		},
		"work with non-proposals in between": {
			add:                []*cluster.Proposal{one, t1, two},
			expect:             []*cluster.Proposal{one},
			amount:             1,
			typeIDsToSearchFor: []string{"foo"},
			typeIDsToCreate:    []string{"foo", "bar"},
		},
		"make sure there aren't duplicates": {
			add:                []*cluster.Proposal{one, two, t1, t1},
			expect:             []*cluster.Proposal{two, one},
			amount:             2,
			typeIDsToSearchFor: []string{"foo", "bar"},
			typeIDsToCreate:    []string{"foo", "bar"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db := createDB()
			defer db.Close()

			db.EatCommitC()

			for _, id := range tc.typeIDsToCreate {
				tipe := createType(id)
				err := db.ProposeType(tipe.config)
				require.Nil(t, err)
			}

			for _, p := range tc.add {
				err := db.Propose(p)
				require.Nil(t, err)
			}

			time.Sleep(1 * time.Second)

			ps, err := db.Proposals(tc.amount, tc.typeIDsToSearchFor...)
			require.Nil(t, err)
			require.Equal(t, len(tc.expect), len(ps))
			for i := range tc.expect {
				require.Equal(t, string(tc.expect[i].Entities[0].Key), string(ps[i].Entities[0].Key))
			}
		})
	}
}

func createProposal(key string, typeID string) *cluster.Proposal {
	t := &cluster.Type{ID: typeID}

	p := &cluster.Proposal{Entities: []*cluster.Entity{
		{Type: t, Key: []byte(key), Data: []byte("")},
	}}

	return p
}

func createTypeProposal(typeID string) *cluster.Proposal {
	t := &cluster.Type{ID: typeID}

	// Swallow the error
	e, _ := cluster.NewUpsertTypeEntity(t)

	return &cluster.Proposal{Entities: []*cluster.Entity{e}}
}
