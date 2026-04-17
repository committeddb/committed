package db_test

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
			// ProposeType depends on real storage state (version numbering,
			// migration checks), so use wal-backed storage.
			d, _ := newWalDB(t)

			ctx := testCtx(t)
			for _, id := range tc.typeIDsToCreate {
				tipe := createType(id)
				err := d.ProposeType(ctx, tipe.config)
				require.Nil(t, err)
			}

			for _, p := range tc.add {
				err := d.Propose(ctx, p)
				require.Nil(t, err)
			}

			ps, err := d.Proposals(tc.amount, tc.typeIDsToSearchFor...)
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

func TestProposals_UnmarshalError(t *testing.T) {
	s := &dbfakes.FakeStorage{}
	s.FirstIndexReturns(1, nil)
	s.LastIndexReturns(2, nil)
	s.EntriesReturns([]raftpb.Entry{
		{Type: raftpb.EntryNormal, Data: []byte("not valid protobuf")},
	}, nil)

	db := createDBWithStorage(s)
	defer db.Close()

	ps, err := db.Proposals(1)
	require.Error(t, err)
	require.Nil(t, ps)
}

func createTypeProposal(typeID string) *cluster.Proposal {
	t := &cluster.Type{ID: typeID}

	// Swallow the error
	e, _ := cluster.NewUpsertTypeEntity(t)

	return &cluster.Proposal{Entities: []*cluster.Entity{e}}
}
