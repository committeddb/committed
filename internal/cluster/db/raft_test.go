package db_test

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
)

func TestRaft(t *testing.T) {
	tests := map[string]struct {
		input []byte
	}{
		"simple": {input: []byte("a/b/c")},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s, r := createRafts(1)
			r.Propose(tc.input)

			got := <-r.CommitC
			diff := cmp.Diff(tc.input, got)
			if diff != "" {
				t.Fatalf(diff)
			}

			h, es, snap := s.SaveArgsForCall(0)

			fmt.Println(h)
			for e := range es {
				fmt.Println(e)
			}
			fmt.Println(snap)
		})
	}
}

func createRafts(replicas int) (*dbfakes.FakeStorage, *db.DB) {
	var s *dbfakes.FakeStorage
	var r *db.DB

	peers := make(db.Peers)

	// var ids []int
	// var peers []string
	for id := 1; id <= replicas; id++ {
		port := id*10000 + 2379
		peers[uint64(id)] = fmt.Sprintf("http://127.0.0.1:%d", port)

		fmt.Println(uint64(id))
		// ids = append(ids, id)
		// peers = append(peers, fmt.Sprintf("http://127.0.0.1:%d", port))
	}

	for id := range peers {
		s = &dbfakes.FakeStorage{}
		r = db.New(id, peers, s)
	}

	// for _, id := range ids {
	// 	s = &dbfakes.FakeStorage{}
	// 	r = db.New(id, peers, s)
	// }

	return s, r
}
