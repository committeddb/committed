package db_test

import (
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster/db"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestRaftPropose(t *testing.T) {
	tests := map[string]struct {
		clusterSize int
		inputs      []string
	}{
		"simple": {clusterSize: 1, inputs: []string{"a/b/c"}},
		"two":    {clusterSize: 1, inputs: []string{"a/b/c", "foo"}},
		// "cluster3": {clusterSize: 3, inputs: []string{"a/b/c", "foo"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rafts := createRafts(tc.clusterSize)
			defer rafts.Close()

			r := rafts[0]

			for _, input := range tc.inputs {
				proposeAndCheck(t, r, input)
			}

			offsetToFirstProposal := uint64(3)
			for _, r := range rafts {
				s := r.storage

				ents, err := s.Entries(offsetToFirstProposal, offsetToFirstProposal+uint64(len(tc.inputs)), 5000)
				if err != nil {
					t.Fatal(err)
				}

				for i, e := range ents {
					diff := cmp.Diff([]byte(tc.inputs[i]), e.Data)
					if diff != "" {
						t.Fatalf(diff)
					}
				}
			}
		})
	}
}

func TestRaftRestart(t *testing.T) {
	tests := map[string]struct {
		clusterSize int
		inputs1     []string
		inputs2     []string
	}{
		"simple": {clusterSize: 1, inputs1: []string{"foo", "bar"}, inputs2: []string{"baz"}},
		// "cluster3": {clusterSize: 3, inputs1: []string{"foo"}, inputs2: []string{"bar"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rafts := createRafts(tc.clusterSize)
			defer rafts.Close()

			r := rafts[0]

			for _, input := range tc.inputs1 {
				propose(r, input)
			}

			lastIndex(t, r)

			err := rafts[0].Restart()
			if err != nil {
				t.Fatal(err)
			}

			lastIndex(t, r)

			r = rafts[0]

			hs, cs, err := r.storage.InitialState()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("hard state: %v, conf state: %v\n", hs, cs)

			for _, input := range tc.inputs2 {
				propose(r, input)
			}

			c := <-r.commitC
			fmt.Printf("committed: %s\n", string(c))
			c = <-r.commitC
			fmt.Printf("committed: %s\n", string(c))

			lastIndex(t, rafts[0])

			// offsetToFirstProposal := uint64(3)
			for _, r := range rafts {
				inputs := slices.Concat(tc.inputs1, tc.inputs2)

				es, err := r.ents()
				if err != nil {
					t.Fatal(err)
				}

				for i, e := range es {
					diff := cmp.Diff(inputs[i], string(e.Data))
					if diff != "" {
						t.Fatalf(diff)
					}
				}
			}
		})
	}
}

func proposeAndCheck(t *testing.T, r *Raft, input string) {
	r.proposeC <- []byte(input)
	got := <-r.commitC
	diff := cmp.Diff([]byte(input), got)
	if diff != "" {
		t.Fatalf(diff)
	}
}

func propose(r *Raft, input string) {
	fmt.Printf("proposing: %s\n", input)
	r.proposeC <- []byte(input)
	c := <-r.commitC
	fmt.Printf("committed: %s\n", string(c))
}

func lastIndex(t *testing.T, r *Raft) uint64 {
	li, err := r.storage.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("LastIndex: %d\n", li)

	return li
}

func createRafts(replicas int) Rafts {
	var rafts Rafts

	var peers []raft.Peer
	for id := uint64(1); id <= uint64(replicas); id++ {
		port := id*10000 + 2379
		context := fmt.Sprintf("http://127.0.0.1:%d", port)
		peers = append(peers, raft.Peer{ID: id, Context: []byte(context)})

		fmt.Println(id)
	}

	for _, p := range peers {
		s := &MemoryStorage{raft.NewMemoryStorage()}
		rafts = append(rafts, createRaft(p.ID, peers, s))
	}

	return rafts
}

func createRaft(id uint64, peers []raft.Peer, s db.Storage) *Raft {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	commitC, errorC, closer := db.NewRaft(id, peers, s, proposeC, confChangeC)

	return &Raft{
		storage:     s,
		commitC:     commitC,
		errorC:      errorC,
		closer:      closer,
		peers:       peers,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		id:          id,
	}
}

type Raft struct {
	storage     db.Storage
	peers       []raft.Peer
	commitC     <-chan []byte
	errorC      <-chan error
	closer      io.Closer
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	id          uint64
}

func (rs *Raft) Restart() error {
	err := rs.closer.Close()
	if err != nil {
		return err
	}

	r := createRaft(rs.id, rs.peers, rs.storage)
	rs.storage = r.storage
	rs.peers = r.peers
	rs.commitC = r.commitC
	rs.errorC = r.errorC
	rs.closer = r.closer
	rs.proposeC = r.proposeC
	rs.confChangeC = r.confChangeC
	rs.id = r.id

	return nil
}

func (rs *Raft) ents() ([]raftpb.Entry, error) {
	fi, err := rs.storage.FirstIndex()
	if err != nil {
		return nil, err
	}

	li, err := rs.storage.LastIndex()
	if err != nil {
		return nil, err
	}

	ents, err := rs.storage.Entries(fi, li, 10000)
	if err != nil {
		return nil, err
	}

	var es []raftpb.Entry
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			es = append(es, e)
		}
	}

	return es, nil
}

type Rafts []*Raft

func (rs Rafts) Close() error {
	var err error

	for _, r := range rs {
		ierr := r.closer.Close()
		if ierr != nil {
			err = ierr
		}
	}

	return err
}

type MemoryStorage struct {
	*raft.MemoryStorage
}

func (ms *MemoryStorage) Close() error {
	return nil
}

func (ms *MemoryStorage) Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
	err := ms.Append(ents)
	if err != nil {
		return err
	}

	return ms.SetHardState(st)
}
