package db_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func TestDBPropose(t *testing.T) {
	tests := map[string]struct {
		inputs [][]string
	}{
		"simple":  {inputs: [][]string{{"foo"}}},
		"two":     {inputs: [][]string{{"foo", "bar"}}},
		"two-one": {inputs: [][]string{{"foo", "bar"}, {"baz"}}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db := createDB()
			defer db.Close()

			ps := createProposals(tc.inputs)

			for _, p := range ps {
				err := db.Propose(p)
				if err != nil {
					t.Fatal(err)
				}
				<-db.CommitC
			}

			ents, err := db.ents()
			if err != nil {
				t.Fatal(err)
			}

			for i, p := range ps {
				diff := cmp.Diff(p, ents[i])
				if diff != "" {
					t.Fatalf(diff)
				}
			}
		})
	}
}

func TestProposeType(t *testing.T) {
	tests := map[string]struct {
		types []*cluster.Type
	}{
		"simple": {types: []*cluster.Type{createType("foo")}},
		"two":    {types: []*cluster.Type{createType("foo"), createType("bar")}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db := createDB()
			defer db.Close()

			for _, tipe := range tc.types {
				err := db.ProposeType(tipe)
				if err != nil {
					t.Fatal(err)
				}
				<-db.CommitC
			}

			ents, err := db.ents()
			if err != nil {
				t.Fatal(err)
			}

			offset := len(ents) - len(tc.types)
			for i := range ents {
				tipe := &cluster.Type{}

				e := ents[offset+i]
				err := tipe.Unmarshal(e.Entities[0].Data)
				if err != nil {
					t.Fatal(err)
				}

				diff := cmp.Diff(tc.types[i], tipe)
				if diff != "" {
					t.Fatalf(diff)
				}
			}
		})
	}
}

func TestType(t *testing.T) {
	s := &dbfakes.FakeStorage{}
	db := createDBWithStorage(s)
	defer db.Close()

	expected := createType("foo")
	s.TypeReturns(expected, nil)
	s.FirstIndexReturns(1, nil)

	got, err := db.Type(expected.ID)
	if err != nil {
		t.Fatal(err)
	}

	diff := cmp.Diff(expected, got)
	if diff != "" {
		t.Fatalf(diff)
	}
}

func TestSync(t *testing.T) {
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
			s := &dbfakes.FakeStorage{}
			db := createDBWithStorage(s)
			defer db.Close()

			size := len(tc.inputs)

			s.FirstIndexReturns(1, nil)
			s.LastIndexReturns(uint64(size), nil)

			ctx, cancel := context.WithCancel(context.Background())

			ps := createProposals(tc.inputs)
			sync := NewSyncable(size, cancel)
			r := &TestReader{proposals: ps}
			s.ReaderReturns(r)

			go db.Sync(ctx, id, sync)
			<-sync.done

			diff := cmp.Diff(size, sync.count)
			if diff != "" {
				t.Fatalf(diff)
			}

			i := 0
			for _, got := range sync.proposals {
				if !cluster.IsSystem(got.Entities[0].Type.ID) {
					diff := cmp.Diff(ps[i], got)
					if diff != "" {
						t.Fatalf(diff)
					}
					i++
				}
			}
		})
	}
}

// TODO Test deletes - may have to test with a syncable because a delete doesn't have context except when read

func createType(name string) *cluster.Type {
	return &cluster.Type{ID: name, Name: name}
}

func createProposals(input [][]string) []*cluster.Proposal {
	var ps []*cluster.Proposal

	for fi, entities := range input {
		fmt.Printf("Entities: %v", entities)
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

func createDB() *DB {
	return createDBWithStorage(&MemoryStorage{raft.NewMemoryStorage()})
}

func createDBWithStorage(s db.Storage) *DB {
	id := uint64(1)
	url := fmt.Sprintf("http://127.0.0.1:%d", 12379)
	peers := make(db.Peers)
	peers[id] = url
	parser := parser.New()

	db := db.New(id, peers, s, parser)
	return &DB{db, s, peers, id}
}

type DB struct {
	*db.DB
	storage db.Storage
	peers   db.Peers
	id      uint64
}

func (db *DB) ents() ([]*cluster.Proposal, error) {
	fi, err := db.storage.FirstIndex()
	if err != nil {
		return nil, err
	}

	li, err := db.storage.LastIndex()
	if err != nil {
		return nil, err
	}

	ents, err := db.storage.Entries(fi, li+1, 10000)
	if err != nil {
		return nil, err
	}

	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			p.Unmarshal(e.Data)
			ps = append(ps, p)
		}
	}

	return ps, nil
}

type MemorySyncable struct {
	proposals   []*cluster.Proposal
	cancel      func()
	count       int
	done        chan any
	doneAtCount int
}

func NewSyncable(doneAtCount int, cancel func()) *MemorySyncable {
	return &MemorySyncable{doneAtCount: doneAtCount, cancel: cancel, done: make(chan any)}
}

func (ms *MemorySyncable) Init(ctx context.Context) error {
	return nil
}

func (ms *MemorySyncable) Sync(ctx context.Context, p *cluster.Proposal) error {
	fmt.Printf("syncing: %v\n", p)

	ms.count++
	ms.proposals = append(ms.proposals, p)
	if ms.doneAtCount == ms.count {
		ms.cancel()
		ms.done <- ""
	}

	return nil
}

func (ms *MemorySyncable) Close() error {
	return nil
}

type TestReader struct {
	nextIndex int
	proposals []*cluster.Proposal
}

func (tr *TestReader) Read() (uint64, *cluster.Proposal, error) {
	if tr.nextIndex >= len(tr.proposals) {
		return 0, nil, io.EOF
	}

	p := tr.proposals[tr.nextIndex]
	tr.nextIndex++
	if p == nil {
		return 0, nil, io.EOF
	}

	return uint64(tr.nextIndex - 1), p, nil
}
