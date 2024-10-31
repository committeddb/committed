package db_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/stretchr/testify/require"
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
					t.Fatal(diff)
				}
			}
		})
	}
}

func TestProposeType(t *testing.T) {
	tests := map[string]struct {
		types []*Type
	}{
		"simple": {types: []*Type{createType("foo")}},
		"two":    {types: []*Type{createType("foo"), createType("bar")}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db := createDB()
			defer db.Close()

			for _, tipe := range tc.types {
				err := db.ProposeType(tipe.config)
				require.Nil(t, err)
				<-db.CommitC
			}

			ents, err := db.ents()
			require.Nil(t, err)

			offset := len(ents) - len(tc.types)
			for i := range ents {
				tipe := &cluster.Type{}

				e := ents[offset+i]
				err := tipe.Unmarshal(e.Entities[0].Data)
				require.Nil(t, err)

				// The ID is a generated ID that we can't predict ahead of time. Just copy over and trust...
				tc.types[i].tipe.ID = tipe.ID
				require.Equal(t, tc.types[i].tipe, tipe)
			}
		})
	}
}

func TestType(t *testing.T) {
	s := &dbfakes.FakeStorage{}
	db := createDBWithStorage(s)
	defer db.Close()

	expected := createType("foo")
	s.TypeReturns(expected.tipe, nil)
	s.FirstIndexReturns(1, nil)

	got, err := db.Type(expected.tipe.ID)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, expected.tipe, got)
}

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

			ents, err := db.ents()
			require.Nil(t, err)
			require.Equal(t, len(ents), size)

			for i, p := range ents {
				got := cluster.IsIngestablePosition(p.Entities[0].Type.ID)
				expected := i == size-1
				require.Equal(t, expected, got)
			}
		})
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
			s := NewMemoryStorage()
			db := createDBWithStorage(s)
			defer db.Close()

			size := len(tc.inputs)

			ctx, cancel := context.WithCancel(context.Background())

			ps := createProposals(tc.inputs)
			for _, p := range ps {
				require.Equal(t, nil, db.Propose(p))
				<-db.CommitC
			}

			syncable := NewSyncable(size, cancel)
			err := db.Sync(ctx, id, syncable)
			require.Nil(t, err)
			for i := 0; i < len(tc.inputs); i++ {
				<-db.CommitC
			}
			require.Equal(t, size, syncable.count)

			ps, err = db.ents()
			require.Nil(t, err)
			require.Equal(t, len(tc.inputs)*2, len(ps))

			for i, p := range ps {
				got := cluster.IsSyncableIndex(p.Entities[0].Type.ID)
				expected := i >= len(tc.inputs)
				require.Equal(t, expected, got)
			}
		})
	}
}

func TestResumeSync(t *testing.T) {
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

			size := len(tc.inputs)

			ps := createProposalsAndProposeThem(t, db, tc.inputs)
			ctx, cancel := context.WithCancel(context.Background())
			syncable := NewSyncable(size, cancel)

			_ = db.Sync(ctx, id, syncable)
			for i := 0; i < len(tc.inputs); i++ {
				<-db.CommitC
			}

			inputs2 := modifyInputs(tc.inputs)

			ps2 := createProposalsAndProposeThem(t, db, inputs2)
			ctx, cancel = context.WithCancel(context.Background())
			syncable = NewSyncable(size, cancel)

			storage.indexes[id] = getLastIndex(storage, ps)

			_ = db.Sync(ctx, id, syncable)
			for i := 0; i < len(inputs2); i++ {
				<-db.CommitC
			}

			require.Equal(t, size, syncable.count)

			for i, p := range storage.Proposals() {
				fmt.Printf("Testing [%v] - %v", i, p)
				if i < size {
					require.False(t, cluster.IsSystem(p.Entities[0].Type.ID))
					require.Equal(t, ps[i%size], p)
				} else if i >= size*2 && i < size*3 {
					require.False(t, cluster.IsSystem(p.Entities[0].Type.ID))
					require.Equal(t, ps2[i%size], p)
				} else {
					require.True(t, cluster.IsSystem(p.Entities[0].Type.ID))
				}
			}

			for i, got := range syncable.proposals {
				require.False(t, cluster.IsSystem(got.Entities[0].Type.ID))
				require.Equal(t, ps2[i], got)
			}
		})
	}
}

// TODO Test deletes - may have to test with a syncable because a delete doesn't have context except when read

func getLastIndex(s db.Storage, ps []*cluster.Proposal) uint64 {
	var i uint64
	var got *cluster.Proposal
	r := s.Reader("storage")

	for _, expected := range ps {
		for {
			i, got, _ = r.Read()
			if reflect.DeepEqual(expected, got) {
				break
			}
		}
	}

	return i
}

func modifyInputs(is [][]string) [][]string {
	var newInput [][]string
	for _, outer := range is {
		var newOuter []string
		for _, inner := range outer {
			newOuter = append(newOuter, inner+"'")
		}
		newInput = append(newInput, newOuter)
	}

	return newInput
}

type Type struct {
	tipe   *cluster.Type
	config *cluster.Configuration
}

func createType(name string) *Type {
	toml := fmt.Sprintf("[type]\nname = \"%s\"", name)
	fmt.Println(toml)

	return &Type{
		tipe: &cluster.Type{ID: name, Name: name},
		config: &cluster.Configuration{
			ID:       name,
			MimeType: "text/toml",
			Data:     []byte(toml),
		},
	}
}

func createProposalsAndProposeThem(t *testing.T, db *DB, inputs [][]string) []*cluster.Proposal {
	ps := createProposals(inputs)
	for _, p := range ps {
		require.Equal(t, nil, db.Propose(p))
		<-db.CommitC
	}

	return ps
}

func createProposals(input [][]string) []*cluster.Proposal {
	var ps []*cluster.Proposal

	for fi, entities := range input {
		fmt.Printf("Entities: %v\n", entities)
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
	return createDBWithStorage(NewMemoryStorage())
}

func createDBWithStorage(s db.Storage) *DB {
	id := uint64(1)
	url := fmt.Sprintf("http://127.0.0.1:%d", 12379)
	peers := make(db.Peers)
	peers[id] = url
	parser := parser.New()

	db := db.New(id, peers, s, parser, nil)
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

	ps := db.entsToProposals(ents)

	return ps, nil
}

func (db *DB) entsToProposals(ents []raftpb.Entry) []*cluster.Proposal {
	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			_ = p.Unmarshal(e.Data)
			ps = append(ps, p)
		}
	}

	return ps
}

type MemoryIngestable struct {
	proposals    []*cluster.Proposal
	positions    []cluster.Position
	cancel       func()
	proposalChan chan *cluster.Proposal
	positionChan chan cluster.Position
}

func NewIngestable(proposals []*cluster.Proposal, positions []cluster.Position, cancel func()) *MemoryIngestable {
	proposalChan := make(chan *cluster.Proposal)
	positionChan := make(chan cluster.Position)
	return &MemoryIngestable{
		proposals:    proposals,
		positions:    positions,
		cancel:       cancel,
		proposalChan: proposalChan,
		positionChan: positionChan,
	}
}

func (mi *MemoryIngestable) Ingest(pos cluster.Position) (<-chan *cluster.Proposal, <-chan cluster.Position, error) {
	go func() {
		for _, p := range mi.proposals {
			mi.proposalChan <- p
		}
		for _, p := range mi.positions {
			mi.positionChan <- p
		}
	}()

	return mi.proposalChan, mi.positionChan, nil
}

func (mi *MemoryIngestable) Close() error {
	return nil
}

type MemorySyncable struct {
	proposals   []*cluster.Proposal
	cancel      func()
	count       int
	doneAtCount int
}

func NewSyncable(doneAtCount int, cancel func()) *MemorySyncable {
	return &MemorySyncable{doneAtCount: doneAtCount, cancel: cancel}
}

func (ms *MemorySyncable) Init(ctx context.Context) error {
	return nil
}

func (ms *MemorySyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	fmt.Printf("syncing: %v\n", p)

	ms.count++
	ms.proposals = append(ms.proposals, p)
	if ms.doneAtCount == ms.count {
		ms.cancel()
	}

	var shouldSnapshot = cluster.ShouldSnapshot(len(p.Entities) == 1 && cluster.IsSystem(p.Entities[0].Type.ID))

	return shouldSnapshot, nil
}

func (ms *MemorySyncable) Close() error {
	return nil
}
