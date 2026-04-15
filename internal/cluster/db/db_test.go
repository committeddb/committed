package db_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/dbfakes"
	parser "github.com/philborlin/committed/internal/cluster/db/parser"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// testCtx returns a context with a generous deadline. PR2's blocking
// Propose needs *some* deadline so a hung test fails fast instead of
// hanging indefinitely. 5s is comfortably above any sane single-node
// commit + apply latency.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

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
				err := db.Propose(testCtx(t), p)
				if err != nil {
					t.Fatal(err)
				}
				// No more <-db.CommitC: blocking Propose returns after
				// apply, so the entry is already in storage by here.
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
				err := db.ProposeType(testCtx(t), tipe.config)
				require.Nil(t, err)
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

func TestResolveType(t *testing.T) {
	expected := createType("foo")

	// FakeStorage must return a sane FirstIndex BEFORE the DB is constructed:
	// db.New synchronously starts a Raft node which calls FirstIndex during
	// bootstrap. With the default zero return, etcd's raftLog computes
	// committed = firstIndex - 1 = MaxUint64 and panics. (Previously this
	// "worked" only because startRaft ran in a goroutine that hadn't been
	// scheduled by the time the test finished.)
	s := &dbfakes.FakeStorage{}
	s.FirstIndexReturns(1, nil)
	s.ResolveTypeReturns(expected.tipe, nil)

	db := createDBWithStorage(s)
	defer db.Close()

	got, err := db.ResolveType(cluster.LatestTypeRef(expected.tipe.ID))
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, expected.tipe, got)
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
		// Version is system-assigned by ProposeType (1 on first PUT).
		// Tests that load the type back out of the log will see Version 1.
		tipe: &cluster.Type{ID: name, Name: name, Version: 1},
		config: &cluster.Configuration{
			ID:       name,
			MimeType: "text/toml",
			Data:     []byte(toml),
		},
	}
}

func TestParseType(t *testing.T) {
	t.Run("without schema", func(t *testing.T) {
		cfg := &cluster.Configuration{
			ID:       "t1",
			MimeType: "text/toml",
			Data:     []byte("[type]\nname = \"Person\""),
		}

		name, typ, err := db.ParseType(cfg, nil)
		require.Nil(t, err)
		require.Equal(t, "Person", name)
		require.Equal(t, "t1", typ.ID)
		require.Empty(t, typ.SchemaType)
		require.Empty(t, typ.Schema)
		require.Equal(t, cluster.NoValidation, typ.Validate)
	})

	t.Run("with JSONSchema", func(t *testing.T) {
		schema := `{"type":"object","required":["name"]}`
		toml := fmt.Sprintf("[type]\nname = \"Person\"\nschemaType = \"JSONSchema\"\nvalidate = 1\nschema = '%s'", schema)

		cfg := &cluster.Configuration{
			ID:       "t2",
			MimeType: "text/toml",
			Data:     []byte(toml),
		}

		name, typ, err := db.ParseType(cfg, nil)
		require.Nil(t, err)
		require.Equal(t, "Person", name)
		require.Equal(t, "JSONSchema", typ.SchemaType)
		require.Equal(t, schema, string(typ.Schema))
		require.Equal(t, cluster.ValidateSchema, typ.Validate)
	})

	t.Run("validate enabled but no schemaType", func(t *testing.T) {
		toml := "[type]\nname = \"Bad\"\nvalidate = 1\nschema = '{\"type\":\"object\"}'"

		cfg := &cluster.Configuration{
			ID:       "t3",
			MimeType: "text/toml",
			Data:     []byte(toml),
		}

		_, _, err := db.ParseType(cfg, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schemaType is not set")
	})

	t.Run("validate enabled but no schema", func(t *testing.T) {
		toml := "[type]\nname = \"Bad\"\nschemaType = \"JSONSchema\"\nvalidate = 1"

		cfg := &cluster.Configuration{
			ID:       "t4",
			MimeType: "text/toml",
			Data:     []byte(toml),
		}

		_, _, err := db.ParseType(cfg, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema is empty")
	})

	t.Run("schema present but validation disabled", func(t *testing.T) {
		schema := `{"type":"object"}`
		toml := fmt.Sprintf("[type]\nname = \"Loose\"\nschemaType = \"JSONSchema\"\nschema = '%s'", schema)

		cfg := &cluster.Configuration{
			ID:       "t5",
			MimeType: "text/toml",
			Data:     []byte(toml),
		}

		_, typ, err := db.ParseType(cfg, nil)
		require.Nil(t, err)
		require.Equal(t, "JSONSchema", typ.SchemaType)
		require.Equal(t, cluster.NoValidation, typ.Validate)
	})

	t.Run("with Protobuf", func(t *testing.T) {
		proto := "syntax = \\\"proto3\\\"; message Person { string name = 1; }"
		toml := fmt.Sprintf("[type]\nname = \"Person\"\nschemaType = \"Protobuf\"\nvalidate = 1\nschema = \"%s\"", proto)

		cfg := &cluster.Configuration{
			ID:       "t6",
			MimeType: "text/toml",
			Data:     []byte(toml),
		}

		_, typ, err := db.ParseType(cfg, nil)
		require.Nil(t, err)
		require.Equal(t, "Protobuf", typ.SchemaType)
		require.Equal(t, cluster.ValidateSchema, typ.Validate)
		require.Contains(t, string(typ.Schema), "message Person")
	})
}

func createProposalsAndProposeThem(t *testing.T, db *DB, inputs [][]string) []*cluster.Proposal {
	ps := createProposals(inputs)
	for _, p := range ps {
		require.Equal(t, nil, db.Propose(testCtx(t), p))
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

// testTickInterval is the Raft tick used by every test in this package.
// Combined with the (still hard-coded) ElectionTick=10, it makes single-node
// leader election complete in ~10ms instead of ~1s. Without this, every test
// pays a one-second startup tax for the first proposal to be committed.
const testTickInterval = 1 * time.Millisecond

func createDBWithStorage(s db.Storage) *DB {
	id := uint64(1)
	// Empty local-peer URL skips the rafthttp listener bind. Single-node
	// tests have no peers to receive from, so taking a port (and the
	// cross-package collision risk that comes with it) buys us nothing.
	peers := make(db.Peers)
	peers[id] = ""
	parser := parser.New()

	d := db.New(id, peers, s, parser, nil, nil, db.WithTickInterval(testTickInterval))
	return &DB{d, s, peers, id}
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

	var ents []raftpb.Entry
	// Make sure storage is not empty
	if li+1 > fi {
		ents, err = db.storage.Entries(fi, li+1, 10000)
		if err != nil {
			return nil, err
		}
	}

	ps := db.entsToProposals(ents)

	return ps, nil
}

func (db *DB) entsToProposals(ents []raftpb.Entry) []*cluster.Proposal {
	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal && e.Data != nil {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(e.Data, db.storage); err != nil {
				continue
			}
			ps = append(ps, p)
		}
	}

	return ps
}

// MemorySyncable is a test syncable that records every proposal it sees.
//
// All state is guarded by mu because Sync() runs on the DB's internal sync
// goroutine while tests inspect count/proposals from the test goroutine
// (directly or via require.Eventually's polling goroutine).
type MemorySyncable struct {
	mu          sync.Mutex
	proposals   []*cluster.Proposal
	cancel      func()
	count       int
	doneAtCount int
}

func NewSyncable(doneAtCount int, cancel func()) *MemorySyncable {
	return &MemorySyncable{doneAtCount: doneAtCount, cancel: cancel}
}

// Count returns the number of proposals Sync has been called with so far.
// Safe to call concurrently with Sync.
func (ms *MemorySyncable) Count() int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.count
}

// Proposals returns a snapshot of all proposals Sync has received. Safe to
// call concurrently with Sync.
func (ms *MemorySyncable) Proposals() []*cluster.Proposal {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	out := make([]*cluster.Proposal, len(ms.proposals))
	copy(out, ms.proposals)
	return out
}

func (ms *MemorySyncable) Init(ctx context.Context) error {
	return nil
}

func (ms *MemorySyncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	fmt.Printf("syncing: %v\n", p)

	ms.mu.Lock()
	ms.count++
	ms.proposals = append(ms.proposals, p)
	done := ms.doneAtCount == ms.count
	ms.mu.Unlock()

	if done {
		ms.cancel()
	}

	shouldSnapshot := true
	for _, e := range p.Entities {
		if cluster.IsSystem(e.Type.ID) {
			shouldSnapshot = false
		}
	}

	return cluster.ShouldSnapshot(shouldSnapshot), nil
}

func (ms *MemorySyncable) Close() error {
	return nil
}

// MemoryBatchSyncable extends MemorySyncable with BatchSyncable support.
// db.sync will detect the SyncBatch method and use the batching path.
type MemoryBatchSyncable struct {
	MemorySyncable
	batchMu     sync.Mutex
	batchSizes  []int
}

func NewBatchSyncable(doneAtCount int, cancel func()) *MemoryBatchSyncable {
	return &MemoryBatchSyncable{
		MemorySyncable: MemorySyncable{doneAtCount: doneAtCount, cancel: cancel},
	}
}

func (ms *MemoryBatchSyncable) SyncBatch(ctx context.Context, ps []*cluster.Proposal) (bool, error) {
	fmt.Printf("batch syncing: %d proposals\n", len(ps))

	ms.mu.Lock()
	ms.batchMu.Lock()
	ms.batchSizes = append(ms.batchSizes, len(ps))
	ms.batchMu.Unlock()
	for _, p := range ps {
		ms.count++
		ms.proposals = append(ms.proposals, p)
	}
	done := ms.count >= ms.doneAtCount
	ms.mu.Unlock()

	if done {
		ms.cancel()
	}

	return true, nil
}

// BatchSizes returns a snapshot of the sizes of each batch that was
// flushed via SyncBatch.
func (ms *MemoryBatchSyncable) BatchSizes() []int {
	ms.batchMu.Lock()
	defer ms.batchMu.Unlock()
	out := make([]int, len(ms.batchSizes))
	copy(out, ms.batchSizes)
	return out
}
