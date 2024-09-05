package wal_test

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/philborlin/committed/internal/cluster/db/wal"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

var defaultHardState pb.HardState = pb.HardState{}
var defaultSnap pb.Snapshot = pb.Snapshot{
	Data: nil,
	Metadata: pb.SnapshotMetadata{
		ConfState: pb.ConfState{
			Voters:         []uint64{},
			Learners:       []uint64{},
			VotersOutgoing: []uint64{},
			LearnersNext:   []uint64{},
			AutoLeave:      false,
		},
		Index: 0,
		Term:  0,
	},
}

type SyncableConfig struct {
	Details *Details `json:"syncable"`
}

type DatabaseConfig struct {
	Details *Details `json:"database"`
}

type Details struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// index is a helper type for generating slices of pb.Entry. The value of index
// is the first entry index in the generated slices.
type index uint64

// terms generates a slice of entries at indices [index, index+len(terms)), with
// the given terms of each entry. Terms must be non-decreasing.
func (i index) terms(terms ...uint64) []pb.Entry {
	index := uint64(i)
	entries := make([]pb.Entry, 0, len(terms))
	for _, term := range terms {
		entries = append(entries, pb.Entry{Term: term, Index: index})
		index++
	}
	return entries
}

type StorageWrapper struct {
	*wal.Storage
	path   string
	parser db.Parser
}

func (s *StorageWrapper) CloseAndReopen() (*StorageWrapper, error) {
	err := s.Close()
	if err != nil {
		return nil, err
	}

	wal, err := wal.Open(s.path, s.parser)
	if err != nil {
		return nil, err
	}

	return &StorageWrapper{wal, s.path, s.parser}, nil
}

func (s *StorageWrapper) Cleanup() error {
	_ = s.Close()
	return os.RemoveAll(s.path)
}

func (s *StorageWrapper) ents(t *testing.T) []pb.Entry {
	fi, err := s.EntryLog.FirstIndex()
	if err != nil {
		t.Error(err)
		return nil
	}

	li, err := s.EntryLog.LastIndex()
	if err != nil {
		t.Error(err)
		return nil
	}

	var es []pb.Entry
	for i := fi; i <= li; i++ {
		data, err := s.EntryLog.Read(i)
		if err != nil {
			t.Error(err)
			return nil
		}

		e := pb.Entry{}
		err = e.Unmarshal(data)
		if err != nil {
			t.Error(err)
			return nil
		}

		es = append(es, e)
	}

	return es
}

func (s *StorageWrapper) states(t *testing.T) []wal.State {
	fi, err := s.StateLog.FirstIndex()
	if err != nil {
		t.Error(err)
		return nil
	}

	li, err := s.StateLog.LastIndex()
	if err != nil {
		t.Error(err)
		return nil
	}

	var es []wal.State
	for i := fi; i <= li; i++ {
		data, err := s.StateLog.Read(i)
		if err != nil {
			t.Error(err)
			return nil
		}

		e := wal.State{}
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		if err := dec.Decode(&e); err != nil {
			t.Error(err)
			return nil
		}

		es = append(es, e)
	}

	return es
}

func NewStorage(t *testing.T, ents []pb.Entry) *StorageWrapper {
	return NewStorageWithParser(t, ents, nil)
}

func NewStorageWithParser(t *testing.T, ents []pb.Entry, p db.Parser) *StorageWrapper {
	dir, err := os.MkdirTemp("", "wal-storage-test-")
	if err != nil {
		t.Fatal(err)
		return nil
	}

	s := OpenStorage(t, dir, p)
	if ents != nil {
		s.Save(defaultHardState, ents, defaultSnap)
	}

	return s
}

func (s *StorageWrapper) CloseAndReopenStorage(t *testing.T) *StorageWrapper {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
		return nil
	}

	return OpenStorage(t, s.path, s.parser)
}

func OpenStorage(t *testing.T, dir string, p db.Parser) *StorageWrapper {
	wal, err := wal.Open(dir, p)
	if err != nil {
		t.Fatal(err)
		return nil
	}

	s := &StorageWrapper{wal, dir, p}

	return s
}

func TestNewStorage(t *testing.T) {
	s := NewStorage(t, nil)
	defer s.Cleanup()

	last, err := s.LastIndex()
	require.Equal(t, nil, err)
	require.Equal(t, uint64(0), last)

	term, err := s.Term(uint64(0))
	require.Equal(t, nil, err)
	require.Equal(t, uint64(0), term)
}

func TestStorageTerm(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			// s := &MemoryStorage{ents: ents}
			s := NewStorage(t, ents)
			defer s.Cleanup()

			if tt.wpanic {
				require.Panics(t, func() {
					_, _ = s.Term(tt.i)
				})
			}
			term, err := s.Term(tt.i)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wterm, term)
		})
	}
}

func TestStorageEntries(t *testing.T) {
	ents := index(3).terms(3, 4, 5, 6)
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []pb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, index(4).terms(4)},
		{4, 6, math.MaxUint64, nil, index(4).terms(4, 5)},
		{4, 7, math.MaxUint64, nil, index(4).terms(4, 5, 6)},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, index(4).terms(4)},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, index(4).terms(4, 5)},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, index(4).terms(4, 5)},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, index(4).terms(4, 5)},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, index(4).terms(4, 5, 6)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, ents)
			defer s.Cleanup()

			entries, err := s.Entries(tt.lo, tt.hi, tt.maxsize)
			require.Equal(t, tt.werr, err)
			require.Equal(t, tt.wentries, entries)
		})
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	s := NewStorage(t, ents)
	defer s.Cleanup()

	last, err := s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)

	// require.NoError(t, s.Append(index(6).terms(5)))
	require.NoError(t, s.Save(defaultHardState, index(6).terms(5), defaultSnap))
	last, err = s.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(6), last)
}

func TestStorageFirstIndex(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	s := NewStorage(t, ents)
	defer s.Cleanup()

	first, err := s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), first)

	require.NoError(t, s.Compact(4))
	first, err = s.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), first)
}

func TestStorageCompact(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		i uint64

		werr   error
		windex uint64
		wterm  uint64
		wlen   int
	}{
		{2, raft.ErrCompacted, 3, 3, 3},
		{3, raft.ErrCompacted, 3, 3, 3},
		{4, nil, 4, 4, 2},
		{5, nil, 5, 5, 1},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, ents)
			defer s.Cleanup()

			// {4, 5, math.MaxUint64, nil, index(4).terms(4)},
			// func (s *WalStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {

			require.Equal(t, tt.werr, s.Compact(tt.i))

			ents := s.ents(t)
			require.Equal(t, tt.windex, ents[0].Index)
			require.Equal(t, tt.wterm, ents[0].Term)
			require.Equal(t, tt.wlen, len(ents))
		})
	}
}

// func TestStorageCreateSnapshot(t *testing.T) {
// 	ents := index(3).terms(3, 4, 5)
// 	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
// 	data := []byte("data")

// 	tests := []struct {
// 		i uint64

// 		werr  error
// 		wsnap pb.Snapshot
// 	}{
// 		{4, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
// 		{5, nil, pb.Snapshot{Data: data, Metadata: pb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
// 	}

// 	for _, tt := range tests {
// 		t.Run("", func(t *testing.T) {
// 			s := NewStorage(t, ents)
// 			defer s.Cleanup()

// 			snap, err := s.CreateSnapshot(tt.i, cs, data)
// 			require.Equal(t, tt.werr, err)
// 			require.Equal(t, tt.wsnap, snap)
// 		})
// 	}
// }

func TestStorageAppend(t *testing.T) {
	ents := index(3).terms(3, 4, 5)
	tests := []struct {
		entries []pb.Entry

		werr     error
		wentries []pb.Entry
	}{
		{
			index(1).terms(1, 2),
			nil,
			index(3).terms(3, 4, 5),
		},
		{
			index(3).terms(3, 4, 5),
			nil,
			index(3).terms(3, 4, 5),
		},
		{
			index(3).terms(3, 6, 6),
			nil,
			index(3).terms(3, 6, 6),
		},
		{
			index(3).terms(3, 4, 5, 5),
			nil,
			index(3).terms(3, 4, 5, 5),
		},
		// Truncate incoming entries, truncate the existing entries and append.
		{
			index(2).terms(3, 3, 5),
			nil,
			index(3).terms(3, 5),
		},
		// Truncate the existing entries and append.
		{
			index(4).terms(5),
			nil,
			index(3).terms(3, 5),
		},
		// Direct append.
		{
			index(6).terms(5),
			nil,
			index(3).terms(3, 4, 5, 5),
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, ents)
			defer s.Cleanup()

			// require.Equal(t, tt.werr, s.Append(tt.entries))
			require.Equal(t, tt.werr, s.Save(defaultHardState, tt.entries, defaultSnap))
			require.Equal(t, tt.wentries, s.ents(t))
		})
	}
}

// func TestStorageApplySnapshot(t *testing.T) {
// 	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
// 	data := []byte("data")

// 	tests := []pb.Snapshot{{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}},
// 		{Data: data, Metadata: pb.SnapshotMetadata{Index: 3, Term: 3, ConfState: *cs}},
// 	}

// 	s := NewStorage(t, nil)
// 	defer s.Cleanup()

// 	i := 0
// 	tt := tests[i]
// 	require.NoError(t, s.ApplySnapshot(tt))

// 	// ApplySnapshot fails due to ErrSnapOutOfDate.
// 	i = 1
// 	tt = tests[i]
// 	require.Equal(t, raft.ErrSnapOutOfDate, s.ApplySnapshot(tt))
// }

func TestStateStorage(t *testing.T) {
	tests := []struct {
		st   pb.HardState
		snap pb.Snapshot
	}{
		{st: defaultHardState, snap: defaultSnap},
		{st: pb.HardState{
			Term:   1,
			Vote:   1,
			Commit: 1,
		}, snap: pb.Snapshot{
			Data: []byte("Foo"),
			Metadata: pb.SnapshotMetadata{
				ConfState: pb.ConfState{
					Voters:         []uint64{1},
					Learners:       []uint64{1},
					VotersOutgoing: []uint64{1},
					LearnersNext:   []uint64{1},
					AutoLeave:      true,
				},
				Index: 1,
				Term:  1,
			},
		}},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, nil)
			defer s.Cleanup()
			s.Save(tt.st, nil, tt.snap)

			stb, err := tt.st.Marshal()
			require.Equal(t, nil, err)

			snapb, err := tt.snap.Marshal()
			require.Equal(t, nil, err)

			states := []wal.State{
				{Type: wal.HardState, Data: stb},
				{Type: wal.Snapshot, Data: snapb},
			}
			require.Equal(t, states, s.states(t))
		})
	}
}

func TestStartupWithExistingLogs(t *testing.T) {
	tests := []struct {
		entries []pb.Entry
	}{
		{index(3).terms(3, 4, 5)},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			s := NewStorage(t, tt.entries)

			s, err := s.CloseAndReopen()
			defer s.Cleanup()
			require.Equal(t, nil, err)

			require.Equal(t, tt.entries, s.ents(t))
		})
	}
}

func saveEntity(t *testing.T, e *cluster.Entity, s db.Storage, term, index uint64) {
	p := &cluster.Proposal{Entities: []*cluster.Entity{e}}
	saveProposal(t, p, s, term, index)
}

func saveProposal(t *testing.T, p *cluster.Proposal, s db.Storage, term, index uint64) {
	bs, err := p.Marshal()
	require.Equal(t, nil, err)

	ent := pb.Entry{Term: term, Index: index, Data: bs}

	err = s.Save(defaultHardState, []pb.Entry{ent}, defaultSnap)
	require.Equal(t, nil, err)
}

func createDatabaseConfiguration(name string) *cluster.Configuration {
	d := &DatabaseConfig{Details: &Details{Name: name, Type: name}}
	return createConfiguration(name, d)
}

func createConfiguration(name string, v any) *cluster.Configuration {
	bs, _ := json.Marshal(v)
	return &cluster.Configuration{ID: name, Name: name, MimeType: "application/json", Data: bs}
}
