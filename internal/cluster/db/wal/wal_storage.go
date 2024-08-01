package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

var ErrOutOfBounds = errors.New("requested index is greater than last index")
var ErrTypeMissing = errors.New("type not found")
var ErrBucketMissing = errors.New("key value bucket missing")

var typeBucket = []byte("types")

type StateType int

const (
	HardState = 0
	Snapshot  = 1
)

type State struct {
	Type StateType
	Data []byte
}

type WalStorage struct {
	EntryLog    *wal.Log
	StateLog    *wal.Log
	typeStorage *bolt.DB
	snapshot    pb.Snapshot
	hardState   pb.HardState
	firstIndex  uint64
	lastIndex   uint64
	stateIndex  uint64
}

// Returns a *WalStorage, whether this storage existed already, or an error
// func Open() (*WalStorage, bool, error) {
func Open(dir string) (*WalStorage, error) {
	entryLogDir := filepath.Join(dir, "entry-log")
	stateLogDir := filepath.Join(dir, "state-log")
	typeStorageDir := filepath.Join(dir, "type-storage")

	err := os.MkdirAll(entryLogDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(stateLogDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(typeStorageDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	entryLog, err := wal.Open(entryLogDir, nil)
	if err != nil {
		return nil, err
	}
	stateLog, err := wal.Open(stateLogDir, nil)
	if err != nil {
		return nil, err
	}

	typeStorage, err := bolt.Open(filepath.Join(typeStorageDir, "types.db"), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	typeStorage.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(typeBucket)
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	ws := &WalStorage{EntryLog: entryLog, StateLog: stateLog, typeStorage: typeStorage}

	fi, err := entryLog.FirstIndex()
	if err != nil {
		return nil, err
	}

	if fi > 0 {
		fe, _, err := ws.entry(fi)
		if err != nil {
			return nil, err
		}
		ws.firstIndex = fe.Index
	}

	li, err := entryLog.LastIndex()
	if err != nil {
		return nil, err
	}
	ws.lastIndex = li

	if li > 0 {
		le, _, err := ws.entry(li)
		if err != nil {
			return nil, err
		}
		ws.lastIndex = le.Index
	}

	li, err = stateLog.LastIndex()
	if err != nil {
		return nil, err
	}
	ws.stateIndex = li

	st, snap, err := ws.getLastStates(ws.stateIndex)
	if err != nil {
		return nil, err
	}
	ws.hardState = *st
	ws.snapshot = *snap

	return ws, nil
}

func (s *WalStorage) Close() error {
	err1 := s.EntryLog.Close()
	err2 := s.StateLog.Close()
	err3 := s.typeStorage.Close()

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	return err3
}

func (s *WalStorage) getLastStates(li uint64) (*pb.HardState, *pb.Snapshot, error) {
	st := &pb.HardState{}
	snap := &pb.Snapshot{
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

	if li > 0 {
		stDone := false
		snapDone := false

		fi, err := s.StateLog.FirstIndex()
		if err != nil {
			return nil, nil, err
		}

		for i := li; i >= fi; i-- {
			e, err := s.state(i)
			if err != nil {
				return nil, nil, err
			}

			if e.Type == HardState && !stDone {
				err = st.Unmarshal(e.Data)
				if err != nil {
					return nil, nil, err
				}
				stDone = true
			} else if e.Type == Snapshot && !snapDone {
				err = snap.Unmarshal(e.Data)
				if err != nil {
					return nil, nil, err
				}
				snapDone = true
			}

			if stDone && snapDone {
				break
			}
		}
	}

	return st, snap, nil
}

func (s *WalStorage) ConfState(c *pb.ConfState) {
	s.snapshot.Metadata.ConfState = *c
}

func (w *WalStorage) Save(st pb.HardState, ents []pb.Entry, snap pb.Snapshot) error {
	w.hardState = st
	w.snapshot = snap
	err := w.appendEntries(ents)
	if err != nil {
		return err
	}

	for _, ent := range ents {
		if ent.Type == pb.EntryNormal {
			p := &cluster.Proposal{}
			err := p.Unmarshal(ent.Data)
			if err != nil {
				continue
			}

			for _, entity := range p.Entities {
				if cluster.IsType(entity.ID) {
					if string(entity.Data) == string(cluster.Delete) {
						w.deleteType(entity.Key)
					} else {
						t := &cluster.Type{}
						err := t.Unmarshal(entity.Data)
						if err != nil {
							continue
						}
						w.saveType(t)
					}
				}
			}
		}
	}

	return w.appendState(st, snap)
}

func (w *WalStorage) saveType(t *cluster.Type) error {
	return w.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return err
		}
		return b.Put([]byte(t.ID), bs)
	})
}

func (w *WalStorage) deleteType(id []byte) error {
	return w.typeStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return b.Delete(id)
	})
}

func (s *WalStorage) appendEntries(ents []pb.Entry) error {
	if len(ents) == 0 {
		return nil
	}

	first := s.firstIndex + 1
	last := ents[0].Index + uint64(len(ents)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > ents[0].Index {
		ents = ents[first-ents[0].Index:]
	}

	offset := ents[0].Index - s.firstIndex
	l := s.lastIndex - s.firstIndex + 1

	// Don't error when this is the first write
	if s.firstIndex > 0 && l < offset {
		return fmt.Errorf("missing log entry [last: %d, append at: %d]", s.lastIndex, s.firstIndex)
	}

	// We have received previous log entries a second time and/or have log entries newer than the ones being received
	// This can happen during leadership changes or because we wrote data that was later not accepted by consensus
	// Trust the new data over the old data
	if l > offset {
		err := s.EntryLog.TruncateBack(offset)
		if err != nil {
			return err
		}
	}

	// case len > offset:
	// 	// NB: full slice expression protects ms.ents at index >= offset from
	// 	// rewrites, as they may still be referenced from outside MemoryStorage.
	// 	ms.ents = append(ms.ents[:offset:offset], entries...)
	// case len == offset:
	// 	ms.ents = append(ms.ents, entries...)
	// default:
	// 	return fmt.Errorf("missing log entry [last: %d, append at: %d]", s.lastIndex, s.firstIndex)
	// }

	if s.firstIndex == 0 && s.lastIndex == 0 && ents != nil {
		s.firstIndex = ents[0].Index
	}

	for _, e := range ents {
		data, err := e.Marshal()
		if err != nil {
			return err
		}

		i := e.Index - s.firstIndex + 1
		err = s.EntryLog.Write(i, data)
		if err != nil {
			return fmt.Errorf("index %d to position %d: %w", e.Index, i, err)
		}
	}

	s.lastIndex = ents[len(ents)-1].Index

	return nil
}

func (s *WalStorage) appendState(st pb.HardState, snap pb.Snapshot) error {
	var ss []State
	stData, err := st.Marshal()
	if err != nil {
		return err
	}
	ss = append(ss, State{Type: HardState, Data: stData})

	snapData, err := snap.Marshal()
	if err != nil {
		return err
	}
	ss = append(ss, State{Type: Snapshot, Data: snapData})

	for _, e := range ss {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(e); err != nil {
			return err
		}

		s.stateIndex++
		err = s.StateLog.Write(s.stateIndex, buf.Bytes())
		if err != nil {
			return fmt.Errorf("position %d: %w", s.stateIndex, err)
		}
	}

	return nil
}

// InitialState returns the saved HardState and ConfState information.
func (s *WalStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *WalStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	var totalSize uint64

	if lo <= s.firstIndex {
		return nil, raft.ErrCompacted
	}

	var es []pb.Entry
	logIndex := lo - s.firstIndex
	for x := lo; x < hi; x++ {
		logIndex++
		e, size, err := s.entry(logIndex)
		if err != nil {
			return nil, err
		}

		totalSize += size
		if len(es) == 0 || totalSize <= maxSize {
			es = append(es, *e)
		}
	}

	return es, nil
}

// Returns the entry, the size of the entry (in bytes) before being unmarshalled, and an error
func (s *WalStorage) entry(i uint64) (*pb.Entry, uint64, error) {
	e := &pb.Entry{}
	data, err := s.EntryLog.Read(i)
	if err != nil {
		return nil, 0, err
	}

	err = e.Unmarshal(data)
	if err != nil {
		return nil, 0, err
	}

	return e, uint64(len(data)), nil
}

func (s *WalStorage) state(li uint64) (*State, error) {
	e := &State{}
	data, err := s.StateLog.Read(li)
	if err != nil {
		return nil, err
	}

	dec := gob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(e); err != nil {
		return nil, err
	}

	return e, nil
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (s *WalStorage) Term(i uint64) (uint64, error) {
	if s.firstIndex == 0 && s.lastIndex == 0 {
		return uint64(0), nil
	}

	if i < s.firstIndex {
		return 0, raft.ErrCompacted
	}

	if i > s.lastIndex {
		return 0, raft.ErrUnavailable
	}

	logIndex := i - s.firstIndex + 1
	e, _, err := s.entry(logIndex)
	if err != nil {
		return 0, fmt.Errorf("wal index %d: %w", logIndex, err)
	}

	return e.Term, nil
}

func (s *WalStorage) LastIndex() (uint64, error) {
	return s.lastIndex, nil
}

func (s *WalStorage) FirstIndex() (uint64, error) {
	return s.firstIndex + uint64(1), nil
}

func (s *WalStorage) Snapshot() (pb.Snapshot, error) {
	return s.snapshot, nil
}

func (s *WalStorage) Compact(compactIndex uint64) error {
	if compactIndex <= s.firstIndex {
		return raft.ErrCompacted
	}
	if compactIndex > s.lastIndex {
		return ErrOutOfBounds
	}

	i := compactIndex - s.firstIndex + 1
	err := s.EntryLog.TruncateFront(i)
	if err != nil {
		return err
	}

	s.firstIndex = compactIndex

	return nil
}

func (s *WalStorage) Type(id string) (*cluster.Type, error) {
	t := &cluster.Type{}

	return t, s.typeStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs := b.Get([]byte(id))
		if bs == nil {
			return ErrTypeMissing
		}
		return t.Unmarshal(bs)
	})
}
