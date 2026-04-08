package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

var ErrOutOfBounds = errors.New("requested index is greater than last index")
var ErrTypeMissing = errors.New("type not found")
var ErrDatabaseMissing = errors.New("database not found")
var ErrBucketMissing = errors.New("key value bucket missing")

var typeBucket = []byte("types")
var databaseBucket = []byte("databases")
var ingestableBucket = []byte("ingestables")

// TODO var ingestablePositionBucket = []byte("ingestablesPositions")
var syncableBucket = []byte("syncables")
var syncableIndexBucket = []byte("syncableIndexes")

// appliedIndexBucket holds a single key ("idx") whose value is the
// big-endian uint64 of the highest raft entry index that ApplyCommitted has
// fully applied. Persisted so that on restart the Ready loop's replay of
// already-applied committed entries is a no-op.
var appliedIndexBucket = []byte("appliedIndex")
var appliedIndexKey = []byte("idx")

var buckets = [][]byte{typeBucket, databaseBucket, ingestableBucket, syncableBucket, syncableIndexBucket, appliedIndexBucket}

type StateType int

const (
	HardState = 0
	Snapshot  = 1
)

type State struct {
	Type StateType
	Data []byte
}

type Storage struct {
	EntryLog          *wal.Log
	StateLog          *wal.Log // Should we get rid of this and store the latest state in the bbolt db?
	keyValueStorage   *bolt.DB
	TimeSeriesStorage tstorage.Storage
	snapshot          pb.Snapshot
	hardState         pb.HardState
	firstIndex        uint64
	lastIndex         uint64
	stateIndex        uint64
	databases         map[string]cluster.Database
	parser            db.Parser
	sync              chan<- *db.SyncableWithID
	ingest            chan<- *db.IngestableWithID
	// appliedIndex is the highest raft entry index that ApplyCommitted has
	// fully applied. Bumped after each successful per-entry apply (and
	// persisted to bbolt in the same step). Loaded from bbolt on Open.
	appliedIndex atomic.Uint64
}

// Returns a *WalStorage, whether this storage existed already, or an error
// func Open() (*WalStorage, bool, error) {
func Open(dir string, p db.Parser, sync chan<- *db.SyncableWithID, ingest chan<- *db.IngestableWithID, opts ...Option) (*Storage, error) {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	entryLogDir := filepath.Join(dir, "entry-log")
	stateLogDir := filepath.Join(dir, "state-log")
	keyValueStorageDir := filepath.Join(dir, "type-storage")
	timeSeriesStorageDir := filepath.Join(dir, "time-series")

	err := os.MkdirAll(entryLogDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(stateLogDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(keyValueStorageDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(timeSeriesStorageDir, os.ModePerm)
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

	boltOpts := &bolt.Options{Timeout: 1 * time.Second, NoSync: cfg.fsyncDisabled}
	keyValueStorage, err := bolt.Open(filepath.Join(keyValueStorageDir, "types.db"), 0600, boltOpts)
	if err != nil {
		return nil, err
	}

	tssOpts := []tstorage.Option{
		tstorage.WithTimestampPrecision(tstorage.Milliseconds),
	}
	if !cfg.inMemoryTimeSeries {
		tssOpts = append(tssOpts, tstorage.WithDataPath("./data"))
	}
	timeSeriesStorage, err := tstorage.NewStorage(tssOpts...)
	if err != nil {
		return nil, err
	}

	err = keyValueStorage.Update(func(tx *bolt.Tx) error {
		for _, bucket := range buckets {
			_, err := tx.CreateBucketIfNotExists(bucket)
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	dbs := make(map[string]cluster.Database)
	ws := &Storage{
		EntryLog:          entryLog,
		StateLog:          stateLog,
		keyValueStorage:   keyValueStorage,
		TimeSeriesStorage: timeSeriesStorage,
		databases:         dbs,
		parser:            p,
		sync:              sync,
		ingest:            ingest,
	}

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

	err = ws.loadDatabases()
	if err != nil {
		return nil, err
	}

	idx, err := ws.loadAppliedIndex()
	if err != nil {
		return nil, err
	}
	ws.appliedIndex.Store(idx)

	return ws, nil
}

func (s *Storage) Close() error {
	var finalErr error

	finalErr = s.EntryLog.Close()

	err := s.StateLog.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}
	err = s.keyValueStorage.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	err = s.TimeSeriesStorage.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	for _, db := range s.databases {
		err = db.Close()
		if err != nil && finalErr == nil {
			finalErr = err
		}
	}

	return finalErr
}

func (s *Storage) getLastStates(li uint64) (*pb.HardState, *pb.Snapshot, error) {
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

func (s *Storage) ConfState(c *pb.ConfState) {
	s.snapshot.Metadata.ConfState = *c
}

// Save persists raft state and entries durably. It does NOT apply entities
// to BoltDB / time series — that happens in ApplyCommitted, which raft.go
// calls separately on rd.CommittedEntries. Splitting these is important
// because the raft Ready loop hands Save the *to-persist* slice (rd.Entries),
// which on a multi-node follower may include uncommitted entries; applying
// them to bucket state before commit would diverge the cluster.
func (s *Storage) Save(st pb.HardState, ents []pb.Entry, snap pb.Snapshot) error {
	s.hardState = st
	s.snapshot = snap

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	if err := s.appendState(st, snap); err != nil {
		return fmt.Errorf("[wal.storage] appendState: %w", err)
	}

	return nil
}

// ApplyCommitted applies a single committed raft entry to application
// state. It is called by the raft Ready loop on each entry from
// rd.CommittedEntries, after Save has persisted the entry and before
// node.Advance(). Apply must complete before Advance per etcd-raft contract.
//
// ApplyCommitted is idempotent on re-apply: entries with index <=
// AppliedIndex are skipped. The applied index is bumped (and persisted to
// bbolt) after each successful apply, so a restart that replays committed
// entries through the Ready loop will skip the already-applied portion.
//
// Errors here are treated as fatal by raft.go; see the apply error policy
// comment in raft.go's Ready loop.
func (s *Storage) ApplyCommitted(entry pb.Entry) error {
	if entry.Type != pb.EntryNormal || entry.Data == nil {
		return nil
	}

	// Skip already-applied entries (replay-on-restart safety).
	if entry.Index <= s.appliedIndex.Load() {
		return nil
	}

	p := &cluster.Proposal{}
	if err := p.Unmarshal(entry.Data); err != nil {
		// Match the prior Save behavior for undecodable proposals: skip,
		// but still bump appliedIndex so we don't loop on it forever.
		s.appliedIndex.Store(entry.Index)
		return s.saveAppliedIndex(entry.Index)
	}

	for _, entity := range p.Entities {
		fmt.Printf("[wal.storage] Applying entity %v\n", entity)
		if err := s.applyEntity(entity); err != nil {
			return err
		}
	}

	s.appliedIndex.Store(entry.Index)
	return s.saveAppliedIndex(entry.Index)
}

func (s *Storage) applyEntity(entity *cluster.Entity) error {
	switch {
	case cluster.IsType(entity.ID):
		if err := s.handleType(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleType: %w", err)
		}
	case cluster.IsDatabase(entity.ID):
		if err := s.handleDatabase(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleDatabase: %w", err)
		}
	case cluster.IsIngestable(entity.ID):
		if err := s.handleIngestable(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleIngestable: %w", err)
		}
	case cluster.IsSyncable(entity.ID):
		if err := s.handleSyncable(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleSyncable: %w", err)
		}
	case cluster.IsSyncableIndex(entity.ID):
		if err := s.handleSyncableIndex(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleSyncableIndex: %w", err)
		}
	default:
		if err := s.handleUserDefined(entity); err != nil {
			return fmt.Errorf("[wal.storage] handleUserDefined: %w", err)
		}
	}
	return nil
}

// AppliedIndex returns the highest raft entry index that ApplyCommitted has
// fully applied. Loaded from bbolt on Open and bumped after each successful
// apply.
func (s *Storage) AppliedIndex() uint64 {
	return s.appliedIndex.Load()
}

// saveAppliedIndex persists the applied index to bbolt. Called from
// ApplyCommitted after a per-entry apply succeeds. Each apply runs in its
// own short bbolt transaction; this is acceptable because the per-entity
// handlers are not atomic with each other today either.
func (s *Storage) saveAppliedIndex(idx uint64) error {
	return s.keyValueStorage.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], idx)
		return b.Put(appliedIndexKey, buf[:])
	})
}

// loadAppliedIndex reads the persisted applied index from bbolt, or returns
// 0 if no apply has happened yet (fresh storage).
func (s *Storage) loadAppliedIndex() (uint64, error) {
	var idx uint64
	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(appliedIndexBucket)
		if b == nil {
			return ErrBucketMissing
		}
		v := b.Get(appliedIndexKey)
		if v == nil {
			return nil
		}
		if len(v) != 8 {
			return fmt.Errorf("appliedIndex: expected 8 bytes, got %d", len(v))
		}
		idx = binary.BigEndian.Uint64(v)
		return nil
	})
	return idx, err
}

func (s *Storage) appendEntries(ents []pb.Entry) error {
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

func (s *Storage) appendState(st pb.HardState, snap pb.Snapshot) error {
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
func (s *Storage) InitialState() (pb.HardState, pb.ConfState, error) {
	return s.hardState, s.snapshot.Metadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
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
func (s *Storage) entry(i uint64) (*pb.Entry, uint64, error) {
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

func (s *Storage) state(li uint64) (*State, error) {
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
func (s *Storage) Term(i uint64) (uint64, error) {
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

func (s *Storage) LastIndex() (uint64, error) {
	return s.lastIndex, nil
}

func (s *Storage) FirstIndex() (uint64, error) {
	return s.firstIndex + uint64(1), nil
}

func (s *Storage) Snapshot() (pb.Snapshot, error) {
	return s.snapshot, nil
}

func (s *Storage) Compact(compactIndex uint64) error {
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
