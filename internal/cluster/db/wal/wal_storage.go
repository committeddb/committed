package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nakabonne/tstorage"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
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
	// raftLogDir is the on-disk directory tidwall/wal writes entry-log
	// segments into. Kept around so RaftLogApproxSize can stat the
	// segment files without poking through tidwall's internals.
	raftLogDir string
	EntryLog   *wal.Log
	// eventLog is the permanent event log — the "forever" tier described
	// in docs/event-log-architecture.md. ApplyCommitted mirrors every
	// committed raft entry into it, keyed by raft index. Unlike EntryLog,
	// eventLog is never compacted: it is the canonical store that
	// syncables read from, and the one a rebuild rsync covers.
	//
	// Unexported so the raft-index ↔ wal-seq mapping (maintained by
	// appendEvent + the in-memory eventIndex / firstEventIndex atomics)
	// can't be bypassed by an outside caller writing directly. Access
	// goes through appendEvent / readEventAt / EventIndex /
	// firstEventSeq / lastEventSeq.
	eventLog          *wal.Log
	StateLog          *wal.Log // Should we get rid of this and store the latest state in the bbolt db?
	keyValueStorage   *bolt.DB
	// kvMu guards swaps of keyValueStorage (RestoreSnapshot replaces the
	// bbolt handle wholesale: close → rename file → reopen → reassign).
	// Every bbolt transaction routes through view / update, which take
	// kvMu.RLock; RestoreSnapshot takes kvMu.Lock so it waits for
	// in-flight reads to drain before closing the handle and blocks new
	// reads until the reopen finishes. Without this an HTTP query that
	// happened to run mid-restore would hit a closed-file error.
	kvMu              sync.RWMutex
	TimeSeriesStorage tstorage.Storage
	snapshot          pb.Snapshot
	hardState         pb.HardState
	// firstIndex and lastIndex are mutated only from the raft serveChannels
	// goroutine (via appendEntries and Compact) but read from many other
	// goroutines: sync workers (Reader.Read), ingest workers, HTTP handlers
	// (db.ents()), and tests. Atomics make those reads race-free without
	// having to thread a mutex through every caller. The single-writer
	// invariant means no compare-and-swap is needed.
	//
	// firstIndex semantics: the raft index that maps to wal sequence 1
	// under the invariant seq = raftIndex - firstIndex + 1. Set once on
	// the first Save and NEVER updated after that, so the seq mapping
	// stays stable across compactions. tidwall/wal preserves absolute
	// sequence numbers across TruncateFront, so the formula continues to
	// work against the surviving portion of the wal even after the low
	// end has been truncated. (Compact moves the COMPACTION BOUNDARY,
	// tracked in compactedUpTo below; it does not re-base the seq
	// mapping.)
	firstIndex atomic.Uint64
	lastIndex  atomic.Uint64
	// compactedUpTo is the highest raft index that has been compacted
	// away on this node's raft log (conceptually the "dummy" entry at
	// the current wal first-seq — readable via Term for the match
	// check, not via Entries). Zero means no compaction has happened
	// on this storage. Mutated only from the raft serveChannels
	// goroutine (via Compact) so single-writer atomic semantics apply.
	//
	// Why separate from firstIndex: before this split, firstIndex was
	// overloaded to mean BOTH the seq-mapping offset AND the compaction
	// boundary. Compact updated it to the compact index, which broke
	// the seq mapping for future reads and writes (see the function-
	// level comment on firstIndex). Splitting them lets each carry a
	// single, invariant responsibility.
	compactedUpTo atomic.Uint64
	stateIndex uint64
	databases  map[string]cluster.Database
	parser     db.Parser
	sync       chan<- *db.SyncableWithID
	ingest     chan<- *db.IngestableWithID
	// appliedIndex is the highest raft entry index that ApplyCommitted has
	// fully applied. Bumped after each successful per-entry apply (and
	// persisted to bbolt in the same step). Loaded from bbolt on Open.
	//
	// This is "R_local" in the storage invariant P_local == R_local.
	appliedIndex atomic.Uint64
	// eventIndex is the highest raft index written to EventLog. Bumped
	// before appliedIndex in ApplyCommitted so that a crash between the
	// EventLog write and the bbolt appliedIndex persist doesn't lose
	// events: on restart, appliedIndex is <= eventIndex, replay re-runs
	// ApplyCommitted for the gap, and the crash-window guard skips the
	// duplicate EventLog write.
	//
	// This is "P_local" in the storage invariant P_local == R_local.
	eventIndex atomic.Uint64
	// firstEventIndex is the raft index of the first entry in EventLog
	// (the entry at wal sequence 1). 0 means the event log is empty.
	// Used by Reader to translate raft index ↔ wal seq: since Phase 1
	// mirrors every committed raft entry, seq = raftIndex -
	// firstEventIndex + 1 for any raftIndex in [firstEventIndex,
	// eventIndex]. Mutated only from the raft serveChannels goroutine
	// (on first write and on any future compaction of EventLog) so the
	// atomic is for race-free reads, not for CAS.
	firstEventIndex atomic.Uint64

	logger *zap.Logger
}

// Returns a *WalStorage, whether this storage existed already, or an error
// func Open() (*WalStorage, bool, error) {
func Open(dir string, p db.Parser, sync chan<- *db.SyncableWithID, ingest chan<- *db.IngestableWithID, opts ...Option) (*Storage, error) {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	entryLogDir := filepath.Join(dir, "raft", "log")
	stateLogDir := filepath.Join(dir, "raft", "state")
	eventLogDir := filepath.Join(dir, "events")
	keyValueStorageDir := filepath.Join(dir, "metadata")
	timeSeriesStorageDir := filepath.Join(dir, "time-series")

	for _, d := range []string{entryLogDir, stateLogDir, eventLogDir, keyValueStorageDir, timeSeriesStorageDir} {
		if err := os.MkdirAll(d, os.ModePerm); err != nil {
			return nil, err
		}
	}

	entryLog, err := wal.Open(entryLogDir, nil)
	if err != nil {
		return nil, err
	}
	stateLog, err := wal.Open(stateLogDir, nil)
	if err != nil {
		return nil, err
	}
	eventLog, err := wal.Open(eventLogDir, nil)
	if err != nil {
		return nil, err
	}

	boltOpts := &bolt.Options{Timeout: 1 * time.Second, NoSync: cfg.fsyncDisabled}
	keyValueStorage, err := bolt.Open(filepath.Join(keyValueStorageDir, "bbolt.db"), 0600, boltOpts)
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

	// Migrate flat key=value entries in resource buckets to the versioned
	// nested-bucket layout. Idempotent: a no-op if already migrated.
	err = keyValueStorage.Update(func(tx *bolt.Tx) error {
		return migrateToVersionedBuckets(tx)
	})
	if err != nil {
		return nil, fmt.Errorf("migrate to versioned buckets: %w", err)
	}

	logger := cfg.logger
	if logger == nil {
		logger = zap.NewNop()
	}

	dbs := make(map[string]cluster.Database)
	ws := &Storage{
		raftLogDir:        entryLogDir,
		EntryLog:          entryLog,
		eventLog:          eventLog,
		StateLog:          stateLog,
		keyValueStorage:   keyValueStorage,
		TimeSeriesStorage: timeSeriesStorage,
		databases:         dbs,
		parser:            p,
		sync:              sync,
		ingest:            ingest,
		logger:            logger,
	}

	fi, err := entryLog.FirstIndex()
	if err != nil {
		return nil, err
	}

	if fi > 0 {
		// Recover firstIndex (stable seq-mapping offset) and
		// compactedUpTo (current compaction boundary) from the wal
		// state: the entry at wal seq `fi` is by construction the one
		// whose raft index equals the current compaction boundary
		// (seq 1 pre-compaction, or the new first-seq after a Compact
		// moved it forward). Inverting the seq-mapping formula
		// seq = raftIndex - firstIndex + 1 gives
		// firstIndex = raftIndex - seq + 1, which is valid whether
		// seq 1 still physically exists or not — tidwall/wal preserves
		// absolute seq numbers across TruncateFront, so a post-compact
		// read of seq `fi` returns the same bytes it would have
		// returned pre-compact.
		fe, _, err := ws.entry(fi)
		if err != nil {
			return nil, err
		}
		ws.firstIndex.Store(fe.Index - fi + 1)
		ws.compactedUpTo.Store(fe.Index)
	}

	li, err := entryLog.LastIndex()
	if err != nil {
		return nil, err
	}
	ws.lastIndex.Store(li)

	if li > 0 {
		le, _, err := ws.entry(li)
		if err != nil {
			return nil, err
		}
		ws.lastIndex.Store(le.Index)
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

	// eventIndex (P_local) is the raft index of the last entry durably
	// written to EventLog. EventLog's own sequence numbers are
	// monotonic-from-1 and don't match raft index, so we recover the
	// raft index by unmarshaling the last stored entry. Empty EventLog
	// (fresh install or pre-Phase-1 migration) leaves eventIndex at 0.
	evLast, err := eventLog.LastIndex()
	if err != nil {
		return nil, err
	}
	if evLast > 0 {
		data, err := eventLog.Read(evLast)
		if err != nil {
			return nil, fmt.Errorf("event log read last entry: %w", err)
		}
		last := &pb.Entry{}
		if err := last.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("event log unmarshal last entry: %w", err)
		}
		ws.eventIndex.Store(last.Index)

		// Read the first entry to initialize firstEventIndex so
		// Reader.Read can map raft index ↔ wal seq.
		evFirst, err := eventLog.FirstIndex()
		if err != nil {
			return nil, err
		}
		firstData, err := eventLog.Read(evFirst)
		if err != nil {
			return nil, fmt.Errorf("event log read first entry: %w", err)
		}
		first := &pb.Entry{}
		if err := first.Unmarshal(firstData); err != nil {
			return nil, fmt.Errorf("event log unmarshal first entry: %w", err)
		}
		ws.firstEventIndex.Store(first.Index)
	}

	return ws, nil
}

func (s *Storage) Close() error {
	var finalErr error

	finalErr = s.EntryLog.Close()

	err := s.eventLog.Close()
	if err != nil && finalErr == nil {
		finalErr = err
	}

	err = s.StateLog.Close()
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

// ConfState records the ConfState most recently produced by
// raft.Node.ApplyConfChange into the storage's in-memory snapshot
// metadata. The next Save call persists the snapshot (with this
// ConfState) to the state log via appendState, so InitialState returns
// the correct voter set on restart.
//
// Called by the raft Ready loop from raft.go after each EntryConfChange
// apply. This is the write half of the "conf state must survive across
// restart" invariant; the read half is wal.Open reloading the last
// snapshot out of stateLog.
func (s *Storage) ConfState(c *pb.ConfState) {
	s.snapshot.Metadata.ConfState = *c
}

// Save persists raft state and entries durably. It does NOT apply entities
// to BoltDB / time series — that happens in ApplyCommitted, which raft.go
// calls separately on rd.CommittedEntries. Splitting these is important
// because the raft Ready loop hands Save the *to-persist* slice (rd.Entries),
// which on a multi-node follower may include uncommitted entries; applying
// them to bucket state before commit would diverge the cluster.
//
// Empty-snapshot handling: raft's Ready loop hands Save an empty snap
// on every iteration except when a new InstallSnapshot is being
// processed. Overwriting s.snapshot with the empty value each time
// would destroy the ConfState that the conf-change apply path wrote via
// (*Storage).ConfState, which breaks raft.RestartNode's voter-set
// recovery. We only adopt the Ready-supplied snap when it's non-empty
// (i.e. a real InstallSnapshot). In all other cases s.snapshot keeps
// whatever ConfState the conf-change path set — and the subsequent
// appendState call persists that snapshot (metadata, including
// ConfState) to the state log.
func (s *Storage) Save(st pb.HardState, ents []pb.Entry, snap pb.Snapshot) error {
	s.hardState = st
	if !raft.IsEmptySnap(snap) {
		s.snapshot = snap
	}

	if err := s.appendEntries(ents); err != nil {
		return fmt.Errorf("[wal.storage] appendEntries: %w", err)
	}

	if err := s.appendState(st, s.snapshot); err != nil {
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
// It also mirrors the raw entry into the permanent EventLog — every
// committed entry, regardless of EntryType or entity kind, so EventLog's
// sequence number stays 1:1 with raft index. Readers filter at read time.
// This is what keeps the storage invariant P_local == R_local true under
// normal operation: both advance together per entry.
//
// Errors here are treated as fatal by raft.go; see the apply error policy
// comment in raft.go's Ready loop.
func (s *Storage) ApplyCommitted(entry pb.Entry) error {
	// Skip already-applied entries (replay-on-restart safety). A bare
	// EntryNormal with nil data (e.g. the leader's post-election no-op
	// entry from raft) still counts as applied — we bump appliedIndex so
	// the Ready loop's invariant check stays P_local == R_local.
	if entry.Index <= s.appliedIndex.Load() {
		return nil
	}

	// Mirror the raw raft entry into the permanent event log. The guard
	// against entry.Index <= eventIndex covers the crash window where
	// appendEvent succeeded but saveAppliedIndex didn't persist:
	// without it, replay would try to append the same entry again and
	// diverge seq vs. raft index on disk.
	if entry.Index > s.eventIndex.Load() {
		if err := s.appendEvent(entry); err != nil {
			return fmt.Errorf("[wal.storage] %w", err)
		}
	}

	// For EntryNormal with data, apply entities to bucket state and the
	// time-series store. Other entry types (conf changes, no-op entries)
	// still count as "applied" for invariant purposes — we just have
	// nothing to do at the entity level.
	if entry.Type == pb.EntryNormal && entry.Data != nil {
		p := &cluster.Proposal{}
		if err := p.Unmarshal(entry.Data, s); err != nil {
			// Match the prior Save behavior for undecodable proposals:
			// skip the entity apply, but still bump appliedIndex so we
			// don't loop on it forever.
			s.appliedIndex.Store(entry.Index)
			return s.saveAppliedIndex(entry.Index)
		}

		for _, entity := range p.Entities {
			s.logger.Debug("applying entity", zap.String("typeID", entity.Type.ID), zap.String("key", string(entity.Key)))
			if err := s.applyEntity(entity); err != nil {
				return err
			}
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
// apply. This is "R_local" in the storage invariant P_local == R_local.
func (s *Storage) AppliedIndex() uint64 {
	return s.appliedIndex.Load()
}

// EventIndex returns the highest raft entry index that has been durably
// written to the permanent event log on this node. This is "P_local" in
// the storage invariant P_local == R_local, which the Ready loop checks
// after every iteration (see raft.go's checkStorageInvariant). On a fresh
// or brand-new node eventLog starts empty and EventIndex is 0.
func (s *Storage) EventIndex() uint64 {
	return s.eventIndex.Load()
}

// RaftLogApproxSize returns the approximate on-disk size (bytes) of the
// raft log by summing tidwall/wal's segment file sizes under
// raft/log/. Called by the compaction trigger in raft.go to decide
// whether the "size > threshold" limb of the 10GB/1hr policy has been
// crossed. Approximate because tidwall/wal cycles segments, so a
// segment that's still being written may be sized below its final
// on-disk length by a few kB — close enough for a trigger decision.
// Errors walking the directory are returned as (0, err); callers
// treat that as "no trigger" rather than panicking.
func (s *Storage) RaftLogApproxSize() (uint64, error) {
	entries, err := os.ReadDir(s.raftLogDir)
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			// File vanished between ReadDir and Info (e.g. a
			// concurrent cycle); skip, don't fail the whole sum.
			continue
		}
		total += uint64(info.Size())
	}
	return total, nil
}

// firstEventSeq returns the wal sequence of the first entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
func (s *Storage) firstEventSeq() (uint64, error) {
	return s.eventLog.FirstIndex()
}

// lastEventSeq returns the wal sequence of the last entry in the
// permanent event log, or 0 when the log is empty. Package-internal.
func (s *Storage) lastEventSeq() (uint64, error) {
	return s.eventLog.LastIndex()
}

// readEventAt reads the raw pb.Entry bytes at the given wal sequence
// from the permanent event log. Package-internal; callers outside the
// storage tier should go through the Reader abstraction, which handles
// raft index ↔ wal seq translation and filters metadata entries.
func (s *Storage) readEventAt(seq uint64) ([]byte, error) {
	return s.eventLog.Read(seq)
}

// appendEvent writes a committed raft entry's raw bytes to the
// permanent event log, stamping firstEventIndex on the very first
// write and bumping eventIndex on success. Called only from
// ApplyCommitted, which already guards against writes at or below
// eventIndex (crash-window idempotence). Kept on Storage (not inlined
// into ApplyCommitted) so there's exactly one site that advances
// P_local.
func (s *Storage) appendEvent(entry pb.Entry) error {
	entryBytes, err := entry.Marshal()
	if err != nil {
		return fmt.Errorf("marshal entry for event log: %w", err)
	}
	nextSeq, err := s.eventLog.LastIndex()
	if err != nil {
		return fmt.Errorf("event log last index: %w", err)
	}
	nextSeq++
	if err := s.eventLog.Write(nextSeq, entryBytes); err != nil {
		return fmt.Errorf("event log write seq %d (raft index %d): %w", nextSeq, entry.Index, err)
	}
	if nextSeq == 1 {
		s.firstEventIndex.Store(entry.Index)
	}
	s.eventIndex.Store(entry.Index)
	return nil
}

// view runs fn in a bbolt read-only transaction while holding kvMu for
// read. Guards against RestoreSnapshot swapping the bbolt handle out
// from under an in-flight query.
func (s *Storage) view(fn func(tx *bolt.Tx) error) error {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	return s.keyValueStorage.View(fn)
}

// update runs fn in a bbolt read-write transaction while holding kvMu
// for read. Same concurrency shape as view: many concurrent update
// calls proceed in parallel (bbolt itself serializes writers) and
// RestoreSnapshot waits for them all to drain.
func (s *Storage) update(fn func(tx *bolt.Tx) error) error {
	s.kvMu.RLock()
	defer s.kvMu.RUnlock()
	return s.keyValueStorage.Update(fn)
}

// saveAppliedIndex persists the applied index to bbolt. Called from
// ApplyCommitted after a per-entry apply succeeds. Each apply runs in its
// own short bbolt transaction; this is acceptable because the per-entity
// handlers are not atomic with each other today either.
func (s *Storage) saveAppliedIndex(idx uint64) error {
	return s.update(func(tx *bolt.Tx) error {
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
	err := s.view(func(tx *bolt.Tx) error {
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

	// Snapshot the current bounds. appendEntries is only ever invoked from
	// the raft serveChannels goroutine (the sole writer), so observing them
	// once at the top is sufficient — no other writer can race us.
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	// `first` is the lowest raft index the wal can accept an append at
	// — one past the current compacted-dummy (boundary()). Entries at
	// or below that are already compacted away and must be skipped, not
	// rewritten.
	first := s.boundary() + 1
	last := ents[0].Index + uint64(len(ents)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > ents[0].Index {
		ents = ents[first-ents[0].Index:]
	}

	offset := ents[0].Index - firstIndex
	l := lastIndex - firstIndex + 1

	// Don't error when this is the first write
	if firstIndex > 0 && l < offset {
		return fmt.Errorf("missing log entry [last: %d, append at: %d]", lastIndex, firstIndex)
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

	if firstIndex == 0 && lastIndex == 0 && ents != nil {
		firstIndex = ents[0].Index
		s.firstIndex.Store(firstIndex)
	}

	for _, e := range ents {
		data, err := e.Marshal()
		if err != nil {
			return err
		}

		i := e.Index - firstIndex + 1
		err = s.EntryLog.Write(i, data)
		if err != nil {
			return fmt.Errorf("index %d to position %d: %w", e.Index, i, err)
		}
	}

	s.lastIndex.Store(ents[len(ents)-1].Index)

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
//
// Boundary check uses boundary(), which returns the higher of
// firstIndex (the seq-mapping offset) and compactedUpTo (the
// current compaction boundary). On a fresh storage both equal the
// first written raft index, which correctly treats that entry as
// the compacted-dummy per existing wal.Storage semantics. On a
// post-compact storage, compactedUpTo dominates and the boundary
// moves with each Compact without disturbing the seq mapping.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	var totalSize uint64

	firstIndex := s.firstIndex.Load()
	if lo <= s.boundary() {
		return nil, raft.ErrCompacted
	}

	var es []pb.Entry
	logIndex := lo - firstIndex
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

// boundary returns the higher of firstIndex (seq-mapping offset) and
// compactedUpTo (current compaction boundary). Used by Entries, Term,
// FirstIndex, and Compact to locate the "dummy" entry's raft index —
// the index below which data is unavailable (ErrCompacted).
//
// Split rationale (from firstIndex's doc): firstIndex is stable for
// seq mapping, compactedUpTo moves with each Compact. max() collapses
// them to a single boundary for callers that only care about the
// compaction view.
func (s *Storage) boundary() uint64 {
	fi := s.firstIndex.Load()
	cu := s.compactedUpTo.Load()
	if cu > fi {
		return cu
	}
	return fi
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
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	if firstIndex == 0 && lastIndex == 0 {
		return uint64(0), nil
	}

	if i < s.boundary() {
		return 0, raft.ErrCompacted
	}

	if i > lastIndex {
		return 0, raft.ErrUnavailable
	}

	logIndex := i - firstIndex + 1
	e, _, err := s.entry(logIndex)
	if err != nil {
		return 0, fmt.Errorf("wal index %d: %w", logIndex, err)
	}

	return e.Term, nil
}

func (s *Storage) LastIndex() (uint64, error) {
	return s.lastIndex.Load(), nil
}

// FirstIndex returns the first raft index that Entries can return.
// The entry at index boundary() is retained for Term() match checks
// (the compacted-dummy semantic etcd raft expects) but is not
// returnable via Entries — so the first readable index is
// boundary()+1.
func (s *Storage) FirstIndex() (uint64, error) {
	return s.boundary() + 1, nil
}

func (s *Storage) Snapshot() (pb.Snapshot, error) {
	return s.snapshot, nil
}

// Compact drops raft log entries up to and including compactIndex.
// Only the raft-log tier is trimmed; eventLog is never compacted.
//
// Sequence mapping: compactIndex maps to wal seq
// `compactIndex - firstIndex + 1`. tidwall/wal's TruncateFront removes
// every seq strictly less than the argument and leaves the surviving
// seqs at their absolute positions (no renumbering). firstIndex
// therefore STAYS at its original offset — moving it would break the
// seq-mapping formula for all future reads and writes (the entry at
// raft index R is at wal seq R - firstIndex + 1; once firstIndex has
// been advanced past 1, any subsequent read of R computes a wrong seq).
//
// compactedUpTo records the new compaction boundary so FirstIndex(),
// Entries(), and Term() know where the current compacted-dummy sits.
//
// Store order: compactedUpTo is advanced BEFORE TruncateFront fires.
// Reads from etcd raft's internal node goroutine (storage.Term during
// leader AppendEntries construction, e.g. maybeSendAppend) race
// against the serveChannels Compact call; if we truncated the wal
// first, a racing Term read of an about-to-be-compacted index would
// pass the boundary check against the stale compactedUpTo value,
// fall through to EntryLog.Read on a seq that no longer exists, and
// panic with "wal index N: not found". Advancing compactedUpTo first
// makes such reads return ErrCompacted — raft treats that as a
// signal to send an InstallSnapshot instead, which is safe even if
// the wal still briefly holds the old seq. If TruncateFront fails
// after compactedUpTo has been advanced, we're in an "over-reported
// compaction" state where the wal retains data whose indices
// we'll no longer return via Entries — benign: the data becomes
// unreachable but isn't corrupted.
func (s *Storage) Compact(compactIndex uint64) error {
	firstIndex := s.firstIndex.Load()
	lastIndex := s.lastIndex.Load()

	if compactIndex <= s.boundary() {
		return raft.ErrCompacted
	}
	if compactIndex > lastIndex {
		return ErrOutOfBounds
	}

	s.compactedUpTo.Store(compactIndex)

	i := compactIndex - firstIndex + 1
	if err := s.EntryLog.TruncateFront(i); err != nil {
		return err
	}

	return nil
}
