package testing

import (
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
)

type MemoryStorageSaveArgsForCall struct {
	st   raftpb.HardState
	ents []raftpb.Entry
	snap raftpb.Snapshot
}

type MemoryStorage struct {
	*raft.MemoryStorage
	saveArgsForCall []*MemoryStorageSaveArgsForCall
	indexes         map[string]uint64
	node            uint64

	// appliedMu guards appliedIndex. ApplyCommitted is invoked from the
	// raft Ready loop goroutine while AppliedIndex can be read from any
	// goroutine (e.g., the /ready HTTP probe), so the bare uint64 needs
	// a lock to satisfy the race detector.
	appliedMu    sync.Mutex
	appliedIndex uint64
}

func NewMemoryStorage() *MemoryStorage {
	indexes := make(map[string]uint64)
	return &MemoryStorage{
		MemoryStorage: raft.NewMemoryStorage(),
		indexes:       indexes,
	}
}

func (ms *MemoryStorage) Close() error {
	return nil
}

func (ms *MemoryStorage) Save(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) error {
	err := ms.Append(ents)
	if err != nil {
		return err
	}

	ms.maybeAppendArgsForCall(st, ents, snap)

	return ms.SetHardState(st)
}

// ApplyCommitted records the entry's index as the highest applied index
// and otherwise does nothing. The in-memory test storage has no buckets
// and no downstream channels to feed, so there is no real apply work —
// but recording the index is enough for tests that need a non-zero
// AppliedIndex (notably the /ready HTTP probe test, which gates on
// AppliedIndex > 0). Tests that exercise the real apply path still use
// wal.Storage.
func (ms *MemoryStorage) ApplyCommitted(entry raftpb.Entry) error {
	ms.appliedMu.Lock()
	defer ms.appliedMu.Unlock()
	if entry.Index > ms.appliedIndex {
		ms.appliedIndex = entry.Index
	}
	return nil
}

// AppliedIndex returns the highest entry index that ApplyCommitted has
// observed. For MemoryStorage there is no real apply step, so this
// reflects "raft told us this entry was committed" rather than "we
// successfully wrote it to a bucket" — but for liveness/readiness
// purposes that's the same signal.
func (ms *MemoryStorage) AppliedIndex() uint64 {
	ms.appliedMu.Lock()
	defer ms.appliedMu.Unlock()
	return ms.appliedIndex
}

// EventIndex mirrors AppliedIndex for MemoryStorage. Real storage
// maintains a separate permanent event log index so the Ready loop can
// detect divergence after a snapshot install, but MemoryStorage has no
// event log to diverge from — so returning AppliedIndex keeps the
// invariant P_local == R_local trivially true and lets tests that use
// MemoryStorage run the same invariant check as production code.
func (ms *MemoryStorage) EventIndex() uint64 {
	return ms.AppliedIndex()
}

// CreateSnapshot delegates to the embedded raft.MemoryStorage, which
// already supports snapshot creation for in-memory tests. Passing nil
// Data is fine here — MemoryStorage has no bucket state to capture;
// tests that need a real snapshot payload use wal.Storage.
// ConfState is a no-op on the in-process test storage: tests that
// use this MemoryStorage never restart across process boundaries, so
// InitialState is only ever called once on a fresh struct. The method
// exists to satisfy the db.Storage interface contract added when
// restart-correct ConfState persistence was plumbed through wal.Storage.
func (ms *MemoryStorage) ConfState(c *raftpb.ConfState) {}

func (ms *MemoryStorage) CreateSnapshot(index uint64, confState *raftpb.ConfState) (raftpb.Snapshot, error) {
	return ms.MemoryStorage.CreateSnapshot(index, confState, nil)
}

// RestoreSnapshot is a no-op for MemoryStorage. The embedded
// raft.MemoryStorage handles raft-log-level snapshot state internally
// when Save is called with a non-empty rd.Snapshot; there is no
// application-level state for this in-memory stub to restore.
func (ms *MemoryStorage) RestoreSnapshot(snap raftpb.Snapshot) error {
	return nil
}

// RaftLogApproxSize reports 0 for MemoryStorage — there is no on-disk
// raft log to measure, and the compaction trigger against an
// in-memory test double is always driven by options other than size.
func (ms *MemoryStorage) RaftLogApproxSize() (uint64, error) {
	return 0, nil
}

func (ms *MemoryStorage) maybeAppendArgsForCall(st raftpb.HardState, ents []raftpb.Entry, snap raftpb.Snapshot) {
	normalEntry := false
	for _, ent := range ents {
		if ent.Type == raftpb.EntryNormal {
			normalEntry = true
		}
	}

	if normalEntry {
		ms.saveArgsForCall = append(ms.saveArgsForCall, &MemoryStorageSaveArgsForCall{st, ents, snap})
	}
}

func (ms *MemoryStorage) SaveCallCount() int {
	return len(ms.saveArgsForCall)
}

func (ms *MemoryStorage) SaveArgsForCall(i int) (raftpb.HardState, []raftpb.Entry, raftpb.Snapshot) {
	a := ms.saveArgsForCall[i]
	return a.st, a.ents, a.snap
}

// ResolveType returns a bare Type with just the identity fields
// populated from the ref. The in-memory test storage doesn't track
// type definitions, so "unknown type" is ambiguous — returning a stub
// keeps tests that propose-and-read-back user data working without
// each test having to first register a real type. Production storage
// fails a lookup loudly; tests that care about that path should use
// wal.Storage.
func (ms *MemoryStorage) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	return &cluster.Type{ID: ref.ID, Version: ref.Version}, nil
}

func (ms *MemoryStorage) TimePoints(typeID string, start time.Time, end time.Time) ([]cluster.TimePoint, error) {
	return nil, nil
}

func (ms *MemoryStorage) Reader(id string) db.ProposalReader {
	i, ok := ms.indexes[id]
	if !ok {
		i = 0
	}

	return &Reader{index: i, s: ms}
}

func (ms *MemoryStorage) Position(id string) cluster.Position {
	return nil
}

func (ms *MemoryStorage) Database(id string) (cluster.Database, error) {
	return nil, nil
}

func (ms *MemoryStorage) Node(id string) uint64 {
	return ms.node
}

func (ms *MemoryStorage) SetNode(n uint64) {
	ms.node = n
}

func (ms *MemoryStorage) Databases() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Ingestables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Syncables() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) Types() ([]*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) DatabaseVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) DatabaseVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) SyncableVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) SyncableVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

func (ms *MemoryStorage) TypeVersions(id string) ([]cluster.VersionInfo, error) {
	return nil, nil
}

func (ms *MemoryStorage) TypeVersion(id string, version uint64) (*cluster.Configuration, error) {
	return nil, nil
}

// Proposals is a test helper that decodes every normal raft entry in
// storage into a Proposal. Unmarshal failures panic — this helper is
// only used by tests against known-good data, and silently skipping
// would hide real test bugs (the same failure mode
// apply-unmarshal-error-handling removed from the production path).
func (ms *MemoryStorage) Proposals() []*cluster.Proposal {
	fi, _ := ms.FirstIndex()
	li, _ := ms.LastIndex()
	ents, _ := ms.Entries(fi, li+1, math.MaxUint64)

	var ps []*cluster.Proposal
	for _, e := range ents {
		if e.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(e.Data, ms); err != nil {
				panic(fmt.Sprintf("MemoryStorage.Proposals: unmarshal entry at index %d: %v", e.Index, err))
			}

			if len(p.Entities) > 0 {
				ps = append(ps, p)
			}
		}
	}

	return ps
}

// TODO Pull this reader out and make it concrete instead of an interface
type Reader struct {
	sync.Mutex
	index uint64
	s     db.Storage
}

func (r *Reader) Read() (uint64, *cluster.Proposal, error) {
	r.Lock()
	defer r.Unlock()

	for {
		readIndex := r.index + 1

		li, err := r.s.LastIndex()
		if err != nil {
			return 0, nil, err
		}

		if readIndex > li {
			return 0, nil, io.EOF
		}

		ents, err := r.s.Entries(readIndex, readIndex+1, math.MaxUint)
		if err != nil {
			return 0, nil, err
		}

		ent := ents[0]

		r.index = readIndex

		if ent.Type == raftpb.EntryNormal {
			p := &cluster.Proposal{}
			if err := p.Unmarshal(ent.Data, r.s); err != nil {
				return 0, nil, err
			}

			if len(p.Entities) > 0 && !cluster.IsSyncableIndex(p.Entities[0].Type.ID) {
				return readIndex, p, nil
			}
		}
	}
}
