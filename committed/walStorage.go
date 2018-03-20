package committed

import (
	"errors"
	"log"
	"os"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/coreos/etcd/wal/walpb"

	pb "github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
)

// WalStorage implements the Storage interface backed by a wal
type WalStorage struct {
	wal     *wal.WAL
	dirPath string
	// Protects access to all fields. Most methods of WalStorage are
	// run on the raft goroutine, but Append() is run on an application
	// goroutine.
	// TODO Not using this yet
	sync.Mutex

	hardState  pb.HardState
	snapshot   pb.Snapshot
	firstIndex uint64
	lastIndex  uint64
}

// NewWalStorage creates a new WalStorage with an open wal
func NewWalStorage(dirPath string) (*WalStorage, error) {
	// wal, err := wal.Open(dirPath, walpb.Snapshot{})
	wal, err := openWAL(dirPath, walpb.Snapshot{})

	if err != nil {
		log.Printf("openWAL err: %v", err)
		return nil, err
	}

	_, state, ents, err := wal.ReadAll()
	log.Printf("found %d entries", len(ents))

	if err != nil {
		log.Printf("wal.ReadAll err: %v", err)
		return nil, err
	}

	var snapshot pb.Snapshot
	var firstIndex uint64
	var lastIndex uint64

	if len(ents) > 0 {
		last := ents[len(ents)-1]
		firstIndex = ents[0].Index
		lastIndex = last.Index

		meta := pb.SnapshotMetadata{Index: last.Index, Term: last.Term}
		snapshot = pb.Snapshot{Metadata: meta}
	} else {
		firstIndex = 0
		lastIndex = 0
		snapshot = pb.Snapshot{Metadata: pb.SnapshotMetadata{}}
	}

	walStorage := WalStorage{
		wal:        wal,
		dirPath:    dirPath,
		hardState:  state,
		snapshot:   snapshot,
		firstIndex: firstIndex,
		lastIndex:  lastIndex,
	}

	return &walStorage, nil
}

func openWAL(dirPath string, walsnap walpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(dirPath) {
		if err := os.Mkdir(dirPath, 0750); err != nil {
			return nil, err
		}

		w, err := wal.Create(dirPath, nil)
		if err != nil {
			return nil, err
		}
		w.Close()
	}

	log.Printf("loading WAL in dir [%s] at term %d and index %d", dirPath, walsnap.Term, walsnap.Index)
	w, err := wal.Open(dirPath, walsnap)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// InitialState from the Storage interface
func (ws *WalStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ws.hardState, ws.snapshot.Metadata.ConfState, nil
}

// Entries from the Storage interface
func (ws *WalStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	owal, err := wal.OpenForRead(ws.dirPath, walpb.Snapshot{Index: lo})

	if err != nil {
		return nil, err
	}

	defer owal.Close()

	var length uint64
	himlo := hi - lo
	if himlo < maxSize {
		length = himlo
	} else {
		length = maxSize
	}
	_, _, ents, err := owal.ReadAll()
	if err != nil {
		return nil, err
	}
	return ents[lo:length], nil
}

// Term from the Storage interface
func (ws *WalStorage) Term(i uint64) (uint64, error) {
	offset := ws.firstIndex
	adjustedIndex := i - offset
	if i < offset {
		return 0, errors.New("i is less than first index")
	}
	if adjustedIndex >= ws.lastIndex-ws.firstIndex {
		return 0, errors.New("i is greater than last index")
	}

	entry, err := ws.Entries(adjustedIndex, adjustedIndex, 1)
	if err != nil {
		return 0, err
	}
	return entry[0].Term, nil
}

// LastIndex from the Storage interface
func (ws *WalStorage) LastIndex() (uint64, error) {
	return ws.lastIndex, nil
}

// FirstIndex from the Storage interface
func (ws *WalStorage) FirstIndex() (uint64, error) {
	return ws.firstIndex, nil
}

// Snapshot from the Storage interface
func (ws *WalStorage) Snapshot() (pb.Snapshot, error) {
	return ws.snapshot, nil
}

// Append the new entries to storage.
func (ws *WalStorage) Append(st raftpb.HardState, entries []pb.Entry) error {
	return ws.wal.Save(st, entries)
}

// SaveSnapshot caches a copy of the snapshot. It also generates a walpb.Snapshot and saves it in the wal
func (ws *WalStorage) SaveSnapshot(snapshot pb.Snapshot) {
	ws.snapshot = snapshot
	ws.wal.SaveSnapshot(walpb.Snapshot{Index: snapshot.Metadata.Index, Term: snapshot.Metadata.Term})
}

// Close closes the underlying wal
func (ws *WalStorage) Close() error {
	return ws.wal.Close()
}

/*
type Storage interface {
	// InitialState returns the saved HardState and ConfState information.
	InitialState() (pb.HardState, pb.ConfState, error)
	// Entries returns a slice of log entries in the range [lo,hi).
	// MaxSize limits the total size of the log entries returned, but
	// Entries returns at least one entry if any.
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)
	// Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]. The term of the entry before
	// FirstIndex is retained for matching purposes even though the
	// rest of that entry may not be available.
	Term(i uint64) (uint64, error)
	// LastIndex returns the index of the last entry in the log.
	LastIndex() (uint64, error)
	// FirstIndex returns the index of the first log entry that is
	// possibly available via Entries (older entries have been incorporated
	// into the latest Snapshot; if storage only contains the dummy entry the
	// first log entry is not available).
	FirstIndex() (uint64, error)
	// Snapshot returns the most recent snapshot.
	// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
	// so raft state machine could know that Storage needs some time to prepare
	// snapshot and call Snapshot later.
	Snapshot() (pb.Snapshot, error)
}
*/
