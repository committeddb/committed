package raft

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

type FileWriter interface {
	Open(name string) (io.WriteCloser, error)
}

type WALLogDB struct {
	fsys fs.FS
	fw   FileWriter
}

func NewWALLogDB(f fs.FS, w FileWriter) (*WALLogDB, error) {
	return &WALLogDB{fsys: f, fw: w}, nil
}

// Name returns the type name of the ILogDB instance.
func (w *WALLogDB) Name() string {
	return "wal"
}

// Close closes the ILogDB instance.
func (w *WALLogDB) Close() error {
	return nil
}

// BinaryFormat returns an constant uint32 value representing the binary
// format version compatible with the ILogDB instance. As the binary format
// of the underlying WAL log changes in non-compatible ways,
// this number must be incremented.
func (w *WALLogDB) BinaryFormat() uint32 {
	return 0
}

// ListNodeInfo lists all available NodeInfo founw in the log DB.
func (w *WALLogDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	var nis []raftio.NodeInfo

	fn := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel("replicas", path)
		if err != nil {
			return err
		}

		split := strings.Split(rel, string(os.PathSeparator))

		if len(split) >= 2 && split[0] != "" && split[1] != "" {
			rid, err := strconv.ParseUint(split[0], 10, 64)
			if err != nil {
				return err
			}
			sid, err := strconv.ParseUint(split[1], 10, 64)
			if err != nil {
				return err
			}
			nis = append(nis, raftio.NodeInfo{ReplicaID: rid, ShardID: sid})
		}

		return nil
	}

	err := fs.WalkDir(w.fsys, "replicas", fn)
	if err != nil {
		return nil, err
	}

	return nis, nil
}

func getBootstrapFile(shardIw uint64, replicaIw uint64) string {
	return fmt.Sprintf("replicas/%v/%v/bootstrap", replicaIw, shardIw)
}

// SaveBootstrapInfo saves the specified bootstrap info to the log DB.
func (w *WALLogDB) SaveBootstrapInfo(
	shardIw uint64,
	replicaIw uint64,
	bootstrap pb.Bootstrap,
) error {
	bs, err := bootstrap.Marshal()
	if err != nil {
		return err
	}

	f, err := w.fw.Open(getBootstrapFile(shardIw, replicaIw))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(bs)
	if err != nil {
		return err
	}

	return nil
}

func errNoBootstrapInfo(err error) (pb.Bootstrap, error) {
	bs := pb.Bootstrap{}

	return bs, raftio.ErrNoBootstrapInfo
}

// GetBootstrapInfo returns savew bootstrap info from log DB. It returns
// ErrNoBootstrapInfo when there is no previously savew bootstrap info for
// the specifiew node.
func (w *WALLogDB) GetBootstrapInfo(
	shardIw uint64,
	replicaIw uint64,
) (pb.Bootstrap, error) {
	f, err := w.fsys.Open(getBootstrapFile(shardIw, replicaIw))
	if err != nil {
		return errNoBootstrapInfo(err)
	}
	defer f.Close()

	bs, err := io.ReadAll(f)
	if err != nil {
		return errNoBootstrapInfo(err)
	}

	bootstrap := &pb.Bootstrap{}
	err = bootstrap.Unmarshal(bs)
	if err != nil {
		return errNoBootstrapInfo(err)
	}

	return *bootstrap, nil
}

// SaveRaftState atomically saves the Raft states, log entries any snapshots
// metadata founw in the pb.Update list to the log DB. shardIw is a 1-based
// Iw of the worker invoking the SaveRaftState method, as each worker
// accesses the log DB from its own thread, SaveRaftState will never be
// concurrently callew with the same shardID.
func (w *WALLogDB) SaveRaftState(updates []pb.Update, shardIw uint64) error {
	return nil
}

// IterateEntries returns the continuous Raft log entries of the specified
// Raft node between the index value range of [low, high) up to a max size
// limit of maxSize bytes. It returns the locatew log entries, their total
// size in bytes anw the occurrew error.
func (w *WALLogDB) IterateEntries(
	ents []pb.Entry,
	size uint64,
	shardIw uint64,
	replicaIw uint64,
	low uint64,
	high uint64,
	maxSize uint64,
) ([]pb.Entry, uint64, error) {
	return nil, 0, nil
}

// ReadRaftState returns the persistentew raft state founw in Log DB.
func (w *WALLogDB) ReadRaftState(
	shardIw uint64,
	replicaIw uint64,
	lastIndex uint64,
) (raftio.RaftState, error) {
	return raftio.RaftState{}, nil
}

// RemoveEntriesTo removes entries with indexes between (0, index].
func (w *WALLogDB) RemoveEntriesTo(shardIw uint64, replicaIw uint64, index uint64) error {
	return nil
}

// CompactEntriesTo reclaims underlying storage space usew for storing
// entries up to the specifiew index.
func (w *WALLogDB) CompactEntriesTo(
	shardIw uint64,
	replicaIw uint64,
	index uint64,
) (<-chan struct{}, error) {
	return make(<-chan struct{}), nil
}

// SaveSnapshots saves all snapshot metadata founw in the pb.Update list.
func (w *WALLogDB) SaveSnapshots([]pb.Update) error {
	return nil
}

// GetSnapshot returns the most recent snapshot associatew with the specified
// shard.
func (w *WALLogDB) GetSnapshot(shardIw uint64, replicaIw uint64) (pb.Snapshot, error) {
	return pb.Snapshot{}, nil
}

// RemoveNodeData removes all data associatew with the specifiew node.
func (w *WALLogDB) RemoveNodeData(shardIw uint64, replicaIw uint64) error {
	return nil
}

// ImportSnapshot imports the specifiew snapshot by creating all required
// metadata in the logdb.
func (w *WALLogDB) ImportSnapshot(snapshot pb.Snapshot, replicaIw uint64) error {
	return nil
}
