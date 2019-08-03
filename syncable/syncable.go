package syncable

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/philborlin/committed/types"
)

// TODO A couple of things:
//    1. We need the actual topic so we can read from the wal
//    2. We need to store the last synced segment and index numbers of the wal
//    3. We should take part in the global snapshot

// TopicSyncable is a Syncable that includes the topics it syncs on
type TopicSyncable interface {
	Syncable
	topics() []string
}

// Syncable represents a synchable concept
type Syncable interface {
	Init() error
	Sync(ctx context.Context, entry raftpb.Entry) error
	Close() error
}

type syncableWrapper struct {
	Syncable    TopicSyncable
	topics      map[string]struct{}
	snapshotter *snap.Snapshotter
}

func newSyncableWrapper(name string, s TopicSyncable, snapshotDir string) *syncableWrapper {
	var topics = make(map[string]struct{})
	for _, item := range s.topics() {
		topics[item] = struct{}{}
	}
	snapshotter := snap.New(snapshotDir + "/" + name)
	return &syncableWrapper{Syncable: s, topics: topics, snapshotter: snapshotter}
}

func (s *syncableWrapper) containsTopic(t string) bool {
	_, ok := s.topics[t]
	return ok
}

// Init implements Syncable
func (s *syncableWrapper) Init() error {
	return s.Syncable.Init()
}

// Sync implements Syncable
// During Sync maintain a record of applied entry index and terms and save those in snapshots.
// Maybe don't sync the data but just use this as a message that there is more data and that
// we need to go and read more from the wal. This allows us to drop messages if we aren't caught
// up with the wal (since we will pick them up in the wal), and prevents us from monitoring the
// wal because we will have a notification when there is more data.

// We will also want to keep track of an applied index so if a message gets delivered twice, we
// don't apply it twice. We also need to watch out for go routines to make sure messages get
// applied in order. Does that mean we need a buffer? Probably, and it needs to be user
// configurable. This way we block new appends if we can't keep up while syncing. [Theory] The
// larger the io cababilities of the system the larger the buffer can be.
func (s *syncableWrapper) Sync(ctx context.Context, entry raftpb.Entry) error {
	byteSlice := entry.Data
	p, err := decode(byteSlice)
	if err == nil && s.containsTopic(p.Topic) {
		entry.Data = []byte(p.Proposal)
		if err = s.Syncable.Sync(ctx, entry); err != nil {
			return err
		}

		// Mandatory - we have to send this through the raft so that all nodes get the snapshot, not just the leader

		// TODO Do we want to set a timer so we don't snapshot on ever sync, but maybe snapshot in x seconds?
		if err = s.saveSnap(entry); err != nil {
			// Just log, this is not a critical error
			fmt.Printf("[Warning] Snapshot failed: %v", err)
		}
	}

	return err
}

func (s *syncableWrapper) saveSnap(entry raftpb.Entry) error {
	metadata := raftpb.SnapshotMetadata{Index: entry.Index, Term: entry.Term}
	snapshot := raftpb.Snapshot{Metadata: metadata}
	return s.snapshotter.SaveSnap(snapshot)
}

// Close implements Syncable
func (s *syncableWrapper) Close() error {
	return s.Syncable.Close()
}

func decode(byteSlice []byte) (types.Proposal, error) {
	p := types.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(byteSlice)).Decode(&p)
	return p, err
}
