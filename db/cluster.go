package db

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/cskr/pubsub"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// Cluster represents a cluster for the committeddb. It manages a raft cluster
// and n number of topics
type Cluster struct {
	id        int
	Databases map[string]types.Database
	Topics    map[string]*Topic
	Syncables map[string]syncable.Syncable
	nodes     []string
	proposeC  chan<- string
	syncp     *pubsub.PubSub
	storage   *raft.MemoryStorage

	join bool

	commitC <-chan *string
	errorC  <-chan error
}

// NewCluster creates a new Cluster
func NewCluster(nodes []string, id int, join bool) *Cluster {
	c := &Cluster{id: id, Topics: make(map[string]*Topic), nodes: nodes, join: join}

	return c
}

// Start starts the cluster
func (c *Cluster) Start() (chan raftpb.ConfChange, <-chan error) {
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)
	syncp := pubsub.New(0)

	c.proposeC = proposeC
	c.syncp = syncp

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady, storage := newRaftNode(
		c.id, c.nodes, c.join, getSnapshot, proposeC, confChangeC, syncp)

	c.storage = storage
	c.commitC = commitC
	c.errorC = errorC

	// We can't get rid of this until we have a select statement to take care of commitC
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	return confChangeC, c.errorC
}

// Shutdown shutdowns the cluster including closing server ports
func (c *Cluster) Shutdown() (err error) {
	log.Printf("Shutting down...")
	c.syncp.Shutdown()
	close(c.proposeC)
	// close(c.confChangeC)
	for range c.commitC {
		// drain pending commits
	}
	// wait for channel to close
	if erri := <-c.errorC; erri != nil {
		err = erri
	}

	return err
}

// Append proposes an addition to the raft
func (c *Cluster) Append(proposal types.Proposal) {
	// var buf bytes.Buffer
	buf := bytes.NewBufferString("")
	if err := gob.NewEncoder(buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	// log.Printf("[%d] Appending: %s to %v", c.id, buf.String(), c.proposeC)
	c.proposeC <- buf.String()
}

func decodeProposal(b []byte) (types.Proposal, error) {
	p := &types.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(p)
	return *p, err
}

// CreateTopic appends a topic to the raft and returns a Topic object if successful
func (c *Cluster) CreateTopic(name string) *Topic {
	t := newTopic(name)

	log.Printf("About to append topic: %s...\n", name)
	c.Append(types.Proposal{Topic: "topic", Proposal: name})
	log.Printf("...Appended topic: %s\n", name)

	c.Topics[name] = t
	return t
}

// CreateDatabase creates a database
func (c *Cluster) CreateDatabase(name string, database types.Database) error {
	log.Printf("About to append database: %s...\n", name)
	databaseJSON, _ := json.Marshal(database)
	// TODO We need a protobuf with name, type, and databaseJSON
	c.Append(types.Proposal{Topic: "database", Proposal: string(databaseJSON)})
	if err := database.Init(); err != nil {
		return err
	}
	log.Printf("...Appended database: %s\n", name)

	c.Databases[name] = database
	return nil
}

// CreateSyncable creates a Syncable, appends the original file to the raft, starts the syncable,
// and returns it if successful
func (c *Cluster) CreateSyncable(name string, syncable syncable.Syncable) error {
	log.Printf("About to append syncable: %s...\n", name)
	syncableJSON, _ := json.Marshal(syncable)
	// TODO We need a protobuf with name, type, and syncableJSON
	c.Append(types.Proposal{Topic: "syncable", Proposal: string(syncableJSON)})
	if err := syncable.Init(); err != nil {
		return err
	}
	log.Printf("...Appended syncable: %s\n", name)

	c.Syncables[name] = syncable
	go c.sync(context.Background(), syncable)

	return nil
}

func size(ctx context.Context, storage *raft.MemoryStorage) uint64 {
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()

	entries, error := storage.Entries(first, last+1, uint64(1024*1024))
	if error != nil {
		fmt.Println("[topic] Error retrieving entries from storage")
	}

	count := uint64(0)
	for _, e := range entries {
		if e.Type == raftpb.EntryNormal && len(e.Data) != 0 {
			count++
		}
	}

	return count
}

func readIndex(ctx context.Context, storage *raft.MemoryStorage, index uint64) string {
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()

	entries, error := storage.Entries(first, last+1, uint64(1024*1024))
	if error != nil {
		fmt.Println("[topic] Error retrieving entries from storage")
	}

	count := uint64(0)
	for _, e := range entries {
		if e.Type == raftpb.EntryNormal && len(e.Data) != 0 {
			if count == index {
				return string(e.Data[:])
			}
			count++
		}
	}

	// TODO This should be an error
	fmt.Println("[topic] Could not find index")
	return ""
}

// Sync the contents of the topic into a Syncable
func (c *Cluster) sync(ctx context.Context, s syncable.Syncable) {
	// We want to start the listener first, peek at the next append but don't consume it yet
	// then start reading the WAL until we hit that next append
	// lastly we want to drain the queue and stay up to date

	wait := make(chan bool)
	c.syncNode(ctx, s, wait)

	// How do we do the peek? Maybe we process twice and it is ok? Probably fine for the first pass
	// Is there a way to just do continuous processing where we keep the last index processed
	// and we just get notified when a new index is ready for processing so if we hit the end of
	// the WAL we know when to try again.

	// Now we need to read the WAL

	size := size(ctx, c.storage)

	for i := uint64(0); i < size; i++ {
		s.Sync(ctx, []byte(readIndex(ctx, c.storage, uint64(i))))
	}

	wait <- false
}

func (c *Cluster) syncNode(ctx context.Context, s syncable.Syncable, wait <-chan bool) {
	subc := c.syncp.Sub("StoredData")
	go func(chan interface{}, <-chan bool) {
		// We wait until we are ready to start processing the subscription
		select {
		case _ = <-wait:
		}

		for {
			select {
			case e := <-subc:
				if e != nil {
					s.Sync(ctx, e.(raftpb.Entry).Data)
				}
			default:
				time.Sleep(time.Millisecond * 1)
			}
		}
	}(subc, wait)
}
