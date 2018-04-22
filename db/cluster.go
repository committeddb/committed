package db

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"time"

	"github.com/cskr/pubsub"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/util"

	"github.com/coreos/etcd/raft/raftpb"
)

// Cluster represents a cluster for the committeddb. It manages a raft cluster
// and n number of topics
type Cluster struct {
	id       int
	topics   map[string]*Topic
	nodes    []string
	proposeC chan<- string
	syncp    *pubsub.PubSub
}

// NewCluster creates a new Cluster
func NewCluster(nodes []string, id int, apiPort int, join bool) *Cluster {
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	syncp := pubsub.New(0)
	defer syncp.Shutdown()

	c := &Cluster{id: id, topics: make(map[string]*Topic), nodes: nodes, proposeC: proposeC}

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(id, nodes, join, getSnapshot, proposeC, confChangeC, syncp)
	// _, errorC, _ := newRaftNode(*id, nodes, *join, getSnapshot, proposeC, confChangeC)

	// We can't get rid of this until we have a select statement to take care of commitC
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveAPI(c, apiPort, confChangeC, errorC)

	return c
}

// Append proposes an addition to the raft
func (c *Cluster) Append(proposal util.Proposal) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	log.Printf("[%d] Appending: %s to %v", c.id, buf.String(), c.proposeC)
	c.proposeC <- buf.String()
}

// CreateTopic appends a topic to the raft and returns a Topic object if successful
func (c *Cluster) CreateTopic(name string) *Topic {
	t := newTopic(name)

	c.Append(util.Proposal{Topic: "topic", Proposal: name})

	c.topics[name] = t
	return t
}

// CreateSyncable creates a Syncable, appends the original file to the raft, starts the syncable,
// and returns it if successful
func (c *Cluster) CreateSyncable(style string, syncableFile string) syncable.Syncable {
	s, err := syncable.Parse(style, []byte(syncableFile))
	if err != nil {
		log.Printf("Failed to create syncable: %v", err)
		return nil
	}

	c.Append(util.Proposal{Topic: "syncable", Proposal: syncableFile})

	// Add to the pub/sub
	c.sync(context.Background(), s)

	return s
}

// Sync the contents of the topic into a Syncable
func (c *Cluster) sync(ctx context.Context, s syncable.Syncable) {
	// We want to start the listener first, peek at the next append but don't consume it yet
	// then start reading the WAL until we hit that next append
	// lastly we want to drain the queue and stay up to date

	// size := t.size(ctx)

	// for i := uint64(0); i < size; i++ {
	// 	s.Sync(ctx, []byte(t.ReadIndex(ctx, uint64(i))))
	// }

	// for _, n := range t.Nodes {
	// 	syncNode(ctx, s)
	// }
}

func (c *Cluster) syncNode(ctx context.Context, s syncable.Syncable) {
	// We need a way to pause the syncing of the appends until the WAL crawler is up to date

	subc := c.syncp.Sub("StoredData")
	go func() {
		for {
			select {
			case e := <-subc:
				s.Sync(ctx, e.(raftpb.Entry).Data)
			default:
				time.Sleep(time.Millisecond * 1)
			}
		}
	}()
}
