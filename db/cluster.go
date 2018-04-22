package db

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/philborlin/committed/syncable"

	"github.com/coreos/etcd/raft/raftpb"
)

// Cluster represents a cluster for the committeddb. It manages a raft cluster
// and n number of topics
type Cluster struct {
	id        int
	topics    map[string]*Topic
	syncables map[string][]syncable.Syncable
	nodes     []string
	proposeC  chan<- string
}

// Proposal is an item to put on a raft log
type Proposal struct {
	Topic    string
	Proposal string
}

// NewCluster creates a new Cluster
func NewCluster(nodes []string, id int, apiPort int, join bool) *Cluster {
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	c := &Cluster{id: id, topics: make(map[string]*Topic), nodes: nodes, proposeC: proposeC}

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(id, nodes, join, getSnapshot, proposeC, confChangeC)
	// _, errorC, _ := newRaftNode(*id, nodes, *join, getSnapshot, proposeC, confChangeC)

	// We can't get rid of this until we have a select statement to take care of commitC
	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveAPI(c, apiPort, confChangeC, errorC)

	return c
}

// Append proposes an addition to the raft
func (c *Cluster) Append(proposal Proposal) {
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

	c.Append(Proposal{Topic: "topic", Proposal: name})

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

	c.Append(Proposal{Topic: "syncable", Proposal: syncableFile})

	for _, t := range s.Topics() {
		syncs := append(c.syncables[t], s)
		c.syncables[t] = syncs
	}

	// Add to the pub/sub

	return s
}

// Sync the contents of the topic into a Syncable
// func (c *Cluster) Sync(ctx context.Context, s syncable.Syncable) {
// 	size := t.size(ctx)

// 	for i := uint64(0); i < size; i++ {
// 		s.Sync(ctx, []byte(t.ReadIndex(ctx, uint64(i))))
// 	}

// 	for _, n := range t.Nodes {
// 		syncNode(ctx, s, n)
// 	}
// }

// func syncNode(ctx context.Context, s syncable.Syncable) {
// 	subc := n.syncp.Sub("StoredData")
// 	go func() {
// 		for {
// 			select {
// 			case e := <-subc:
// 				s.Sync(ctx, e.(raftpb.Entry).Data)
// 			default:
// 				time.Sleep(time.Millisecond * 1)
// 			}
// 		}
// 	}()
// }
