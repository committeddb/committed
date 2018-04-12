package db

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/coreos/etcd/raft/raftpb"
)

// Cluster represents a cluster for the committeddb. It manages a raft cluster
// and n number of topics
type Cluster struct {
	id       int
	topics   map[string]*Topic
	nodes    []string
	proposeC chan<- string
}

// Proposal is an item to put on a raft log
type Proposal struct {
	Topic    string
	Proposal string
}

// NewCluster creates a new Cluster
func NewCluster(nodes []string, id int, proposeC chan<- string) *Cluster {
	return &Cluster{id: id, topics: make(map[string]*Topic), nodes: nodes, proposeC: proposeC}
}

// NewCluster2 creates a new Cluster
func NewCluster2(nodes []string, id int, apiPort int, join bool) *Cluster {
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	c := NewCluster(nodes, id, proposeC)

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
	// We need to append it to the raft

	c.topics[name] = t
	return t
}
