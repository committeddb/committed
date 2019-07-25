package db

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"

	"github.com/coreos/etcd/snap"
)

// Cluster represents a cluster for the committeddb and manages data for that cluster
type Cluster struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	snapshotter *snap.Snapshotter
	Data        *ClusterData
}

// ClusterData stores core primitives that will be stored in snapshots
type ClusterData struct {
	Databases map[string]types.Database
	Syncables map[string]syncable.Syncable
	Topics    map[string]*Topic
}

// NewCluster creates a new Cluster
func NewCluster(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *Cluster {
	data := &ClusterData{
		Databases: make(map[string]types.Database),
		Syncables: make(map[string]syncable.Syncable),
		Topics:    make(map[string]*Topic),
	}

	c := &Cluster{
		snapshotter: snapshotter,
		proposeC:    proposeC,
		Data:        data,
	}

	// replay log into cluster
	c.readCommits(commitC, errorC)
	// read commits from raft into cluster until error
	go c.readCommits(commitC, errorC)

	return c
}

func (c *Cluster) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := c.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := c.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var proposal types.Proposal
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&proposal); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}

		c.route(proposal)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (c *Cluster) route(entry types.Proposal) error {
	switch entry.Topic {
	case "database":
		// TODO How do know the name?
	case "syncable":
		// TODO How do know the name?
	case "topic":
		c.mu.Lock()
		c.Data.Topics[entry.Proposal] = &Topic{Name: entry.Proposal}
		c.mu.Unlock()
	}

	return nil
}

// Propose proposes an addition to the raft
func (c *Cluster) Propose(proposal types.Proposal) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	c.proposeC <- buf.String()
}

func decodeProposal(b []byte) (types.Proposal, error) {
	p := &types.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(p)
	return *p, err
}

// CreateTopic appends a topic to the raft and returns a Topic object if successful
func (c *Cluster) CreateTopic(name string) (*Topic, error) {
	if len(strings.TrimSpace(name)) == 0 {
		return nil, errors.New("name is empty")
	}

	if name == "database" || name == "syncable" || name == "topic" {
		return nil, fmt.Errorf("%s is a reserved name", name)
	}

	if _, ok := c.Data.Topics[name]; ok {
		return nil, fmt.Errorf("topic %s already exists", name)
	}

	t := newTopic(name)

	log.Printf("About to append topic: %s...\n", name)
	c.Propose(types.Proposal{Topic: "topic", Proposal: name})
	log.Printf("...Appended topic: %s\n", name)

	return t, nil
}

// CreateDatabase creates a database
func (c *Cluster) CreateDatabase(name string, database types.Database) error {
	if _, ok := c.Data.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	log.Printf("About to append database: %s...\n", name)
	databaseJSON, _ := json.Marshal(database)
	// TODO We need a protobuf with name, type, and databaseJSON
	c.Propose(types.Proposal{Topic: "database", Proposal: string(databaseJSON)})
	if err := database.Init(); err != nil {
		return err
	}
	log.Printf("...Appended database: %s\n", name)

	// TODO Should this happen through the message_router?
	c.Data.Databases[name] = database
	return nil
}

// CreateSyncable creates a Syncable, appends the original file to the raft, starts the syncable,
// and returns it if successful
func (c *Cluster) CreateSyncable(name string, syncable syncable.Syncable) error {
	if _, ok := c.Data.Syncables[name]; ok {
		return fmt.Errorf("syncable %s already exists", name)
	}

	log.Printf("About to append syncable: %s...\n", name)
	syncableJSON, _ := json.Marshal(syncable)
	// TODO We need a protobuf with name, type, and syncableJSON
	c.Propose(types.Proposal{Topic: "syncable", Proposal: string(syncableJSON)})
	if err := syncable.Init(); err != nil {
		return err
	}
	log.Printf("...Appended syncable: %s\n", name)

	// TODO Should this happen through the message_router?
	c.Data.Syncables[name] = syncable
	// TODO Syncables know how to update themselves
	// go c.sync(context.Background(), syncable)

	return nil
}

// GetSnapshot gets a snapshot of the current data struct
func (c *Cluster) GetSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.Data)
	return nil, nil
}

// recoverFromSnapshot loads the latest data struct from the given snapshot
func (c *Cluster) recoverFromSnapshot(snapshot []byte) error {
	var data *ClusterData
	if err := json.Unmarshal(snapshot, data); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data = data
	return nil
}
