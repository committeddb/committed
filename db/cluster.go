package db

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/types"
	"github.com/pkg/errors"

	"github.com/coreos/etcd/snap"
)

// Cluster represents a cluster for the committeddb and manages data for that cluster
type Cluster struct {
	dataDir     string
	mu          sync.RWMutex
	proposeC    chan<- string // channel for proposing updates
	snapshotter *snap.Snapshotter
	Data        *ClusterData
}

// ClusterData stores core primitives that will be stored in snapshots
type ClusterData struct {
	Databases map[string]types.Database
	Syncables map[string]syncable.Syncable
	Topics    map[string]*topic.Topic
}

// NewCluster creates a new Cluster
func NewCluster(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, dataDir string) *Cluster {
	data := &ClusterData{
		Databases: make(map[string]types.Database),
		Syncables: make(map[string]syncable.Syncable),
		Topics:    make(map[string]*topic.Topic),
	}

	c := &Cluster{
		dataDir:     dataDir,
		proposeC:    proposeC,
		snapshotter: snapshotter,
		Data:        data,
	}

	// replay log into cluster
	c.readCommits(commitC, errorC)
	// read commits from raft into cluster until error
	go c.readCommits(commitC, errorC)

	return c
}

// TODO Instead of just the data string we need the whole entry so we can get the index and term to
// store in the topic wals
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

		var ap types.AcceptedProposal
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&ap); err != nil {
			log.Printf("could not decode message (%v)", err)
		}

		c.route(ap)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (c *Cluster) route(ap types.AcceptedProposal) error {
	switch ap.Topic {
	case "database":
		// TODO How do know the name? - Should we send the toml instead of the object?
	case "syncable":
		// TODO How do know the name? - Should we send the toml instead of the object?
	case "topic":
		name := string(ap.Data)
		t, err := topic.New(name, c.dataDir)
		if err != nil {
			return errors.Wrapf(err, "Error creating new topic")
		}
		c.mu.Lock()
		c.Data.Topics[name] = t
		c.mu.Unlock()
	default:
		if t, ok := c.Data.Topics[ap.Topic]; ok != false {
			t.Append(topic.Data{Index: ap.Index, Term: ap.Term, Data: ap.Data})
		}
		// TODO We may want to alert any synables listening to this topic so they can sync more data without polling.
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
func (c *Cluster) CreateTopic(name string) error {
	if len(strings.TrimSpace(name)) == 0 {
		return fmt.Errorf("name is empty")
	}

	if name == "database" || name == "syncable" || name == "topic" {
		return fmt.Errorf("%s is a reserved name", name)
	}

	if _, ok := c.Data.Topics[name]; ok {
		return fmt.Errorf("topic %s already exists", name)
	}

	log.Printf("About to append topic: %s...\n", name)
	c.Propose(types.Proposal{Topic: "topic", Proposal: []byte(name)})
	log.Printf("...Appended topic: %s\n", name)

	return nil
}

// CreateDatabase creates a database
// This should be about validating the toml and then sending that to the raft instead of the database object
func (c *Cluster) CreateDatabase(name string, database types.Database) error {
	if _, ok := c.Data.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	log.Printf("About to append database: %s...\n", name)
	databaseJSON, _ := json.Marshal(database)
	// TODO We need a protobuf with name, type, and databaseJSON
	c.Propose(types.Proposal{Topic: "database", Proposal: databaseJSON})
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
// This should be about validating the toml and then sending that to the raft instead of the syncable object
func (c *Cluster) CreateSyncable(name string, syncable syncable.Syncable) error {
	if _, ok := c.Data.Syncables[name]; ok {
		return fmt.Errorf("syncable %s already exists", name)
	}

	log.Printf("About to append syncable: %s...\n", name)
	syncableJSON, _ := json.Marshal(syncable)
	// TODO We need a protobuf with name, type, and syncableJSON
	c.Propose(types.Proposal{Topic: "syncable", Proposal: syncableJSON})
	if err := syncable.Init(context.Background()); err != nil {
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
