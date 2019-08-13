package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/philborlin/committed/bridge"
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
	errorC      chan error
	proposeC    chan<- []byte // channel for proposing updates
	snapshotter *snap.Snapshotter
	Data        *Data
}

// Data stores core primitives that will be stored in snapshots
type Data struct {
	Databases map[string]types.Database
	Syncables map[string]syncable.Syncable
	Topics    map[string]topic.Topic
}

// New creates a new Cluster
func New(snapshotter *snap.Snapshotter, proposeC chan<- []byte, commitC <-chan []byte,
	errorC <-chan error, dataDir string) *Cluster {
	data := &Data{
		Databases: make(map[string]types.Database),
		Syncables: make(map[string]syncable.Syncable),
		Topics:    make(map[string]topic.Topic),
	}

	c := &Cluster{
		dataDir:     dataDir,
		errorC:      make(chan error),
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

func (c *Cluster) readCommits(commitC <-chan []byte, errorC <-chan error) {
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
			if err := c.ApplySnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		ap, err := types.NewAcceptedProposal(data)
		if err != nil {
			log.Printf("could not decode message (%v)", err)
		}

		err = c.route(ap)
		if err != nil {
			log.Printf("could not route message (%v)", err)
		}
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (c *Cluster) route(ap *types.AcceptedProposal) error {
	switch ap.Topic {
	case "database":
		name, database, err := syncable.ParseDatabase("toml", strings.NewReader(string(ap.Data)))
		if err != nil {
			return errors.Wrap(err, "Router could not create database")
		}

		log.Printf("About to initialize database: %s...\n", name)
		if err := database.Init(); err != nil {
			return errors.Wrapf(err, "could not initialize database %s", name)
		}
		log.Printf("...Initialized database: %s\n", name)

		c.mu.Lock()
		c.Data.Databases[name] = database
		c.mu.Unlock()
	case "syncable":
		name, syncable, err := syncable.ParseSyncable("toml", strings.NewReader(string(ap.Data)), c.Data.Databases)
		if err != nil {
			return errors.Wrap(err, "Router could not create syncable")
		}

		log.Printf("About to initialize syncable: %s...\n", name)
		if err := syncable.Init(context.Background()); err != nil {
			return errors.Wrapf(err, "could not initialize syncable %s", name)
		}
		log.Printf("...Initialized syncable: %s\n", name)

		c.mu.Lock()
		c.Data.Syncables[name] = syncable
		c.mu.Unlock()

		bridge, err := bridge.New("", syncable, c.Data.Topics)
		if err != nil {
			return errors.Wrap(err, "Router could not create bridge")
		}
		go func() {
			err := bridge.Init(context.Background(), c.errorC)
			if err != nil {
				c.errorC <- err
			}
		}()
	case "topic":
		name, topic, err := topic.ParseTopic("toml", strings.NewReader(string(ap.Data)), c.dataDir)
		if err != nil {
			return errors.Wrap(err, "Router could not create topic")
		}

		c.mu.Lock()
		c.Data.Topics[name] = topic
		c.mu.Unlock()
	default:
		t, ok := c.Data.Topics[ap.Topic]
		if !ok {
			return fmt.Errorf("Attempting to append to topic %s which was not found", c.dataDir)
		}

		t.Append(topic.Data{Index: ap.Index, Term: ap.Term, Data: ap.Data})
	}

	return nil
}

// Propose proposes an addition to the raft
func (c *Cluster) Propose(proposal types.Proposal) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	c.proposeC <- buf.Bytes()
}

func decodeProposal(b []byte) (types.Proposal, error) {
	p := &types.Proposal{}
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(p)
	return *p, err
}

// CreateTopic appends a topic to the raft and returns a Topic object if successful
func (c *Cluster) CreateTopic(toml string) error {
	name, err := topic.PreParseTopic("toml", strings.NewReader(toml), c.dataDir)
	if err != nil {
		return errors.Wrap(err, "Could not create topic")
	}

	if name == "database" || name == "syncable" || name == "topic" {
		return fmt.Errorf("%s is a reserved name", name)
	}

	if _, ok := c.Data.Topics[name]; ok {
		return fmt.Errorf("topic %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "topic", Proposal: []byte(toml)})

	return nil
}

// CreateDatabase creates a database
func (c *Cluster) CreateDatabase(toml string) error {
	name, _, err := syncable.ParseDatabase("toml", strings.NewReader(toml))
	if err != nil {
		return errors.Wrap(err, "Could not create database")
	}

	if _, ok := c.Data.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "database", Proposal: []byte(toml)})

	return nil
}

// CreateSyncable creates a Syncable
func (c *Cluster) CreateSyncable(toml string) error {
	name, _, err := syncable.ParseSyncable("toml", strings.NewReader(toml), c.Data.Databases)
	if err != nil {
		return errors.Wrap(err, "Could not create syncable")
	}

	if _, ok := c.Data.Syncables[name]; ok {
		return fmt.Errorf("syncable %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "syncable", Proposal: []byte(toml)})

	return nil
}

// GetSnapshot implements Snapshotter and gets a snapshot of the current data struct
func (c *Cluster) GetSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return json.Marshal(c.Data)
}

// ApplySnapshot implements Snapshotter
func (c *Cluster) ApplySnapshot(snapshot []byte) error {
	// TODO This has to setup the bridges
	var data *Data
	if err := json.Unmarshal(snapshot, data); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data = data
	return nil
}
