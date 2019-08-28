package cluster

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/philborlin/committed/bridge"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/types"

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
	TOML        *TOML
	leader      types.Leader
}

// Data stores core primitives
type Data struct {
	Databases map[string]syncable.Database
	Syncables map[string]syncable.Syncable
	Topics    map[string]topic.Topic
	Bridges   map[string]bridge.Bridge
}

// TOML stores the toml config files for each primitive. The snapshot will be based on these plus some additional data
type TOML struct {
	Databases []string
	Syncables map[string]string
	Topics    []string
}

// New creates a new Cluster
func New(snapshotter *snap.Snapshotter, proposeC chan<- []byte, commitC <-chan *types.AcceptedProposal,
	errorC <-chan error, dataDir string, leader types.Leader) *Cluster {
	data := &Data{
		Databases: make(map[string]syncable.Database),
		Syncables: make(map[string]syncable.Syncable),
		Topics:    make(map[string]topic.Topic),
		Bridges:   make(map[string]bridge.Bridge),
	}

	toml := &TOML{}

	c := &Cluster{
		dataDir:     dataDir,
		errorC:      make(chan error),
		proposeC:    proposeC,
		snapshotter: snapshotter,
		Data:        data,
		TOML:        toml,
		leader:      leader,
	}

	// replay log into cluster
	c.readCommits(commitC, errorC)
	// read commits from raft into cluster until error
	go c.readCommits(commitC, errorC)

	return c
}

func (c *Cluster) readCommits(commitC <-chan *types.AcceptedProposal, errorC <-chan error) {
	for ap := range commitC {
		if ap == nil {
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

		err := c.route(ap)
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
		return c.AddDatabase(ap.Data)
	case "syncable":
		return c.AddSyncable(ap.Data, nil)
	case "topic":
		return c.AddTopic(ap.Data)
	default:
		if strings.HasPrefix(ap.Topic, "bridge.") {
			return c.UpdateBridge(ap)
		}
		t, ok := c.Data.Topics[ap.Topic]
		if !ok {
			return fmt.Errorf("Attempting to append to topic %s which was not found", c.dataDir)
		}

		return t.Append(topic.Data{Index: ap.Index, Term: ap.Term, Data: ap.Data})
	}
}
