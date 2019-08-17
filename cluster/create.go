package cluster

import (
	"context"
	"log"
	"strings"

	"github.com/philborlin/committed/bridge"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/topic"
	"github.com/pkg/errors"
)

var bridgeFactory bridge.Factory = &bridge.TopicSyncableBridgeFactory{}

// AddDatabase creates a database
func (c *Cluster) AddDatabase(toml []byte) error {
	name, database, err := syncable.ParseDatabase("toml", strings.NewReader(string(toml)))
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

	return nil
}

// AddSyncable creates a syncable
func (c *Cluster) AddSyncable(toml []byte) error {
	name, syncable, err := syncable.ParseSyncable("toml", strings.NewReader(string(toml)), c.Data.Databases)
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

	bridge, err := bridgeFactory.New(name, syncable, c.Data.Topics)
	if err != nil {
		return errors.Wrap(err, "Router could not create bridge")
	}
	go func() {
		err := bridge.Init(context.Background(), c.errorC)
		if err != nil {
			c.errorC <- err
		}
	}()

	return nil
}

// AddTopic creates a topic
func (c *Cluster) AddTopic(toml []byte) error {
	name, topic, err := topic.ParseTopic("toml", strings.NewReader(string(toml)), c.dataDir)
	if err != nil {
		return errors.Wrap(err, "Router could not create topic")
	}

	c.mu.Lock()
	c.Data.Topics[name] = topic
	c.mu.Unlock()

	return nil
}
