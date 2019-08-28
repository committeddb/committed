package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/philborlin/committed/bridge"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/topic"
	"github.com/philborlin/committed/types"
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
	c.TOML.Databases = append(c.TOML.Databases, string(toml))
	c.Data.Databases[name] = database
	c.mu.Unlock()

	return nil
}

// AddSyncable creates a syncable
func (c *Cluster) AddSyncable(toml []byte, snap *bridge.Snapshot) error {
	name, syncable, err := syncable.ParseSyncable("toml", strings.NewReader(string(toml)), c.Data.Databases)
	if err != nil {
		return errors.Wrap(err, "Router could not create syncable")
	}

	log.Printf("About to initialize syncable: %s...\n", name)
	if err := syncable.Init(context.Background()); err != nil {
		return errors.Wrapf(err, "could not initialize syncable %s", name)
	}
	log.Printf("...Initialized syncable: %s\n", name)

	bridge, err := bridgeFactory.New(name, syncable, c.Data.Topics, c.leader, c, snap)
	if err != nil {
		return errors.Wrap(err, "Router could not create bridge")
	}
	go func() {
		// TODO Make the 5 seconds tunable in the config file
		err := bridge.Init(context.Background(), c.errorC, 5*time.Second)
		if err != nil {
			c.errorC <- err
		}
	}()

	c.mu.Lock()
	c.TOML.Syncables[name] = string(toml)
	c.Data.Syncables[name] = syncable
	c.Data.Bridges[name] = bridge
	c.mu.Unlock()

	return nil
}

// AddTopic creates a topic
func (c *Cluster) AddTopic(toml []byte) error {
	name, topic, err := topic.ParseTopic("toml", strings.NewReader(string(toml)), c.dataDir)
	if err != nil {
		return errors.Wrap(err, "Router could not create topic")
	}

	c.mu.Lock()
	c.TOML.Topics = append(c.TOML.Topics, string(toml))
	c.Data.Topics[name] = topic
	c.mu.Unlock()

	return nil
}

func (c *Cluster) UpdateBridge(ap *types.AcceptedProposal) error {
	name := ap.Topic[7:len(ap.Topic)]
	b, ok := c.Data.Bridges[name]
	if !ok {
		return fmt.Errorf("Couldn't find bridge for %s", ap.Topic)
	}
	i, err := types.NewIndex(ap.Data)
	if err != nil {
		return errors.Wrapf(err, "Couldn't decode index for %s", ap.Topic)
	}
	b.UpdateIndex(*i)

	return nil
}
