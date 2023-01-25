package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"strings"

	"github.com/philborlin/committed/internal/node/syncable"
	"github.com/philborlin/committed/internal/node/topic"
	"github.com/philborlin/committed/internal/node/types"
	"github.com/pkg/errors"
)

// Propose proposes an addition to the raft
func (c *Cluster) Propose(proposal types.Proposal) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	c.proposeC <- buf.Bytes()
}

// ProposeTopic appends a topic to the raft
func (c *Cluster) ProposeTopic(toml string) error {
	name, err := topic.PreParseTopic("toml", strings.NewReader(toml), c.dataDir)
	if err != nil {
		return errors.Wrap(err, "Could not create proposed topic")
	}

	if name == "database" || name == "syncable" || name == "topic" || strings.HasPrefix(name, "bridge") {
		return fmt.Errorf("%s is a reserved name", name)
	}

	if _, ok := c.Data.Topics[name]; ok {
		return fmt.Errorf("topic %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "topic", Proposal: []byte(toml)})

	return nil
}

// ProposeDatabase appends a database to the raft
func (c *Cluster) ProposeDatabase(toml string) error {
	name, _, err := syncable.ParseDatabase("toml", strings.NewReader(toml))
	if err != nil {
		return errors.Wrap(err, "Could not create proposed database")
	}

	if _, ok := c.Data.Databases[name]; ok {
		return fmt.Errorf("database %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "database", Proposal: []byte(toml)})

	return nil
}

// ProposeSyncable appends a syncable to the raft
func (c *Cluster) ProposeSyncable(toml string) error {
	name, _, err := syncable.ParseSyncable("toml", strings.NewReader(toml), c.Data.Databases)
	if err != nil {
		return errors.Wrap(err, "Could not create proposed syncable")
	}

	if _, ok := c.Data.Syncables[name]; ok {
		return fmt.Errorf("syncable %s already exists", name)
	}

	c.Propose(types.Proposal{Topic: "syncable", Proposal: []byte(toml)})

	return nil
}
