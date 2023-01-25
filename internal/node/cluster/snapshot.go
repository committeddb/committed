package cluster

import (
	"bytes"
	"encoding/gob"

	"github.com/philborlin/committed/internal/node/bridge"
	"github.com/pkg/errors"
)

// Snapshot contains all the data stored in a snapshot
type Snapshot struct {
	TOML    *TOML
	Bridges map[string]*bridge.Snapshot
}

// GetSnapshot implements Snapshotter and gets a snapshot of the current data struct
func (c *Cluster) GetSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	b := make(map[string]*bridge.Snapshot)
	for k, v := range c.Data.Bridges {
		b[k] = v.GetSnapshot()
	}
	s := Snapshot{TOML: c.TOML, Bridges: b}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(s); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ApplySnapshot implements Snapshotter
func (c *Cluster) ApplySnapshot(snapshot []byte) error {
	var s Snapshot
	dec := gob.NewDecoder(bytes.NewBuffer(snapshot))
	if err := dec.Decode(&s); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.TOML = s.TOML
	for _, v := range c.TOML.Databases {
		if err := c.AddDatabase([]byte(v)); err != nil {
			return errors.Wrap(err, "Error while applying snapshot")
		}
	}
	for _, v := range c.TOML.Topics {
		if err := c.RestoreTopic([]byte(v)); err != nil {
			return errors.Wrap(err, "Error while applying snapshot")
		}
	}
	for k, v := range c.TOML.Syncables {
		snap, ok := s.Bridges[k]
		if !ok {
			snap = nil
		}
		if err := c.AddSyncable([]byte(v), snap); err != nil {
			return errors.Wrap(err, "Error while applying snapshot")
		}
	}

	return nil
}
