package cluster

import "encoding/json"

// GetSnapshot implements Snapshotter and gets a snapshot of the current data struct
func (c *Cluster) GetSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// TODO We need to add bridge indexes to the snapshot
	return json.Marshal(c.TOML)
}

// ApplySnapshot implements Snapshotter
func (c *Cluster) ApplySnapshot(snapshot []byte) error {
	// TODO This has to setup the bridges
	var toml *TOML
	if err := json.Unmarshal(snapshot, toml); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TOML = toml
	return nil
}
