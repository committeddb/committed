package cluster

import "encoding/json"

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
