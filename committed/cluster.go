package committed

import "github.com/coreos/etcd/raft"

// Cluster is an engine that spawns and maintains topics
type Cluster struct {
	topics []Topic
}

func newCluster() *Cluster {
	return &Cluster{}
}

func (c *Cluster) createTopic() *Topic {
	storage := raft.NewMemoryStorage()

	nodes := make([]raft.Node, 0)
	nodes = append(nodes, createNode(storage, 0x01))

	return &Topic{nodes: nodes}
}

func createNode(storage raft.Storage, id uint64) raft.Node {
	c := &raft.Config{
		ID:              id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}

	return raft.StartNode(c, nil)
}
