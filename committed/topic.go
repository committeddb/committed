package committed

import (
	"github.com/coreos/etcd/raft"
)

// Topic is a replicated state machine that accepts a partitioned type of data
type Topic struct {
	nodes []raft.Node
}

func (c *Topic) stop() {
	for i := 0; i < len(c.nodes); i++ {
		c.nodes[i].Stop()
	}
}

func (c *Topic) up() bool {
	v := false

	for i := 0; i < len(c.nodes); i++ {
		if c.nodes[i].Status().ID > 0 {
			v = true
		}
	}

	return v
}
