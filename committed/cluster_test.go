package committed

import (
	"testing"
)

// When create topic is called we want to startup a new cluster of raft nodes
func TestCreateTopic(t *testing.T) {
	topic := NewCluster().CreateTopic(3)

	defer topic.stop()

	if !topic.up() {
		t.Fatal("Topic not created")
	}
}

// func TestCreateTopicWithMultipleNodes(t *testing.T) {
// 	c := newCluster()
// 	topic := c.createTopic(3)

// 	defer topic.stop()

// 	up := true
// 	for i := 1; i <= len(topic.nodes); i++ {
// 		node := topic.nodes[i]
// 		if node.Status().ID <= 0 {
// 			up = false
// 		}
// 	}

// 	if !up {
// 		t.Fatal("Not all topics are up")
// 	}
// }

// When stop topic is called we want the underlying raft nodes to stop
func TestStopTopic(t *testing.T) {
	topic := NewCluster().CreateTopic(3)

	topic.stop()

	if topic.up() {
		t.Fatal("Topic did not stop correctly")
	}
}
