package committed

import (
	"testing"
)

// When create topic is called we want to startup a new cluster of raft nodes
func TestCreateTopic(t *testing.T) {
	c := newCluster()
	topic := c.createTopic()

	defer topic.stop()

	if !topic.up() {
		t.Fatal("Topic not created")
	}
}

// When stop topic is called we want the underlying raft nodes to stop
func TestStopTopic(t *testing.T) {
	c := newCluster()
	topic := c.createTopic()

	topic.stop()

	if topic.up() {
		t.Fatal("Topic did not stop correctly")
	}
}
