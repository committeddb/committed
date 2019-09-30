package e2e

import (
	"fmt"
	"log"
)

func (c *Control) postDatabase(toml string) error {
	_, err := c.post("/cluster/databases", toml)
	return err
}

func (c *Control) getDatabases() ([]string, error) {
	return c.get("/cluster/databases")
}

func (c *Control) postPost(topic, data string) error {
	post := fmt.Sprintf("{\"Topic\":\"%s\",\"Proposal\":\"%s\"}", topic, data)
	log.Printf("Sending: %s", post)
	_, err := c.post("/cluster/posts", post)
	return err
}

func (c *Control) postSyncable(toml string) error {
	_, err := c.post("/cluster/syncables", toml)
	return err
}

func (c *Control) getSyncables() ([]string, error) {
	return c.get("/cluster/syncables")
}

func (c *Control) postTopic(toml string) error {
	_, err := c.post("/cluster/topics", toml)
	return err
}

func (c *Control) getTopics() ([]string, error) {
	return c.get("/cluster/topics")
}
