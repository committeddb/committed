package main

import (
	"bytes"
	"encoding/gob"
	"log"
)

// Cluster represents a cluster for the committeddb. It manages a raft cluster
// and n number of topics
type Cluster struct {
	nodes    []string
	proposeC chan<- string
}

// Proposal is an item to put on a raft log
type Proposal struct {
	Topic    string
	Proposal string
}

// NewCluster creates a new Cluster
func NewCluster(nodes []string, id int) *Cluster {
	return &Cluster{nodes: nodes}
}

// Append proposes an addition to the raft
func (c *Cluster) Append(proposal *Proposal) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(proposal); err != nil {
		log.Fatal(err)
	}
	c.proposeC <- buf.String()
}
