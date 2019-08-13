package api

import (
	"net/http"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/types"
)

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal []byte
}

// NewClusterPostHandler creates a new handler for Cluster Topics
func NewClusterPostHandler(c *cluster.Cluster) http.Handler {
	return &clusterPostHandler{c}
}

type clusterPostHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterPostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		n := newClusterTopicPostRequest{}
		_ = unmarshall(r, &n)
		c.c.Propose(types.Proposal{Topic: n.Topic, Proposal: n.Proposal})
		w.Write(nil)
	}
}
