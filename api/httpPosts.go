package api

import (
	"net/http"

	"github.com/philborlin/committed/db"
	"github.com/philborlin/committed/types"
)

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal []byte
}

// NewClusterPostHandler creates a new handler for Cluster Topics
func NewClusterPostHandler(c *db.Cluster) http.Handler {
	return &clusterPostHandler{c}
}

type clusterPostHandler struct {
	c *db.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterPostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		n := newClusterTopicPostRequest{}
		Unmarshall(r, &n)
		c.c.Propose(types.Proposal{Topic: n.Topic, Proposal: n.Proposal})
		w.Write(nil)
	}
}
