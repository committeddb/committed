package api

import (
	"net/http"

	"github.com/philborlin/committed/db"
	"github.com/philborlin/committed/util"
)

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal string
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
		util.Unmarshall(r, &n)
		c.c.Append(util.Proposal{n.Topic, n.Proposal})
		w.Write(nil)
	}
}
