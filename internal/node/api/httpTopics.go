package api

import (
	"net/http"

	"github.com/philborlin/committed/internal/node/cluster"
)

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

// NewClusterTopicHandler is the handler for Cluster Topics
func NewClusterTopicHandler(c *cluster.Cluster) http.Handler {
	return &clusterTopicHandler{c}
}

type clusterTopicHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterTopicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		proposeToml(w, r, c.c.ProposeTopic)
	} else if r.Method == "GET" {
		writeMultipartAndHandleError(c.c.TOML.Topics, w)
	}
}
