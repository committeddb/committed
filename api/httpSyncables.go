package api

import (
	"net/http"

	"github.com/philborlin/committed/cluster"
)

// NewClusterSyncableHandler creates a new handler for Cluster Sycnables
func NewClusterSyncableHandler(c *cluster.Cluster) http.Handler {
	return &clusterSyncableHandler{c}
}

type clusterSyncableHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterSyncableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		proposeToml(w, r, c.c.ProposeSyncable)
	} else if r.Method == "GET" {
		writeMultipartAndHandleError(c.c.TOML.Syncables, w)
	}
}
