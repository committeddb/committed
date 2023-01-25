package api

import (
	"net/http"

	"github.com/philborlin/committed/internal/node/cluster"
)

// NewClusterDatabaseHandler is the handler for Cluster Databases
func NewClusterDatabaseHandler(c *cluster.Cluster) http.Handler {
	return &clusterDatabaseHandler{c}
}

type clusterDatabaseHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterDatabaseHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		proposeToml(w, r, c.c.ProposeDatabase)
	} else if r.Method == "GET" {
		writeMultipartAndHandleError(c.c.TOML.Databases, w)
	}
}
