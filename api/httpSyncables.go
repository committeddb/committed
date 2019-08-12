package api

import (
	"encoding/json"
	"net/http"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/syncable"
)

type clusterSyncableGetResponse struct {
	Syncables map[string]syncable.Syncable
}

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
		name, syncable, err := syncable.ParseSyncable("toml", r.Body, c.c.Data.Databases)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(500)
			return
		}

		c.c.CreateSyncable(name, syncable)
		w.Write(nil)
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterSyncableGetResponse{c.c.Data.Syncables})
		w.Write(response)
	}
}
