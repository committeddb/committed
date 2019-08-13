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
		toml, err := ReaderToString(r.Body)
		if err != nil {
			ErrorTo500(w, err)
			return
		}

		err = c.c.CreateSyncable(toml)
		if err != nil {
			ErrorTo500(w, err)
			return
		}

		w.Write(nil)
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterSyncableGetResponse{c.c.Data.Syncables})
		w.Write(response)
	}
}
