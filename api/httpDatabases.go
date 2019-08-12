package api

import (
	"encoding/json"
	"net/http"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/syncable"
	"github.com/philborlin/committed/types"
)

type clusterDatabasesGetResponse struct {
	Databases map[string]types.Database
}

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
		name, database, err := syncable.ParseDatabase("toml", r.Body)
		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(500)
			return
		}

		c.c.CreateDatabase(name, database)
		w.Write(nil)
	} else if r.Method == "GET" {
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterDatabasesGetResponse{c.c.Data.Databases})
		w.Write(response)
	}
}
