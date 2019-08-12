package api

import (
	"encoding/json"
	"net/http"

	"github.com/philborlin/committed/cluster"
)

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

type clusterTopicGetResponse struct {
	Topics []string
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
		n := newClusterTopicRequest{}
		Unmarshall(r, &n)
		err := c.c.CreateTopic(n.Name)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write(nil)
	} else if r.Method == "GET" {
		keys := make([]string, 0, len(c.c.Data.Topics))
		for key := range c.c.Data.Topics {
			keys = append(keys, key)
		}
		w.Header().Set("Content-Type", "application/json")
		response, _ := json.Marshal(clusterTopicGetResponse{keys})
		w.Write(response)
	}
}
