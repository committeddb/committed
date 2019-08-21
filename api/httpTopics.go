package api

import (
	"net/http"

	"github.com/philborlin/committed/cluster"
	"github.com/philborlin/committed/topic"
)

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

type clusterTopicGetResponse struct {
	Topics map[string]topic.Topic
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
		writeMultipartAndHandleError(nil, w)
		// w.Header().Set("Content-Type", "application/json")
		// response, _ := json.Marshal(clusterTopicGetResponse{c.c.Data.Topics})
		// w.Write(response)
	}
}
