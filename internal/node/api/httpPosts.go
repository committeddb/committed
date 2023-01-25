package api

import (
	"log"
	"net/http"

	"github.com/philborlin/committed/internal/node/cluster"
	"github.com/philborlin/committed/internal/node/types"
)

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal string
}

// NewClusterPostHandler creates a new handler for Cluster Topics
func NewClusterPostHandler(c *cluster.Cluster) http.Handler {
	return &clusterPostHandler{c}
}

type clusterPostHandler struct {
	c *cluster.Cluster
}

// ServeHTTP implements http.Handler
func (c *clusterPostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		// TODO We should set a content-type on the topic and handle the proposal based
		// on the content type. Default is json right now, but we can handle avro,
		// protobuf, etc. later on
		n := newClusterTopicPostRequest{}
		_ = unmarshall(r, &n)
		log.Printf("[api.httpPosts] received %v", n)
		go func() {
			c.c.Propose(types.Proposal{Topic: n.Topic, Proposal: []byte(n.Proposal)})
			log.Printf("[api.httpPosts] posted %v", n)
		}()
		w.Write(nil)
	}
}
