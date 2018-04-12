// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/philborlin/committed/util"
)

func createMux(c *Cluster) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/cluster/topics", newClusterTopicHandler(c))
	mux.Handle("/cluster/posts", newClusterTopicPostHandler(c))
	return mux
}

// serveHTTPKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHTTPKVAPI(c *Cluster, kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: createMux(c),
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

type newClusterTopicRequest struct {
	Name      string
	NodeCount int
}

type clusterTopicGetResponse struct {
	Topics []string
}

func newClusterTopicHandler(c *Cluster) http.Handler {
	return &clusterTopicHandler{c}
}

type clusterTopicHandler struct {
	c *Cluster
}

func (c *clusterTopicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	log.Printf("Received: %s %s\n", r.Method, r.RequestURI)
	if r.Method == "POST" {
		n := newClusterTopicRequest{}
		util.Unmarshall(r, &n)
		// c.c.CreateTopic(n.Name, n.NodeCount)
		w.Write(nil)
	} else if r.Method == "GET" {
		log.Printf("Processing GET\n")
		// log.Printf("Found topics %v GET\n", c.c.topics)
		// keys := make([]string, 0, len(c.c.topics))
		// for key := range c.c.topics {
		// 	keys = append(keys, key)
		// }
		response, _ := json.Marshal(clusterTopicGetResponse{nil})
		w.Write(response)
	}
}

type newClusterTopicPostRequest struct {
	Topic    string
	Proposal string
}

func newClusterTopicPostHandler(c *Cluster) http.Handler {
	return &clusterTopicPostHandler{c}
}

type clusterTopicPostHandler struct {
	c *Cluster
}

func (c *clusterTopicPostHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method == "POST" {
		n := newClusterTopicPostRequest{}
		util.Unmarshall(r, &n)
		c.c.Append(&Proposal{n.Topic, n.Proposal})
		w.Write(nil)
	}
}
