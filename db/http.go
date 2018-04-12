package db

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/philborlin/committed/util"

	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft/raftpb"
)

// Raft represents a raft node
type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error
}

// MultiTransporter is able to provide transport for multiple raft clusters
type MultiTransporter interface {
	// Start starts the given Transporter.
	// Start MUST be called before calling other functions in the interface.
	Start() error
	// Send sends out the given messages to the remote peers of a particular raft cluster.
	// Each message has a To field, which is an id that maps
	// to an existing peer in the transport.
	// If the id cannot be found in the transport, the message
	// will be ignored.
	Send(raftID types.ID, m []raftpb.Message)
	// AddRaft adds a raft to the transport
	AddRaft(topic string, raft raftNode)
	// RemoveRaft removes a raft from the transport
	RemoveRaft(topic string)
	// AddPeer adds a peer with given peer urls into the transport.
	// It is the caller's responsibility to ensure the urls are all valid,
	// or it panics.
	// Peer urls are used to connect to the remote peer.
	AddPeer(peerID types.ID, urls []string)
	// RemovePeer removes the peer with given id.
	RemovePeer(peerID types.ID)
	// Stop closes the connections and stops the transporter.
	Stop()
}

// MultiTransport struct
type MultiTransport struct {
	mu    sync.RWMutex
	rafts map[string]*raftNode
	peers map[types.ID]string
	mux   *http.ServeMux
	serve bool
}

// NewMultiTransport creates a new MultiTransport
func NewMultiTransport(mux *http.ServeMux) *MultiTransport {
	t := &MultiTransport{
		rafts: make(map[string]*raftNode),
		peers: make(map[types.ID]string),
		mux:   mux,
		serve: false,
	}

	mux.Handle("/gossip/", &multiTransportHandler{t})
	return t
}

// Start implements MultiTransporter interface
func (t *MultiTransport) Start() error {
	t.serve = true
	return nil
}

// Send implements MultiTransporter interface
func (t *MultiTransport) Send(topic string, ms []raftpb.Message) {
	for _, m := range ms {
		peerID := types.ID(m.To)

		if util.LogMessage(m) {
			log.Printf("[%v@%d] Sending %s to %d\n", m.From, m.Term, m.Type.String(), m.To)
		}

		var peer string
		{
			t.mu.RLock()
			defer t.mu.RUnlock()
			peer = t.peers[peerID]
		}

		if peer == "" {
			fmt.Printf("Peer cannot be found. Message [%v] not delivered\n", m.Type.String())
			return
		}

		// url := peer + "/gossip/?topic=" + topic
		url := fmt.Sprintf("%s/gossip/?topic=%s", peer, topic)
		// url := fmt.Sprintf("%s/gossip", peer)
		util.PostJSONAndClose(url, m)
	}
}

// AddRaft implements MultiTransporter interface
func (t *MultiTransport) AddRaft(topic string, raft *raftNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rafts[topic] = raft
}

// RemoveRaft implements MultiTransporter interface
func (t *MultiTransport) RemoveRaft(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rafts[topic] = nil
}

// AddPeer implements MultiTransporter interface
func (t *MultiTransport) AddPeer(id types.ID, url string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = url
}

// RemovePeer implements MultiTransporter interface
func (t *MultiTransport) RemovePeer(id types.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = ""
}

// Stop implements MultiTransporter interface
func (t *MultiTransport) Stop() {
	t.serve = false
}

type multiTransportHandler struct {
	t *MultiTransport
}

func (c *multiTransportHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := c.t

	if t.serve {
		go func(r *http.Request, t *MultiTransport) {
			m := raftpb.Message{}
			util.Unmarshall(r, &m)
			topic := r.URL.Query().Get("topic")

			if util.LogMessage(m) {
				log.Printf("[%s@%d] /gossip Received message %s", topic, m.Term, m.Type.String())
			}

			// t.mu.RLock()
			// defer t.mu.RUnlock()
			raft := t.rafts[topic]
			raft.Process(context.Background(), m)
		}(r, t)
	}

	w.WriteHeader(http.StatusOK)
}
