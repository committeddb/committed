package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
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
	AddRaft(topic string, raft *Raft)
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
	rafts map[string]Raft
	peers map[types.ID]string
	mux   *http.ServeMux
	serve bool
}

// NewMultiTransport creates a new MultiTransport
func NewMultiTransport(mux *http.ServeMux) *MultiTransport {
	t := &MultiTransport{
		rafts: make(map[string]Raft),
		peers: make(map[types.ID]string),
		mux:   mux,
		serve: false,
	}

	mux.Handle("/gossip", &multiTransportHandler{t})
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

		log.Printf("[%v] Sending: %s\n", peerID, m.Type.String())

		var peer string
		var raft Raft
		{
			t.mu.RLock()
			defer t.mu.RUnlock()
			raft = t.rafts[topic]
			peer = t.peers[peerID]
		}

		if peer == "" {
			fmt.Printf("Peer cannot be found. Message [%v] not delivered\n", m.Type.String())
			return
		}

		if raft == nil {
			fmt.Printf("Raft cannot be found. Message [%v] not delivered\n", m.Type.String())
			return
		}

		r, err := json.Marshal(m)
		if err != nil {
			fmt.Printf("Message %v errored out while marshaling. Message not delivered\nreason: %v\n", m.Type.String(), err)
			return
		}

		url := peer + "/gossip/" + topic
		http.Post(url, "application/json", bytes.NewReader(r))
	}
}

// AddRaft implements MultiTransporter interface
func (t *MultiTransport) AddRaft(topic string, raft Raft) {
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
		m := raftpb.Message{}
		util.Unmarshall(r, &m)
		splits := strings.Split(r.RequestURI, "/")
		topic := splits[len(splits)-1]

		t.mu.RLock()
		defer t.mu.RUnlock()
		r := t.rafts[topic]
		r.Process(context.Background(), m)
	}
}
