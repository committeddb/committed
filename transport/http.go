package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	AddRaft(raftID types.ID, peerID types.ID, raft *Raft)
	// RemoveRaft removes a raft from the transport
	RemoveRaft(raftID types.ID)
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

type peerRaft struct {
	peerID types.ID
	raftID types.ID
}

// MultiTransport struct
type MultiTransport struct {
	mu    sync.RWMutex
	rafts map[peerRaft]Raft
	peers map[types.ID]string
	mux   *http.ServeMux
	serve bool
}

// NewMultiTransport creates a new MultiTransport
func NewMultiTransport(mux *http.ServeMux) *MultiTransport {
	t := &MultiTransport{
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
func (t *MultiTransport) Send(raftID types.ID, ms []raftpb.Message) {
	for _, m := range ms {
		peerID := types.ID(m.To)

		var peer string
		var raft Raft
		{
			t.mu.RLock()
			defer t.mu.RUnlock()
			raft = t.rafts[peerRaft{peerID: peerID, raftID: raftID}]
			peer = t.peers[peerID]
		}

		if peer == "" {
			fmt.Printf("Peer cannot be found. Message [%v] not delivered\n", m)
			return
		}

		if raft == nil {
			fmt.Printf("Raft cannot be found. Message [%v] not delivered\n", m)
			return
		}

		r, err := json.Marshal(m)
		if err == nil {
			fmt.Printf("Message %v errored out while marshaling. Message not delivered\n", m)
			return
		}

		url := peer + "/gossip/" + raftID.String()
		http.Post(url, "application/json", bytes.NewReader(r))
	}
}

// AddRaft implements MultiTransporter interface
func (t *MultiTransport) AddRaft(raftID types.ID, peerID types.ID, raft *Raft) {
	peerRaft := peerRaft{peerID: peerID, raftID: raftID}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rafts[peerRaft] = *raft
}

// RemoveRaft implements MultiTransporter interface
func (t *MultiTransport) RemoveRaft(raftID types.ID, peerID types.ID) {
	peerRaft := peerRaft{peerID: peerID, raftID: raftID}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rafts[peerRaft] = nil
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
		peerID := types.ID(m.To)
		raftID, _ := types.IDFromString(splits[len(splits)-1])
		peerRaft := peerRaft{peerID: peerID, raftID: raftID}

		t.mu.RLock()
		defer t.mu.RUnlock()
		r := t.rafts[peerRaft]
		r.Process(context.Background(), m)
	}
}
