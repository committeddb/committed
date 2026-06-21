package cluster

import (
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// nodeAPIURLType is the built-in entity type for a node's self-announced
// advertised HTTP API base URL. Raft replicates only node ids and raft peer
// URLs, never API addresses, so each node announces its own once it is a
// member; every node applies the record into the memberAPIURLs bucket so any
// node can resolve another's API address — in particular a follower resolving
// the leader's, to proxy a leader-only read (GET /v1/membership). Hidden from
// the default Proposals() listing like every other system type, and NOT
// syncable metadata: it carries nothing a syncable consumes. See
// raft-leader-read-proxy.md.
var nodeAPIURLType = registerSystemType(&Type{
	ID:         "65499eaa-5910-4798-8cc5-0c2d996658e3",
	Name:       "InternalNodeAPIURL",
	Version:    1,
	EntityKind: EntityKindSnapshot,
})

func IsNodeAPIURL(id string) bool {
	return id == nodeAPIURLType.ID
}

// NodeAPIURL is the payload of a node API-URL announcement: the announcing
// node's raft id and its advertised API base URL (e.g. http://n1:8080).
type NodeAPIURL struct {
	NodeID uint64
	APIURL string
}

func (n *NodeAPIURL) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogNodeAPIURL{NodeID: n.NodeID, ApiURL: n.APIURL})
}

func (n *NodeAPIURL) Unmarshal(bs []byte) error {
	ln := &clusterpb.LogNodeAPIURL{}
	if err := proto.Unmarshal(bs, ln); err != nil {
		return err
	}
	n.NodeID = ln.NodeID
	n.APIURL = ln.ApiURL
	return nil
}

// NodeAPIURLKey is the bbolt key a NodeAPIURL record is stored under: the
// node id as 8 big-endian bytes. Exported so the storage layer keys its
// bucket consistently with what NewNodeAPIURLEntity produces.
func NodeAPIURLKey(nodeID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, nodeID)
	return key
}

// NewNodeAPIURLEntity wraps a node's API-URL announcement as an upsert entity,
// proposed through the normal raft path. On commit every node's apply records
// nodeID → apiURL in the memberAPIURLs bucket (last-writer-wins, so a node
// whose advertised URL changed simply re-announces).
func NewNodeAPIURLEntity(nodeID uint64, apiURL string) (*Entity, error) {
	bs, err := (&NodeAPIURL{NodeID: nodeID, APIURL: apiURL}).Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(nodeAPIURLType, NodeAPIURLKey(nodeID), bs), nil
}
