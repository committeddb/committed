package cluster

import (
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// nodeZoneType is the built-in entity type for a node's self-announced zone
// (COMMITTED_ZONE) — the placement identity zone-pinned syncables resolve
// their owner against. Ungated in the compat namespace: an old node skips
// the announcement safely, because ownership resolution is ALSO feature-
// gated (featureLevelZonePinning) — until every member understands zones,
// every node resolves leader-owns, so a skipped announcement can never
// split ownership. Snapshot kind, upsert keyed by node id: a moved node
// re-announces (last writer wins), an empty zone clears the identity.
var nodeZoneType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 4),
	Name:       "InternalNodeZone",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionConfig)

func IsNodeZone(id string) bool {
	return id == nodeZoneType.ID
}

// NodeZone is the payload of a zone announcement: the announcing node's raft
// id and its configured zone ("" = unpinned/cleared).
type NodeZone struct {
	NodeID uint64
	Zone   string
}

func (n *NodeZone) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogNodeZone{NodeID: n.NodeID, Zone: n.Zone})
}

func (n *NodeZone) Unmarshal(bs []byte) error {
	ln := &clusterpb.LogNodeZone{}
	if err := proto.Unmarshal(bs, ln); err != nil {
		return err
	}
	n.NodeID = ln.NodeID
	n.Zone = ln.Zone
	return nil
}

// NewNodeZoneEntity wraps a node's zone announcement as an upsert entity,
// keyed like NodeVersion (the node id as 8 big-endian bytes — see
// NodeVersionKey) and proposed through the normal raft path.
func NewNodeZoneEntity(nodeID uint64, zone string) (*Entity, error) {
	bs, err := (&NodeZone{NodeID: nodeID, Zone: zone}).Marshal()
	if err != nil {
		return nil, err
	}
	return &Entity{Type: nodeZoneType, Key: NodeVersionKey(nodeID), Data: bs}, nil
}
