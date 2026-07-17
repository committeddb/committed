package cluster

import (
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// nodeVersionType is the built-in entity type for a node's self-announced
// cluster feature level (version.FeatureLevel). Raft replicates node ids and
// raft peer URLs, never a node's software capabilities, so each node announces
// its own once it is a member; every node applies the record into the
// memberVersions bucket. That lets any node compute the cluster-agreed minimum
// feature level and gate emission of a feature's entries until every member can
// apply them — the fix for semantic version skew. Hidden from the default
// Proposals() listing like every other system type, and NOT syncable metadata:
// it carries nothing a syncable consumes. See node_version.go (db) and
// version-skew-semantic-rollback-compat.
var nodeVersionType = registerSystemType(&Type{
	ID:         "a4223c88-30c9-4b50-9948-4b6ed096e84b",
	Name:       "InternalNodeVersion",
	Version:    1,
	EntityKind: EntityKindSnapshot,
})

func IsNodeVersion(id string) bool {
	return id == nodeVersionType.ID
}

// NodeVersion is the payload of a node feature-level announcement: the
// announcing node's raft id and the highest cluster feature level its binary
// supports.
type NodeVersion struct {
	NodeID       uint64
	FeatureLevel uint64
}

func (n *NodeVersion) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogNodeVersion{NodeID: n.NodeID, FeatureLevel: n.FeatureLevel})
}

func (n *NodeVersion) Unmarshal(bs []byte) error {
	ln := &clusterpb.LogNodeVersion{}
	if err := proto.Unmarshal(bs, ln); err != nil {
		return err
	}
	n.NodeID = ln.NodeID
	n.FeatureLevel = ln.FeatureLevel
	return nil
}

// NodeVersionKey is the bbolt key a NodeVersion record is stored under: the
// node id as 8 big-endian bytes. Exported so the storage layer keys its bucket
// consistently with what NewNodeVersionEntity produces.
func NodeVersionKey(nodeID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, nodeID)
	return key
}

// NewNodeVersionEntity wraps a node's feature-level announcement as an upsert
// entity, proposed through the normal raft path. On commit every node's apply
// records nodeID → featureLevel in the memberVersions bucket (last-writer-wins,
// so a node whose binary was upgraded simply re-announces a higher level).
func NewNodeVersionEntity(nodeID, featureLevel uint64) (*Entity, error) {
	bs, err := (&NodeVersion{NodeID: nodeID, FeatureLevel: featureLevel}).Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(nodeVersionType, NodeVersionKey(nodeID), bs), nil
}
