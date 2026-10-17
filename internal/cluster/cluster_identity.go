package cluster

import "fmt"

// clusterIdentityType is the built-in entity type that replicates the cluster
// id (see ClusterID). The first leader of a new cluster proposes the id every
// node derived at bootstrap; an upgraded cluster's leader mints one once
// every member runs a release that understands it. Applying it records the
// replicated copy (first value wins) and gives a member with no node-local id
// — a joiner — its id. Ungated in the compat namespace: it is only proposed
// once every member understands it, so an older node never meets it in a live
// cluster.
var clusterIdentityType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 5),
	Name:       "InternalClusterIdentity",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionConfig)

// clusterIdentityKey is the single row's key.
var clusterIdentityKey = []byte("cluster")

func IsClusterIdentity(id string) bool {
	return id == clusterIdentityType.ID
}

// NewClusterIdentityEntity wraps a cluster id for proposal.
func NewClusterIdentityEntity(id ClusterID) (*Entity, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("refusing to propose a zero cluster id")
	}
	data := make([]byte, len(id))
	copy(data, id[:])
	return &Entity{Type: clusterIdentityType, Key: clusterIdentityKey, Data: data}, nil
}

// ClusterIdentityFromEntity reads the id a cluster-identity entity carries.
func ClusterIdentityFromEntity(e *Entity) (ClusterID, error) {
	var id ClusterID
	if len(e.Data) != len(id) {
		return id, fmt.Errorf("cluster identity entity carries %d bytes, want %d", len(e.Data), len(id))
	}
	copy(id[:], e.Data)
	if id.IsZero() {
		return id, fmt.Errorf("cluster identity entity carries a zero id")
	}
	return id, nil
}
