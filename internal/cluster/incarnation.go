package cluster

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
)

// Incarnation identifies one data directory: minted at random the first time
// a directory boots, and again by restore and recover, which make it a member
// of a new cluster. A node id belongs to one data directory for as long as the
// id is a member (a rebuilt node joins under a new id), so two incarnations
// for one node id are two processes running as that node — which can grant
// two votes in one term and elect two leaders. Peer requests carry it, and the
// cluster records the first incarnation it sees for each node id.
type Incarnation [16]byte

func (i Incarnation) String() string { return hex.EncodeToString(i[:]) }

// IsZero reports whether i is the zero incarnation (none known).
func (i Incarnation) IsZero() bool { return i == Incarnation{} }

// ParseIncarnation parses the form String produces.
func ParseIncarnation(s string) (Incarnation, error) {
	var i Incarnation
	h := strings.TrimSpace(s)
	if len(h) != 2*len(i) {
		return i, fmt.Errorf("incarnation %q is not %d hex digits", s, 2*len(i))
	}
	if _, err := hex.Decode(i[:], []byte(h)); err != nil {
		return i, fmt.Errorf("incarnation %q is not hex: %w", s, err)
	}
	if i.IsZero() {
		return i, fmt.Errorf("incarnation %q is zero", s)
	}
	return i, nil
}

// NewIncarnation mints a random incarnation.
func NewIncarnation() (Incarnation, error) {
	var i Incarnation
	if _, err := rand.Read(i[:]); err != nil {
		return i, fmt.Errorf("mint incarnation: %w", err)
	}
	return i, nil
}

// nodeIncarnationType is the built-in entity type that records a member's
// incarnation: each node proposes its own once every member understands it,
// and applying it records the first incarnation proposed for a node id, never
// a later one. Ungated in the compat namespace: it is only proposed once every
// member understands it, so an older node never meets it in a live cluster.
var nodeIncarnationType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 6),
	Name:       "InternalNodeIncarnation",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionConfig)

func IsNodeIncarnation(id string) bool {
	return id == nodeIncarnationType.ID
}

// NodeIncarnationKey is the key a node's incarnation is recorded under: the
// node id as 8 big-endian bytes.
func NodeIncarnationKey(nodeID uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, nodeID)
	return key
}

// NewNodeIncarnationEntity wraps a node's incarnation for proposal.
func NewNodeIncarnationEntity(nodeID uint64, inc Incarnation) (*Entity, error) {
	if inc.IsZero() {
		return nil, fmt.Errorf("refusing to propose a zero incarnation")
	}
	data := make([]byte, len(inc))
	copy(data, inc[:])
	return &Entity{Type: nodeIncarnationType, Key: NodeIncarnationKey(nodeID), Data: data}, nil
}

// NodeIncarnationFromEntity reads the node id and incarnation an entity
// carries.
func NodeIncarnationFromEntity(e *Entity) (uint64, Incarnation, error) {
	var inc Incarnation
	if len(e.Key) != 8 {
		return 0, inc, fmt.Errorf("node incarnation entity has a %d-byte key, want 8", len(e.Key))
	}
	if len(e.Data) != len(inc) {
		return 0, inc, fmt.Errorf("node incarnation entity carries %d bytes, want %d", len(e.Data), len(inc))
	}
	copy(inc[:], e.Data)
	if inc.IsZero() {
		return 0, inc, fmt.Errorf("node incarnation entity carries a zero incarnation")
	}
	return binary.BigEndian.Uint64(e.Key), inc, nil
}
