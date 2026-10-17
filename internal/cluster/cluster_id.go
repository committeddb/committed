package cluster

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ClusterID identifies one committed cluster: identical on every member,
// different for every other cluster — including a cluster restored from
// another's backup. Peer requests carry it and a node refuses one from a
// different cluster, so two clusters can never exchange a raft message even
// when one dials an address the other now answers at. It is identity, not
// authentication: the peer token and mTLS remain the auth.
type ClusterID [16]byte

// String renders the id as a UUID (8-4-4-4-12 lower-case hex).
func (c ClusterID) String() string {
	h := hex.EncodeToString(c[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// IsZero reports whether c is the zero id (no id known).
func (c ClusterID) IsZero() bool { return c == ClusterID{} }

// ParseClusterID parses the UUID form String produces (hyphens optional).
func ParseClusterID(s string) (ClusterID, error) {
	var c ClusterID
	h := strings.ReplaceAll(strings.TrimSpace(s), "-", "")
	if len(h) != 32 {
		return c, fmt.Errorf("cluster id %q is not 32 hex digits", s)
	}
	if _, err := hex.Decode(c[:], []byte(h)); err != nil {
		return c, fmt.Errorf("cluster id %q is not hex: %w", s, err)
	}
	if c.IsZero() {
		return c, fmt.Errorf("cluster id %q is zero", s)
	}
	return c, nil
}

// NewClusterID mints a random id (an upgraded cluster's, or a restore's).
func NewClusterID() (ClusterID, error) {
	var c ClusterID
	if _, err := rand.Read(c[:]); err != nil {
		return c, fmt.Errorf("mint cluster id: %w", err)
	}
	return c, nil
}

// DeriveClusterID is a new cluster's id, computed identically by every node
// at bootstrap from what they share: the canonical peer set (id=url pairs,
// sorted by id) and the optional cluster name (COMMITTED_CLUSTER_NAME), which
// separates two clusters bootstrapped from identical configurations. Peer
// URLs must already be canonical (CanonicalPeerURL), so two nodes spelling an
// address differently still agree.
func DeriveClusterID(peers map[uint64]string, name string) ClusterID {
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	h := sha256.New()
	h.Write([]byte("committed cluster id v1\x00"))
	h.Write([]byte(name))
	for _, id := range ids {
		h.Write([]byte{0})
		h.Write([]byte(strconv.FormatUint(id, 10)))
		h.Write([]byte{'='})
		h.Write([]byte(peers[id]))
	}
	var c ClusterID
	copy(c[:], h.Sum(nil))
	return c
}
