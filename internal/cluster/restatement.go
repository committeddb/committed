package cluster

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// RestatementTypeID is the reserved system-type UUID for the restatements
// interpretation-registry record (clusterpb.LogRestatement) — the first GATED
// entry in the system-type namespace. Gated because restatements are
// correctness-bearing: a node that skipped them would serve stale readings to
// its syncables. Emission is additionally FeatureLevel-gated
// (db.featureLevelRestatements), so a restatement is only ever committed once every
// member can fold it.
var RestatementTypeID = reservedSystemID(compatGated, 0)

// restatementType registers the restatement registry record. EntityKindStandalone on
// purpose: the registry is APPEND-ONLY — every restatement is part of the fold
// history (replay at an interpretation index before a correction must still
// see the corrected restatement), so no record is ever superseded-and-compactable
// the way Snapshot-kind metadata is.
var restatementType = registerSystemType(&Type{
	ID:         RestatementTypeID,
	Name:       "InternalRestatement",
	Version:    1,
	EntityKind: EntityKindStandalone,
}, AdmissionConfig)

func IsRestatement(id string) bool {
	return id == restatementType.ID
}

// Restatement is one append-only interpretation-registry statement: entities of
// TypeID committed in [FromIndex, ToIndex] (inclusive), stamped FromVersion
// (0 = any stamp), and matching Predicate (empty = all) READ AS
// ReadAsVersion. It rebinds the reading, never the bytes. A wrong restatement
// is corrected by appending another — among matching restatements, later in the log
// wins — and matching is always against the STAMPED version (the on-wire
// fact), never an intermediate rebound reading.
type Restatement struct {
	// ID is the operator's identity for the record (the entity Key). Restatements
	// are immutable: re-POSTing an id with different content is refused.
	ID            string
	TypeID        string
	FromIndex     uint64
	ToIndex       uint64
	ReadAsVersion int
	// FromVersion narrows to entities stamped exactly this version; 0 = any.
	FromVersion int
	// Predicate, when non-empty, narrows to payloads the jq program maps to
	// true — pinned to the same deterministic subset as migrations.
	Predicate string
}

func (e *Restatement) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogRestatement{
		TypeID:        e.TypeID,
		FromIndex:     e.FromIndex,
		ToIndex:       e.ToIndex,
		ReadAsVersion: uint32(e.ReadAsVersion), //nolint:gosec // G115: versions are small positive ints, bounded by domain
		Predicate:     e.Predicate,
		FromVersion:   uint32(e.FromVersion), //nolint:gosec // G115: bounded by domain
	})
}

func (e *Restatement) Unmarshal(bs []byte) error {
	le := &clusterpb.LogRestatement{}
	if err := proto.Unmarshal(bs, le); err != nil {
		return err
	}
	e.TypeID = le.TypeID
	e.FromIndex = le.FromIndex
	e.ToIndex = le.ToIndex
	e.ReadAsVersion = int(le.ReadAsVersion)
	e.FromVersion = int(le.FromVersion)
	e.Predicate = le.Predicate
	return nil
}

// NewRestatementEntity wraps a restatement as its committed record, keyed by the
// operator's restatement id.
func NewRestatementEntity(e *Restatement) (*Entity, error) {
	if e.ID == "" {
		return nil, fmt.Errorf("restatement requires an id")
	}
	bs, err := e.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(restatementType, []byte(e.ID), bs), nil
}

// ClusterBelowFeatureLevelError refuses admitting a record some current
// member cannot yet apply — retry once the rolling upgrade completes. The
// HTTP layer renders it 503 (retryable), never 4xx: the config is fine, the
// cluster isn't ready.
type ClusterBelowFeatureLevelError struct {
	Feature    string
	Required   uint64
	ClusterMin uint64
}

func (e *ClusterBelowFeatureLevelError) Error() string {
	return fmt.Sprintf("the cluster is not fully upgraded for %s: requires feature level %d, cluster minimum is %d — finish the rolling upgrade and retry", e.Feature, e.Required, e.ClusterMin)
}

// AppliedRestatement is one applied registry record with the raft index it
// committed at — its interpretation coordinate.
type AppliedRestatement struct {
	Restatement Restatement
	Index       uint64
}

// Matches reports whether this restatement rebinds the entity at dataIndex with
// the given stamped version. The payload predicate (if any) is evaluated by
// the caller's compiled form — this covers the index range and stamp
// selectors only.
func (e *Restatement) Matches(dataIndex uint64, stampedVersion int) bool {
	if dataIndex < e.FromIndex || dataIndex > e.ToIndex {
		return false
	}
	return e.FromVersion == 0 || e.FromVersion == stampedVersion
}
