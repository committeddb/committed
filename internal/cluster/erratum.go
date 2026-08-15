package cluster

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// ErratumTypeID is the reserved system-type UUID for the errata
// interpretation-registry record (clusterpb.LogErratum) — the first GATED
// entry in the system-type namespace. Gated because errata are
// correctness-bearing: a node that skipped them would serve stale readings to
// its syncables. Emission is additionally FeatureLevel-gated
// (db.featureLevelErrata), so an erratum is only ever committed once every
// member can fold it.
var ErratumTypeID = reservedSystemID(compatGated, 0)

// erratumType registers the errata registry record. EntityKindStandalone on
// purpose: the registry is APPEND-ONLY — every erratum is part of the fold
// history (replay at an interpretation index before a correction must still
// see the corrected erratum), so no record is ever superseded-and-compactable
// the way Snapshot-kind metadata is.
var erratumType = registerSystemType(&Type{
	ID:         ErratumTypeID,
	Name:       "InternalErratum",
	Version:    1,
	EntityKind: EntityKindStandalone,
}, AdmissionConfig)

func IsErratum(id string) bool {
	return id == erratumType.ID
}

// Erratum is one append-only interpretation-registry statement: entities of
// TypeID committed in [FromIndex, ToIndex] (inclusive), stamped FromVersion
// (0 = any stamp), and matching Predicate (empty = all) READ AS
// RebindToVersion. It rebinds the reading, never the bytes. A wrong erratum
// is corrected by appending another — among matching errata, later in the log
// wins — and matching is always against the STAMPED version (the on-wire
// fact), never an intermediate rebound reading.
type Erratum struct {
	// ID is the operator's identity for the record (the entity Key). Errata
	// are immutable: re-POSTing an id with different content is refused.
	ID              string
	TypeID          string
	FromIndex       uint64
	ToIndex         uint64
	RebindToVersion int
	// FromVersion narrows to entities stamped exactly this version; 0 = any.
	FromVersion int
	// Predicate, when non-empty, narrows to payloads the jq program maps to
	// true — pinned to the same deterministic subset as migrations.
	Predicate string
}

func (e *Erratum) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogErratum{
		TypeID:          e.TypeID,
		FromIndex:       e.FromIndex,
		ToIndex:         e.ToIndex,
		RebindToVersion: uint32(e.RebindToVersion), //nolint:gosec // G115: versions are small positive ints, bounded by domain
		Predicate:       e.Predicate,
		FromVersion:     uint32(e.FromVersion), //nolint:gosec // G115: bounded by domain
	})
}

func (e *Erratum) Unmarshal(bs []byte) error {
	le := &clusterpb.LogErratum{}
	if err := proto.Unmarshal(bs, le); err != nil {
		return err
	}
	e.TypeID = le.TypeID
	e.FromIndex = le.FromIndex
	e.ToIndex = le.ToIndex
	e.RebindToVersion = int(le.RebindToVersion)
	e.FromVersion = int(le.FromVersion)
	e.Predicate = le.Predicate
	return nil
}

// NewErratumEntity wraps an erratum as its committed record, keyed by the
// operator's erratum id.
func NewErratumEntity(e *Erratum) (*Entity, error) {
	if e.ID == "" {
		return nil, fmt.Errorf("erratum requires an id")
	}
	bs, err := e.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(erratumType, []byte(e.ID), bs), nil
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

// AppliedErratum is one applied registry record with the raft index it
// committed at — its interpretation coordinate.
type AppliedErratum struct {
	Erratum Erratum
	Index   uint64
}

// Matches reports whether this erratum rebinds the entity at dataIndex with
// the given stamped version. The payload predicate (if any) is evaluated by
// the caller's compiled form — this covers the index range and stamp
// selectors only.
func (e *Erratum) Matches(dataIndex uint64, stampedVersion int) bool {
	if dataIndex < e.FromIndex || dataIndex > e.ToIndex {
		return false
	}
	return e.FromVersion == 0 || e.FromVersion == stampedVersion
}
