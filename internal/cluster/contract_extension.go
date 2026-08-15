package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// The validation tripwire (ValidateAnnounce) commits divergent payloads and
// announces each distinct divergent shape once. Two records implement it:
//
//   - The ContractExtension EVENT: an ordinary user-visible entity proposed to
//     the announce-typed Type's SchemaChangeTopic, carrying the diff. It rides
//     the operator's topic type so ordinary syncables deliver it (the syncable
//     reader skips internal types — a system-typed event would never reach a
//     sink).
//   - The contractFingerprint DEDUPE MARK (internal, this file's system type):
//     replicated consensus state recording "this shape was announced", written
//     atomically in the same proposal as the event.

// contractFingerprintType is the tripwire's internal dedupe record. Ungated in
// the system-type namespace: an older node skips it, which at worst
// re-announces a shape later — consumers key on the fingerprint and converge
// (the documented at-least-once contract). Snapshot kind: re-writes of one
// (type, version, fingerprint) key are idempotent upserts the scrubber may
// compact.
var contractFingerprintType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 1),
	Name:       "InternalContractFingerprint",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionCoordination)

func IsContractFingerprint(id string) bool {
	return id == contractFingerprintType.ID
}

// ContractFingerprint marks one announced divergent shape for one type
// version. See clusterpb.LogContractFingerprint.
type ContractFingerprint struct {
	TypeID      string
	Version     int
	Fingerprint string
}

// Key is the record's replicated-state key: NUL-separated so an id containing
// ':' cannot collide with another record's composite.
func (f *ContractFingerprint) Key() []byte {
	return []byte(fmt.Sprintf("%s\x00%d\x00%s", f.TypeID, f.Version, f.Fingerprint))
}

func (f *ContractFingerprint) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogContractFingerprint{
		TypeID:      f.TypeID,
		Version:     uint32(f.Version), //nolint:gosec // G115: versions are small positive ints, bounded by domain
		Fingerprint: f.Fingerprint,
	})
}

func (f *ContractFingerprint) Unmarshal(bs []byte) error {
	lf := &clusterpb.LogContractFingerprint{}
	if err := proto.Unmarshal(bs, lf); err != nil {
		return err
	}
	f.TypeID = lf.TypeID
	f.Version = int(lf.Version)
	f.Fingerprint = lf.Fingerprint
	return nil
}

func NewContractFingerprintEntity(f *ContractFingerprint) (*Entity, error) {
	bs, err := f.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(contractFingerprintType, f.Key(), bs), nil
}

// SchemaDivergenceCause is one structured validation failure inside a
// divergent payload: where in the instance it occurred, which schema keyword
// tripped (e.g. "additionalProperties", "type", "required", "enum"), and the
// human-readable message.
type SchemaDivergenceCause struct {
	Path    string `json:"path"`
	Keyword string `json:"keyword,omitempty"`
	Message string `json:"message"`
}

// SchemaDivergence is a validator's structured report that a well-formed
// payload violates its Type's schema.
type SchemaDivergence struct {
	Causes []SchemaDivergenceCause
}

// EntitySchemaValidator validates one entity payload against its Type's
// schema, reporting divergence structurally rather than as a gate error. It
// returns (nil, nil) for a conformant payload, a non-validating type, or an
// unknown SchemaType (fail-open, matching TypeSchemaValidator); a non-nil
// divergence when the payload is well-formed but violates the schema; and an
// error only when the schema or input is structurally unusable. Injected into
// the db layer (DB.SetEntityValidator) for the same reason as
// TypeSchemaValidator: the schema compilers live in http, which db must not
// import.
type EntitySchemaValidator interface {
	ValidateEntityData(t *Type, data []byte) (*SchemaDivergence, error)
}

// ContractExtension is the event payload announcing that data claiming a type
// diverged from its schema — emitted once per distinct divergent shape, as
// plain JSON on the operator's SchemaChangeTopic. Consumers key on
// (TypeID, Version, Fingerprint) — the event entity's Key is exactly that
// composite — so at-least-once delivery converges in keyed sinks.
type ContractExtension struct {
	// TypeID/TypeName/Version identify the contract diverged from — the
	// version VALIDATED AGAINST (the stamp), not necessarily latest.
	TypeID   string `json:"typeID"`
	TypeName string `json:"typeName"`
	Version  int    `json:"version"`
	// Fingerprint is the divergent payload's shape signature; ObservedShape
	// is the signature's path list (types and paths only — no sample values,
	// the PII posture).
	Fingerprint   string   `json:"fingerprint"`
	ObservedShape []string `json:"observedShape"`
	// Violations are the structured validation failures.
	Violations []SchemaDivergenceCause `json:"violations"`
	// IngestableID and SourceSeq locate the first observed occurrence when it
	// arrived via CDC ingest; empty/0 for a direct proposal.
	IngestableID string `json:"ingestableID,omitempty"`
	SourceSeq    uint64 `json:"sourceSeq,omitempty"`
}

// Key is the event entity's key on the SchemaChangeTopic. ':'-joined for
// readability in sink key columns; the payload carries the fields separately
// for consumers that need to split reliably.
func (c *ContractExtension) Key() []byte {
	return []byte(fmt.Sprintf("%s:%d:%s", c.TypeID, c.Version, c.Fingerprint))
}

// JSONShapeSignature computes a payload's structural shape: every path with
// its JSON type ("$.caption:string", "$.tags[]:string", "$.meta.size:number"),
// sorted, plus a stable fingerprint of the whole (a truncated SHA-256). Two
// payloads share a fingerprint iff they have the same paths with the same
// types — values never enter the signature (types and paths only, the PII
// posture; also what makes the fingerprint a SHAPE dedupe key rather than a
// row dedupe key). Array elements contribute a union of element types under
// "path[]"; empty objects/arrays contribute their container type.
func JSONShapeSignature(data []byte) (shape []string, fingerprint string, err error) {
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, "", fmt.Errorf("shape signature: payload is not valid JSON: %w", err)
	}
	set := map[string]bool{}
	walkJSONShape("$", doc, set)
	shape = make([]string, 0, len(set))
	for s := range set {
		shape = append(shape, s)
	}
	sort.Strings(shape)
	sum := sha256.Sum256([]byte(strings.Join(shape, "\n")))
	return shape, hex.EncodeToString(sum[:16]), nil
}

func walkJSONShape(path string, v any, set map[string]bool) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 0 {
			set[path+":object"] = true
			return
		}
		for k, child := range t {
			walkJSONShape(path+"."+k, child, set)
		}
	case []any:
		if len(t) == 0 {
			set[path+":array"] = true
			return
		}
		for _, child := range t {
			walkJSONShape(path+"[]", child, set)
		}
	case string:
		set[path+":string"] = true
	case float64:
		set[path+":number"] = true
	case bool:
		set[path+":bool"] = true
	case nil:
		set[path+":null"] = true
	}
}
