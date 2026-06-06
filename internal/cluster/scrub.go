package cluster

import (
	"bytes"

	"google.golang.org/protobuf/proto"

	"github.com/philborlin/committed/internal/cluster/clusterpb"
)

// scrubType is the built-in entity type for a Scrub command — the committed
// instruction that triggers physical removal of already-delete-proposed
// entities from the permanent event log. Like every other system type it is
// hidden from the default Proposals() listing. It is NOT syncable metadata:
// the Reader never needs to surface or skip it for projection because a Scrub
// carries no entities a syncable would consume; it is acted on only by the
// storage tier's apply path (handleScrub).
var scrubType = registerSystemType(&Type{
	ID:      "45a0b2d1-99e7-4cf2-958c-a7c7e797d3ab",
	Name:    "InternalScrub",
	Version: 1,
}, hiddenFromProposals)

// scrubKey is the fixed entity key for a Scrub command. A Scrub is a command,
// not a keyed resource — the apply path dispatches on the type id alone — so
// the key is a constant rather than meaningful state.
var scrubKey = []byte("scrub")

func IsScrub(id string) bool {
	return id == scrubType.ID
}

// Scrub is the payload of a Scrub command: a single upper-bound raft index B
// (the "freeze line"). The scrubber physically removes already-delete-proposed
// entities only from event-log entries at raft index <= UpperBound, and only
// for entities whose (type, key) was delete-proposed at a raft index <=
// UpperBound. Pinning B inside the committed command makes every replica remove
// the identical set, keeping the rewritten event logs byte-identical across
// nodes. See docs/event-log-architecture.md § "Right-to-be-forgotten / deletes".
type Scrub struct {
	UpperBound uint64
}

func (s *Scrub) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogScrub{UpperBound: s.UpperBound})
}

func (s *Scrub) Unmarshal(bs []byte) error {
	ls := &clusterpb.LogScrub{}
	if err := proto.Unmarshal(bs, ls); err != nil {
		return err
	}
	s.UpperBound = ls.UpperBound
	return nil
}

// NewScrubEntity wraps a Scrub command (carrying the freeze-line bound b) as an
// upsert entity. It is proposed through the normal raft path; on commit, every
// node's apply records a pending scrub and kicks its background scrubber.
func NewScrubEntity(b uint64) (*Entity, error) {
	bs, err := (&Scrub{UpperBound: b}).Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(scrubType, scrubKey, bs), nil
}

// FilterProposalEntities removes from a marshaled proposal every entity for
// which remove(typeID, key) reports true — EXCEPT delete entities, which are
// always retained (the delete-tombstone must survive so an in-flight syncable
// still receives the delete and a fresh syncable replaying a scrubbed log
// no-ops it). It is the entity-granular core of the scrubber: a proposal that
// bundled several entities keeps its untombstoned siblings.
//
// Returns:
//   - newBytes: the re-marshaled proposal when some (but not all) entities were
//     removed; nil when the record should be dropped or kept verbatim.
//   - allRemoved: true when every entity was removed (caller drops the whole
//     event-log record).
//   - changed: true when the entity set changed (some or all removed).
//
// When changed is false the caller MUST keep the original on-disk bytes
// verbatim — newBytes is nil — so an untouched record stays byte-identical to
// what raft replicated. Re-marshaling uses deterministic protobuf encoding so
// every replica produces identical bytes for a changed record. Working at the
// clusterpb level (not the resolver-hydrated cluster.Proposal) keeps the result
// a pure function of (input bytes, predicate) with no resolver dependency.
func FilterProposalEntities(raw []byte, remove func(typeID string, key []byte) bool) (newBytes []byte, allRemoved bool, changed bool, err error) {
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(raw, lp); err != nil {
		return nil, false, false, err
	}
	if len(lp.LogEntities) == 0 {
		return nil, false, false, nil
	}

	kept := make([]*clusterpb.LogEntity, 0, len(lp.LogEntities))
	for _, le := range lp.LogEntities {
		// A delete entity is the tombstone itself, never PII — always retain it.
		isDelete := bytes.Equal(le.Data, delete)
		if !isDelete && remove(le.Type.GetID(), le.Key) {
			continue
		}
		kept = append(kept, le)
	}

	switch len(kept) {
	case len(lp.LogEntities):
		// Nothing matched — keep the record verbatim.
		return nil, false, false, nil
	case 0:
		// Every entity removed — drop the whole record.
		return nil, true, true, nil
	default:
		lp.LogEntities = kept
		out, err := proto.MarshalOptions{Deterministic: true}.Marshal(lp)
		if err != nil {
			return nil, false, false, err
		}
		return out, false, true, nil
	}
}
