package cluster

import (
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// scrubType is the built-in entity type for a Scrub command — the committed
// instruction that triggers physical removal of already-delete-proposed
// entities from the permanent event log. Like every other system type it is
// hidden from the default Proposals() listing. It is NOT syncable metadata:
// the Reader never needs to surface or skip it for projection because a Scrub
// carries no entities a syncable would consume; it is acted on only by the
// storage tier's apply path (handleScrub).
var scrubType = registerSystemType(&Type{
	ID:         "45a0b2d1-99e7-4cf2-958c-a7c7e797d3ab",
	Name:       "InternalScrub",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionConfig)

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
//
// HashDeleteKeys additionally authorizes the delete-key erasure pass: retained
// user-delete entries whose consumption gate has opened (see the scrubber's
// deleteKeyEraseGate) get their raw subject key rewritten to ErasedKey.
// The proposer sets it only once the cluster minimum feature level supports
// the pass (version.FeatureLevel 4), so every replica computes the identical
// rewrite — carried in the committed command for the same reason UpperBound is.
type Scrub struct {
	UpperBound     uint64
	HashDeleteKeys bool
}

func (s *Scrub) Marshal() ([]byte, error) {
	return proto.Marshal(&clusterpb.LogScrub{UpperBound: s.UpperBound, HashDeleteKeys: s.HashDeleteKeys})
}

func (s *Scrub) Unmarshal(bs []byte) error {
	ls := &clusterpb.LogScrub{}
	if err := proto.Unmarshal(bs, ls); err != nil {
		return err
	}
	s.UpperBound = ls.UpperBound
	s.HashDeleteKeys = ls.HashDeleteKeys
	return nil
}

// NewScrubEntity wraps a Scrub command (carrying the freeze-line bound b) as an
// upsert entity. It is proposed through the normal raft path; on commit, every
// node's apply records a pending scrub and kicks its background scrubber.
// hashDeleteKeys authorizes the delete-key erasure pass for the rewrite; pass
// it true only when the cluster minimum feature level is at least
// version.FeatureLevel 4 (see Scrub.HashDeleteKeys).
func NewScrubEntity(b uint64, hashDeleteKeys bool) (*Entity, error) {
	bs, err := (&Scrub{UpperBound: b, HashDeleteKeys: hashDeleteKeys}).Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(scrubType, scrubKey, bs), nil
}

// ErasedKey is the sentinel an erased RTBF delete-tombstone's key is rewritten
// to once its consumption gate opens: every syncable that could have written
// the raw-keyed downstream row has consumed the delete (or was created against
// an already-scrubbed log and so never saw the row's upsert). At that point the
// raw subject key's only remaining copy is load-free — a replay's downstream
// `DELETE WHERE key = ErasedKey` is a harmless no-op because the scrubbed log
// no longer contains any upsert that could have created such a row — so the
// rewrite erases the last on-disk trace of the subject identifier while keeping
// the entry itself as PII-free evidence that an erasure happened at that index.
//
// A deliberate sentinel rather than a hash of the key: a hash (even keyed)
// remains a stable function of the subject identifier — a brute-forceable
// membership oracle for low-entropy keys and a linkage handle across topics —
// where the sentinel leaves nothing derived from the subject at all. The NUL
// framing makes an accidental collision with a real user key implausible;
// a deliberately colliding user key only ever attracts spurious no-op deletes.
var ErasedKey = []byte("\x00committed:rtbf-erased\x00")

// IsErasedKey reports whether key is the erased-delete sentinel.
func IsErasedKey(key []byte) bool {
	return string(key) == string(ErasedKey)
}

// ScrubStatus is one node's scrub progress. PendingBound is the highest
// committed Scrub command bound; CompletedBound is the highest bound THIS
// node's background rewrite has finished (erasure through that raft index is
// physically done on this node's log once they match).
// PendingDeleteKeyErasures counts retained delete tombstones whose raw
// subject key has not yet been erased — the operator's completion number:
// zero means no erased-subject identifier remains recorded as pending,
// whatever the erasure gate's current eligibility says (a count that is
// nonzero and not shrinking means a consumer is holding the gate — see the
// RTBF runbook).
type ScrubStatus struct {
	PendingBound             uint64
	CompletedBound           uint64
	PendingDeleteKeyErasures int
}

// FilterProposalEntities removes from a marshaled proposal every entity for
// which remove(typeID, key, isDelete) reports true. It is the entity-granular
// core of the scrubber: a proposal that bundled several entities keeps the ones
// the predicate spares.
//
// The caller owns all policy, including whether delete tombstones are retained.
// The RTBF (user-tombstone) pass passes a predicate that never removes a delete
// — the tombstone must survive so an in-flight syncable still receives the
// delete and a fresh syncable replaying a scrubbed log no-ops it — while the
// metadata-GC (system-tombstone) pass may remove a superseded internal delete.
// isDelete lets the predicate distinguish the two without re-deriving it.
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
func FilterProposalEntities(raw []byte, remove func(typeID string, key []byte, isDelete bool) bool) (newBytes []byte, allRemoved bool, changed bool, err error) {
	return ScrubProposalEntities(raw, remove, nil)
}

// ScrubProposalEntities is FilterProposalEntities plus the delete-key erasure
// pass: after the removal predicate spares a DELETE entity, rewriteDeleteKey
// (when non-nil) may replace its key — returning nil to leave the key alone, or
// the replacement bytes (the scrubber passes ErasedKey once a delete's
// consumption gate opens). Upserts and refresh markers are never key-rewritten.
// The same purity contract as FilterProposalEntities holds: unchanged records
// keep their original bytes verbatim (newBytes nil, changed false), and changed
// records re-marshal deterministically, so the result is a pure function of
// (input bytes, predicates) and byte-identical on every replica.
func ScrubProposalEntities(raw []byte, remove func(typeID string, key []byte, isDelete bool) bool, rewriteDeleteKey func(typeID string, key []byte) []byte) (newBytes []byte, allRemoved bool, changed bool, err error) {
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(raw, lp); err != nil {
		return nil, false, false, err
	}
	if len(lp.LogEntities) == 0 {
		return nil, false, false, nil
	}

	rewrote := false
	kept := make([]*clusterpb.LogEntity, 0, len(lp.LogEntities))
	for _, le := range lp.LogEntities {
		v, err := logEntityView(le)
		if err != nil {
			return nil, false, false, err
		}
		if remove(le.Type.GetID(), v.key, v.isDelete()) {
			continue
		}
		if rewriteDeleteKey != nil && v.isDelete() {
			if nk := rewriteDeleteKey(le.Type.GetID(), v.key); nk != nil {
				// A delete's key lives in its envelope body; logEntityView has
				// already rejected every other encoding a delete could ride in.
				le.GetBody().(*clusterpb.LogEntity_Delete).Delete.Key = nk
				rewrote = true
			}
		}
		kept = append(kept, le)
	}

	switch {
	case len(kept) == 0:
		// Every entity removed — drop the whole record.
		return nil, true, true, nil
	case len(kept) == len(lp.LogEntities) && !rewrote:
		// Nothing matched — keep the record verbatim.
		return nil, false, false, nil
	default:
		lp.LogEntities = kept
		out, err := proto.MarshalOptions{Deterministic: true}.Marshal(lp)
		if err != nil {
			return nil, false, false, err
		}
		return out, false, true, nil
	}
}

// ForEachProposalEntity decodes a marshaled proposal and calls fn once per
// entity with its (typeID, key, data, isDelete) — enough to drive scrub/GC
// selection without hydrating the entity through a resolver. data is the raw
// entity payload (the delete sentinel when isDelete is true); the scrubber uses
// it to read a type registration's declared kind. It stops and returns the
// first error from fn or from decoding. The key and data slices alias the
// decoded proposal's memory; copy them if retained beyond the callback. Like
// FilterProposalEntities it works at the clusterpb level, so the traversal is a
// pure function of the input bytes.
func ForEachProposalEntity(raw []byte, fn func(typeID string, key, data []byte, isDelete bool) error) error {
	lp := &clusterpb.LogProposal{}
	if err := proto.Unmarshal(raw, lp); err != nil {
		return err
	}
	for _, le := range lp.LogEntities {
		v, err := logEntityView(le)
		if err != nil {
			return err
		}
		if err := fn(le.Type.GetID(), v.key, v.data, v.isDelete()); err != nil {
			return err
		}
	}
	return nil
}
