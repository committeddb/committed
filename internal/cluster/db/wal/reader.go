package wal

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	pb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// Reader streams committed proposals out of the permanent event log.
// Syncable workers construct one via Storage.Reader(id) and read until
// io.EOF; r.raftIndex tracks the last raft index actually returned, so
// the next Read() resumes at the next higher raft index in the log.
//
// Before Phase 2 this reader scanned the raft entry log, which worked
// only because the raft log was never compacted. Now it reads from
// EventLog — the permanent tier — which is the right shape for the CQRS
// bootstrap path ("new syncable reads from the start") and for operation
// after the raft log gets compacted.
//
// The backend cursor resolves stable Raft indexes into its physical positions.
// Application progress advances only after a successful decode or deliberate skip.
type Reader struct {
	sync.Mutex
	raftIndex uint64 // last raft index returned to caller
	// pos mirrors the raft index of the last entry this reader EXAMINED —
	// including internal/foreign-topic entries it skipped — as an atomic so a
	// concurrent status query can observe scan progress without contending
	// the read loop. A replay scanning millions of other-topic entries is ONE
	// long Read call from the caller's perspective; only an in-loop cursor
	// makes "scanning" distinguishable from "frozen" (the field finding that
	// motivated it: a young mirror at checkpoint 0 is otherwise unreadable).
	// Single writer (the owning worker's Read), rare readers, its own cache
	// line — none of the shared-line contention the reader-scaling work
	// removed. 0 means nothing examined yet (also true for a reader that
	// resolved at head and went straight to EOF — pair with caughtUp).
	pos    atomic.Uint64
	cursor productionEventCursor
	closed bool
	s      *Storage
}

func (r *Reader) Read() (*cluster.Actual, error) {
	r.Lock()
	defer r.Unlock()
	if r.closed {
		return nil, eventlog.ErrClosed
	}

	// Keep one generation through storage reads and proposal/type decoding.
	r.s.eventMu.RLock()
	defer r.s.eventMu.RUnlock()
	cursor := r.eventCursorLocked()

	for {
		if r.raftIndex == ^uint64(0) {
			return nil, io.EOF
		}
		ent, err := cursor.Current()
		if errors.Is(err, eventlog.ErrNotFound) {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		// Visibility watermark: never surface an entry whose apply has not
		// completed. appendEvents publishes a whole Ready's entries to the
		// event log BEFORE their entities are applied to bbolt (the tolerated
		// p>r window, and restart replay), so an entry whose raft index
		// exceeds AppliedIndex may reference a type not yet written — resolving
		// it would fail, and pre-watermark the cursor had already advanced, so
		// the row was skipped forever (reader-sees-unapplied). Treat
		// not-yet-applied as EOF WITHOUT advancing; a later Read surfaces it
		// once AppliedIndex catches up (sub-millisecond in steady state, and
		// as replay progresses on a restart). AppliedIndex is an atomic load,
		// safe under the eventMu.RLock held here.
		if ent.GetIndex() > r.s.AppliedIndex() {
			return nil, io.EOF
		}

		if ent.GetType() != pb.EntryNormal || ent.Data == nil {
			r.raftIndex = ent.GetIndex()
			r.pos.Store(ent.GetIndex())
			cursor.Advance()
			continue
		}

		p := &cluster.Proposal{}
		if err := p.Unmarshal(ent.Data, r.s); err != nil {
			// A namespaced system type from a NEWER version, marked skippable
			// (ungated): the apply path skipped it too (compat namespace), so
			// skip it here — advance the cursor and keep scanning — rather than
			// stalling the syncable on a coordination record it never wanted.
			var ure *cluster.UnknownReservedTypeError
			if errors.As(err, &ure) && ure.Skippable() {
				r.raftIndex = ent.GetIndex()
				r.pos.Store(ent.GetIndex())
				cursor.Advance()
				continue
			}
			// Otherwise: do not advance (as above). With the watermark, a
			// within-AppliedIndex entry's type is guaranteed applied, so a
			// resolution failure here is genuine corruption/bug — retry-loudly
			// beats skip-silently.
			return nil, err
		}

		r.raftIndex = ent.GetIndex()
		r.pos.Store(ent.GetIndex())
		cursor.Advance()

		// Internal metadata entities — committed's own config (type / database /
		// syncable / ingestable) and coordination (syncable index +
		// dead-letters + stuck/skip, ingestable position, scrub, etc.) —
		// are not topic data and must NOT be projected into a syncable: a
		// syncable would otherwise re-Sync its own dead letters, and
		// committed's control plane would leak out of band into every
		// downstream sink. Skip them per-entity so a syncable sees only
		// user-defined topic data (ingested data included — it rides under user
		// topic types). This is the "skipping internal metadata entries" the
		// read path documents; filtering per-entity (not by Entities[0]) makes
		// that literally true regardless of proposal composition. Keep scanning.
		if userEntities := userTopicEntities(p.Entities); len(userEntities) > 0 {
			return &cluster.Actual{Index: ent.GetIndex(), Entities: userEntities}, nil
		}
	}
}

// userTopicEntities returns the user-topic entities of a committed proposal,
// dropping committed's internal config/coordination entities (syncable-index
// bumps, ingestable positions, dead-letters, scrub tombstones, …). It is the
// per-entity form of the read path's promise to skip internal metadata entries,
// so a syncable is handed only user topic data.
//
// It returns the input slice unchanged when every entity is a user entity — the
// overwhelmingly common case, since every production proposer emits a homogeneous
// proposal, so the hot path allocates nothing — and nil when every entity is
// internal (the whole proposal is skipped). Only a mixed proposal, which no
// current path emits, allocates a filtered slice; deciding per-entity means such
// a proposal can neither drop a trailing user entity nor leak a trailing internal
// one, rather than being classified wholesale by Entities[0].
func userTopicEntities(entities []*cluster.Entity) []*cluster.Entity {
	allUser, anyUser := true, false
	for _, e := range entities {
		if cluster.IsInternal(e.Type.ID) {
			allUser = false
		} else {
			anyUser = true
		}
	}
	if allUser {
		return entities // homogeneous user proposal (or empty) — no allocation
	}
	if !anyUser {
		return nil // homogeneous internal proposal — skip it whole
	}
	filtered := make([]*cluster.Entity, 0, len(entities))
	for _, e := range entities {
		if !cluster.IsInternal(e.Type.ID) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// ErrActualNotFound is returned by ActualAt when no committed Actual exists
// at the requested raft index (it was never committed, has been scrubbed, or
// the entry there carries no proposal data).
var ErrActualNotFound = errors.New("wal: no committed entry at raft index")

// ActualAt returns the committed Actual at raft index, read straight from the
// permanent event log. It binary-searches the log by raft index (the event log
// is strictly ascending in raft index — sparse after a scrub, but still sorted
// — so seq order == index order), so it is O(log n) and does not disturb any
// syncable's read cursor. Used by replay to re-drive a single dead-lettered
// Actual. Returns ErrActualNotFound if the index isn't present (never committed
// or scrubbed) or carries no proposal.
//
// Holds eventMu.RLock for the whole search so a concurrent scrub swap can't
// re-densify the seqs mid-search or invalidate the returned entry during decoding.
func (s *Storage) ActualAt(index uint64) (*cluster.Actual, error) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()

	cursor := newLegacyEntryCursor(s, index)
	defer func() { _ = cursor.Close() }()
	entry, err := exactEntry(cursor, index)
	if err != nil {
		return nil, err
	}
	return actualFromEntry(entry, s)
}

func (s *Storage) Reader(id string) db.ActualReader {
	i, err := s.getSyncableIndex(id)
	switch {
	case err == nil:
		if id == "" {
			i = 0
		}
	case errors.Is(err, ErrBucketMissing):
		// No syncable has ever checkpointed (fresh storage) — a legitimate
		// start-from-head, not an error, so no log.
		i = 0
	default:
		// A persisted checkpoint exists but did not decode (corrupt bytes). We
		// cannot know how far this syncable actually got, so we restart from the
		// head of the log (index 0) to avoid MISSING data — but that re-syncs the
		// entire history, which for a non-idempotent sink (webhook /
		// event-append) means duplicate downstream deliveries. Never silent: log
		// loudly so an operator can see the full re-sync and watch for
		// duplicates, instead of an unexplained re-sync storm.
		zap.L().Error("syncable checkpoint failed to decode (corrupt); restarting this syncable from the head of the log — a full re-sync, non-idempotent destinations may see duplicates",
			zap.String("syncable", id),
			zap.Error(err),
		)
		i = 0
	}

	return &Reader{raftIndex: i, s: s}
}

// ReaderAt reads from an arbitrary raft index — the dry-run's window
// sampler. No checkpoint is consulted or advanced.
func (s *Storage) ReaderAt(index uint64) db.ActualReader {
	return &Reader{raftIndex: index, s: s}
}

// Position reports the raft index of the last entry this reader examined —
// skipped entries included — safe to call from any goroutine while Read runs.
// See the pos field doc for semantics (0 = nothing examined yet).
func (r *Reader) Position() uint64 {
	return r.pos.Load()
}

// Close releases the backend cursor. It waits for an in-flight Read to finish.
func (r *Reader) Close() error {
	r.Lock()
	defer r.Unlock()
	r.closed = true
	if r.cursor.entryCursor != nil {
		return r.cursor.Close()
	}
	return nil
}
