package wal

import (
	"encoding/binary"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// typeOnSupersedeBucket holds one nested sub-bucket per type id, mapping the
// big-endian raft index of each of the type's registrations to whether that
// registration declared onSupersede = "scrub" (1) or not (0). Written on the
// deterministic apply path (saveType), so every replica holds the same rows.
//
// The scrub's selection is one forward pass over the log, so it cannot learn
// from the log itself that a type will switch to scrub further on: entries
// written before the switch would already be behind it. This history lets the
// selection resolve each type's setting AS OF the scrub bound before the pass
// starts, so switching to scrub removes every superseded version at or below
// the bound, including those written before the switch.
//
// Only registrations applied by a binary that knows the field leave rows, and
// a declared scrub is admitted only once every member is at feature level 3
// (db.featureLevelOnSupersede), so no replica computes the selection without
// the rows the others use.
var typeOnSupersedeBucket = []byte("typeOnSupersede")

// supersedeScrubCeiling bounds how long a superseded version of a scrub-declared
// type stays in the log: once a scrub-declared type has been written since the
// last completed scrub, the automatic scheduler scrubs at least this often. A
// var so tests can shrink it.
var supersedeScrubCeiling = 7 * 24 * time.Hour

// scrubCompletedAtKey (in pendingScrubBucket) holds the unix time, in seconds,
// at which this node last completed a scrub. Node-local, like the completed
// bound beside it; it only paces the scheduler.
var scrubCompletedAtKey = []byte("completedAt")

// declaresScrub reports whether a type registration declares onSupersede =
// "scrub". Snapshot types scrub by default either way; this is the explicit
// declaration the history records and the selection consults.
func declaresScrub(t *cluster.Type) bool {
	return t.OnSupersede == cluster.OnSupersedeScrub
}

// tracksCorrections reports whether writes to t feed the supersede ceiling:
// an explicit scrub on a kind whose writes are not already counted by the
// metadata volume gate (every snapshot write is).
func tracksCorrections(t *cluster.Type) bool {
	return declaresScrub(t) && t.EntityKind != cluster.EntityKindSnapshot
}

// recordTypeOnSupersedeTx appends one registration's declaration to the type's
// history, inside the caller's (saveType apply) transaction.
func recordTypeOnSupersedeTx(tx *bolt.Tx, t *cluster.Type, raftIndex uint64) error {
	// Created on demand as well as at Open: a bbolt swapped in from a snapshot
	// taken before the bucket existed lacks it.
	root, err := tx.CreateBucketIfNotExists(typeOnSupersedeBucket)
	if err != nil {
		return fmt.Errorf("[wal.type] onSupersede history: %w", err)
	}
	b, err := root.CreateBucketIfNotExists([]byte(t.ID))
	if err != nil {
		return fmt.Errorf("[wal.type] onSupersede history bucket: %w", err)
	}
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], raftIndex)
	v := []byte{0}
	if declaresScrub(t) {
		v[0] = 1
	}
	if err := b.Put(k[:], v); err != nil {
		return fmt.Errorf("[wal.type] record onSupersede: %w", err)
	}
	return nil
}

// scrubTypesAt returns the ids of the types whose latest registration at or
// below bound declared onSupersede = "scrub". A pure function of the history
// rows at or below bound, which every replica holds alike.
func (s *Storage) scrubTypesAt(bound uint64) (map[string]bool, error) {
	out := map[string]bool{}
	err := s.view(func(tx *bolt.Tx) error {
		root := tx.Bucket(typeOnSupersedeBucket)
		if root == nil {
			return nil // no registration has recorded a declaration yet
		}
		return root.ForEachBucket(func(id []byte) error {
			if declaredScrubAt(root.Bucket(id), bound) {
				out[string(id)] = true
			}
			return nil
		})
	})
	return out, err
}

// declaredScrubAt reads one type's history: the declaration of its latest
// registration at or below bound.
func declaredScrubAt(b *bolt.Bucket, bound uint64) bool {
	c := b.Cursor()
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:], bound)
	k, v := c.Seek(seek[:])
	if k == nil || binary.BigEndian.Uint64(k) > bound {
		k, v = c.Prev()
	}
	return k != nil && len(v) == 1 && v[0] == 1
}

// noteCorrectionWrite records that index applied a write the supersede ceiling
// must eventually scrub. Leader-local and in memory, like metadataBacklog.
func (s *Storage) noteCorrectionWrite(index uint64) {
	for {
		cur := s.correctionWrite.Load()
		if index <= cur || s.correctionWrite.CompareAndSwap(cur, index) {
			return
		}
	}
}

// hasSupersedeBacklog reports whether a scrub-declared type has been written
// since the last completed scrub and the ceiling has elapsed since that scrub.
func (s *Storage) hasSupersedeBacklog() bool {
	if s.correctionWrite.Load() <= s.lastScrubbedBound.Load() {
		return false
	}
	last := time.Unix(s.scrubCompletedAt.Load(), 0)
	return time.Since(last) >= supersedeScrubCeiling
}

// seedCorrectionWrite runs at Open and after a snapshot restore: the in-memory
// record of correction writes does not survive either, so if any live type
// tracks corrections, assume one is pending as of the applied index. The cost
// of being wrong is one extra scrub at the next ceiling.
func (s *Storage) seedCorrectionWrite(tx *bolt.Tx, applied uint64) error {
	b := tx.Bucket(typeBucket)
	if b == nil {
		return ErrBucketMissing
	}
	return b.ForEachBucket(func(id []byte) error {
		bs, err := getVersioned(b, id)
		if err != nil {
			return nil
		}
		var t cluster.Type
		if err := t.Unmarshal(bs); err != nil {
			return nil
		}
		if tracksCorrections(&t) {
			s.noteCorrectionWrite(applied)
		}
		return nil
	})
}

// loadScrubCompletedAt reads the persisted completion time (0 if none).
func loadScrubCompletedAt(tx *bolt.Tx) int64 {
	bkt := tx.Bucket(pendingScrubBucket)
	if bkt == nil {
		return 0
	}
	if b := bkt.Get(scrubCompletedAtKey); len(b) == 8 {
		return int64(binary.BigEndian.Uint64(b)) //nolint:gosec // G115: a unix time in seconds
	}
	return 0
}

// putScrubCompletedAt persists the completion time in the caller's transaction.
func putScrubCompletedAt(bkt *bolt.Bucket, at time.Time) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(at.Unix())) //nolint:gosec // G115: a unix time in seconds
	return bkt.Put(scrubCompletedAtKey, buf[:])
}
