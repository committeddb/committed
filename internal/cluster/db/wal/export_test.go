package wal

import (
	"crypto/sha256"
	"fmt"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
)

// BucketSnapshot returns every (bucket, key, value) triple in this
// storage's bbolt as a slice of formatted "path/hex(key)=hex(value)"
// lines, walked in sorted bucket-name order with each bucket's keys
// walked in lexicographic order. Nested sub-buckets (used by versioned
// config storage) are recursed into with "/" separators in the path.
//
// Tests pass two snapshots through testify's require.Equal to get a
// per-line diff when apply has produced divergent state across nodes.
//
// Defined in export_test.go (package wal, not wal_test) so it's only
// compiled into the test binary.
func (s *Storage) BucketSnapshot() ([]string, error) {
	var lines []string
	err := s.view(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(string(name), b, &lines)
		})
	})
	if err != nil {
		return nil, err
	}
	return lines, nil
}

// EventLogLastSeq exposes the event log's wal sequence for test
// assertions. External callers should use EventIndex() (raft index) —
// this accessor is only here because a couple of Phase 1 tests assert
// on the wal-seq count to prove the log is being appended to.
func (s *Storage) EventLogLastSeq() (uint64, error) {
	return s.lastEventSeq()
}

// ReadEventAt exposes event-log entries by wal seq for test assertions.
// Production callers go through Reader.
func (s *Storage) ReadEventAt(seq uint64) ([]byte, error) {
	return s.readEventAt(seq)
}

// Unframe strips and verifies the per-entry checksum frame from a raw log
// read, so tests that inspect on-disk bytes directly (e.g. via EntryLog /
// StateLog) can decode the payload. Production reads go through
// entry()/state()/readEventAt, which unframe and verify on the live path.
// Returns ErrCorruptEntry on a CRC mismatch; legacy un-checksummed bytes
// pass through unchanged.
func Unframe(raw []byte) ([]byte, error) {
	return unframe(raw)
}

// TombstoneSelections exposes tombstoneSelections (the scrubber's "max delete
// index <= bound per (type,key)" view) for test assertions.
func (s *Storage) TombstoneSelections(bound uint64) (map[string]uint64, error) {
	return s.tombstoneSelections(bound)
}

// RunScrubForTest runs the scrub rewrite synchronously and marks it complete,
// bypassing the background worker so tests are deterministic. Tests that use it
// must not also drive a Scrub command through apply (which would race the
// worker); they call this directly instead.
func (s *Storage) RunScrubForTest(bound uint64) error {
	if err := s.runScrub(bound); err != nil {
		return err
	}
	return s.markScrubComplete(bound)
}

// FirstEventIndex exposes the firstEventIndex atomic (raft index of the first
// event-log entry) for determinism assertions after a scrub.
func (s *Storage) FirstEventIndex() uint64 {
	return s.firstEventIndex.Load()
}

// SetPendingScrubBoundForTest persists a pending scrub bound without going
// through a committed Scrub command, so crash-recovery tests can stage a
// "pending scrub" that the next Open's worker must resume.
func (s *Storage) SetPendingScrubBoundForTest(b uint64) error {
	return s.setPendingScrubBound(b)
}

// ScrubCompletedBound exposes the highest completed scrub bound, for polling the
// async worker in end-to-end tests.
func (s *Storage) ScrubCompletedBound() uint64 {
	return s.lastScrubbedBound.Load()
}

// EventLogSnapshot returns one line per event-log record — wal seq, embedded
// raft index, entry type, and a content hash — for byte-identical cross-node
// comparison after a scrub. Mirrors BucketSnapshot's role for the event log.
func (s *Storage) EventLogSnapshot() ([]string, error) {
	first, err := s.firstEventSeq()
	if err != nil {
		return nil, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return nil, err
	}
	var lines []string
	if first == 0 || last == 0 {
		return lines, nil
	}
	for seq := first; seq <= last; seq++ {
		raw, err := s.readEventAt(seq)
		if err != nil {
			return nil, err
		}
		e := &pb.Entry{}
		if err := e.Unmarshal(raw); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		lines = append(lines, fmt.Sprintf("seq=%d idx=%d type=%d sha=%x", seq, e.Index, e.Type, sum))
	}
	return lines, nil
}

// TombstoneKey exposes the bucket-key encoding so tests can assert on the
// selections map without duplicating the separator convention.
func TombstoneKey(typeID string, key []byte) string {
	return string(tombstoneKey(typeID, key))
}

func walkBucket(prefix string, b *bolt.Bucket, lines *[]string) error {
	return b.ForEach(func(k, v []byte) error {
		if v == nil {
			sub := b.Bucket(k)
			if sub != nil {
				return walkBucket(prefix+"/"+string(k), sub, lines)
			}
			return nil
		}
		*lines = append(*lines, fmt.Sprintf("%s/%x=%x", prefix, k, v))
		return nil
	})
}
