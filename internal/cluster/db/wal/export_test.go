package wal

import (
	"crypto/sha256"
	"fmt"

	bolt "go.etcd.io/bbolt"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
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

// PutRawSyncableIndexForTest writes raw bytes under the syncable-index key for
// id, bypassing marshaling, so a test can plant a corrupt (undecodable)
// checkpoint and exercise Reader's corrupt-vs-fresh-start handling. Defined in
// export_test.go (package wal) so external tests can reach the unexported
// bucket.
func (s *Storage) PutRawSyncableIndexForTest(id string, raw []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(syncableIndexBucket)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), raw)
	})
}

// EventLogLastSeq exposes the event log's wal sequence for test
// assertions. External callers should use EventIndex() (raft index) —
// this accessor is only here because a couple of Phase 1 tests assert
// on the wal-seq count to prove the log is being appended to.
func (s *Storage) EventLogLastSeq() (uint64, error) {
	return s.lastEventSeq()
}

// SimulateCrashedSnapshotInstallForTest reproduces the durable state left by a
// crash DURING an in-place snapshot install: saveWithSnapshot has persisted the
// snapshot record and cut the entry log to the snapshot point, but
// RestoreSnapshot has NOT yet swapped bbolt — so the applied index still lags
// below the snapshot index and bbolt holds the pre-install content. The next
// Open must heal it via reconcileBboltWithSnapshot. Test-only.
func (s *Storage) SimulateCrashedSnapshotInstallForTest(snap *pb.Snapshot) error {
	return s.saveWithSnapshot(s.hardState, nil, snap)
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

// MarkScrubCompleteForTest exposes markScrubComplete (prune + compact, fused) so a
// test can assert that the instant it returns, a snapshot carries no erased key —
// i.e. no prune→compact window a concurrent CreateSnapshot could slip through.
func (s *Storage) MarkScrubCompleteForTest(bound uint64) error {
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
		if err := proto.Unmarshal(raw, e); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(raw)
		lines = append(lines, fmt.Sprintf("seq=%d idx=%d type=%d sha=%x", seq, e.GetIndex(), e.GetType(), sum))
	}
	return lines, nil
}

// TombstoneKey exposes the bucket-key encoding so tests can assert on the
// selections map without duplicating the separator convention.
func TombstoneKey(typeID string, key []byte) string {
	return string(tombstoneKey(typeID, key))
}

// EventIndices returns the raft indices present in the event log, in seq order,
// so scrub tests can assert exactly which entries survived without hydrating
// entities through a resolver.
func (s *Storage) EventIndices() ([]uint64, error) {
	first, err := s.firstEventSeq()
	if err != nil {
		return nil, err
	}
	last, err := s.lastEventSeq()
	if err != nil {
		return nil, err
	}
	var out []uint64
	if first == 0 || last == 0 {
		return out, nil
	}
	for seq := first; seq <= last; seq++ {
		raw, err := s.readEventAt(seq)
		if err != nil {
			return nil, err
		}
		e := &pb.Entry{}
		if err := proto.Unmarshal(raw, e); err != nil {
			return nil, err
		}
		out = append(out, e.GetIndex())
	}
	return out, nil
}

// MetadataSupersessions exposes the metadata-GC "max index <= bound per
// system-tombstonable (type, key)" selection for test assertions.
func (s *Storage) MetadataSupersessions(bound uint64) (map[string]uint64, error) {
	return s.metadataSupersessions(bound)
}

// SetMetadataBacklogThresholdForTest lowers the metadata-backlog trigger so a
// test can exercise HasScrubBacklog's metadata term without applying 128
// fsync-bound entries. Returns a func that restores the production value.
func SetMetadataBacklogThresholdForTest(n int64) func() {
	old := metadataBacklogThreshold
	metadataBacklogThreshold = n
	return func() { metadataBacklogThreshold = old }
}

// Frame applies the per-entry checksum frame, so tests can forge
// legacy-format state-log records directly on disk (the pre-truncation code
// appended a HardState + full-snapshot record pair on every Ready; recovery
// and Open-time reclaim of such logs need coverage).
func Frame(payload []byte) []byte {
	return frame(payload)
}

// SetStateLogReanchorFloorForTest lowers the snapshot re-anchor floor so
// tests can exercise appendState's re-anchoring without writing 64KB of
// HardState records. Returns a func that restores the production value.
func SetStateLogReanchorFloorForTest(n int) func() {
	old := stateLogReanchorFloor
	stateLogReanchorFloor = n
	return func() { stateLogReanchorFloor = old }
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

// SeedSyncableConfigForTest / SeedIngestableConfigForTest create a minimal
// versioned config entry for id so the checkpoint/position guards
// (saveSyncableIndex / saveIngestablePosition) see the config as present. They
// enforce the production invariant that a progress record persists only for a
// live config; a test that saves a SyncableIndex / IngestablePosition directly,
// without a real config apply, must first establish that the config exists.
func (s *Storage) SeedSyncableConfigForTest(id string) error {
	bs, err := minimalConfigBytesForTest(id)
	if err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		_, err := putVersioned(tx.Bucket(syncableBucket), []byte(id), bs)
		return err
	})
}

func (s *Storage) SeedIngestableConfigForTest(id string) error {
	bs, err := minimalConfigBytesForTest(id)
	if err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		_, err := putVersioned(tx.Bucket(ingestableBucket), []byte(id), bs)
		return err
	})
}

// minimalConfigBytesForTest marshals a well-formed Configuration so the seeded
// entry survives the startup secret re-validation (which unmarshals every stored
// config). Empty Data carries no ${VAR}, so interpolation is a no-op.
func minimalConfigBytesForTest(id string) ([]byte, error) {
	return (&cluster.Configuration{ID: id, MimeType: "application/json", Data: []byte("{}")}).Marshal()
}
