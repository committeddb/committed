package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// The delete-safety contract for per-config-id state has two halves:
//
//   - (A) sweep on delete — remove the state atomically with the config, here.
//   - (B) write-guard — refuse to (re-)establish the state once the config bucket
//     is gone, via configExists at each writer (dead_letter.go, ingest_source_seq.go,
//     stuck.go; the checkpoint/position references in syncable_index.go /
//     ingestable_position.go).
//
// BOTH are required. A sweep alone can't stop a worker bump that commits after the
// delete (DeleteSyncable does not drain the worker first) or an old write replayed
// from the log before a same-id recreate; a guard alone can't reap the state that
// already exists at delete time. Together: the sweep clears the live state, and the
// guard reaps any post-delete straggler — so a same-id recreate always starts
// clean instead of silently skipping work (dead-letters, source-seq) or reporting
// stale status (stuck, skip-request).
//
// The checkpoint (syncableIndex) and position (ingestablePosition) are NOT swept
// here: each rides its own tombstone in the delete bundle (NewDeleteSyncableEntities
// / NewDeleteIngestableEntities), which is the equivalent of a sweep for a
// single-key-per-id state that is itself a log entity.

// sweepSyncableSiblingState removes the per-syncable-id state kept OUTSIDE the
// syncable config sub-bucket and not tombstoned in the delete bundle: the
// dead-letter sub-bucket, the stuck record, and the skip request. Called in the
// same bbolt tx as the config delete (deleteSyncable) so config and siblings die
// atomically.
func sweepSyncableSiblingState(tx *bolt.Tx, id []byte) error {
	if dl := tx.Bucket(syncableDeadLetterBucket); dl != nil && dl.Bucket(id) != nil {
		if err := dl.DeleteBucket(id); err != nil {
			return fmt.Errorf("[wal.syncable] sweep dead-letters: %w", err)
		}
	}
	if b := tx.Bucket(syncableStuckBucket); b != nil {
		if err := b.Delete(id); err != nil {
			return fmt.Errorf("[wal.syncable] sweep stuck: %w", err)
		}
	}
	if b := tx.Bucket(syncableSkipRequestBucket); b != nil {
		if err := b.Delete(id); err != nil {
			return fmt.Errorf("[wal.syncable] sweep skip-request: %w", err)
		}
	}
	return nil
}

// sweepTypeSiblingState removes the per-type-id state kept OUTSIDE the type config
// sub-bucket and not tombstoned in the delete bundle: the migration dead-letter
// sub-bucket (the type-domain twin of the syncable dead-letter). Called in the same
// bbolt tx as the config delete (deleteType) so a same-id type recreate does not
// inherit stale migration dead-letters and skip those indices.
func sweepTypeSiblingState(tx *bolt.Tx, id []byte) error {
	if dl := tx.Bucket(typeMigrationDeadLetterBucket); dl != nil && dl.Bucket(id) != nil {
		if err := dl.DeleteBucket(id); err != nil {
			return fmt.Errorf("[wal.type] sweep migration dead-letters: %w", err)
		}
	}
	return nil
}

// sweepIngestableSiblingState removes the per-ingestable-id state kept outside the
// ingestable config sub-bucket and not tombstoned in the delete bundle: the
// source-seq highwater. Called in the same tx as the config delete
// (deleteIngestable) so a same-id recreate's re-emitted CDC proposals are not
// dropped pre-raft by a stale highwater.
//
// topicRefreshEpoch is deliberately NOT swept: it is keyed by TOPIC (type id), not
// ingestable id, and must outlive any ingestable incarnation (see wal_storage.go /
// topic_refresh_epoch.go) — it is the intentional inverse of this sweep.
func sweepIngestableSiblingState(tx *bolt.Tx, id []byte) error {
	if b := tx.Bucket(ingestSourceSeqBucket); b != nil {
		if err := b.Delete(id); err != nil {
			return fmt.Errorf("[wal.ingestable] sweep source-seq: %w", err)
		}
	}
	return nil
}

// putConfigGuardedKeyed writes value under id in the flat stateBucket only while the
// config of the given kind (configBucket) still exists; if the config is gone it
// reaps any lingering value instead. It is the flat-bucket twin of the
// checkpoint/position reap guards, used by the per-syncable status writers (stuck,
// skip-request) so a bump that commits after the config was deleted can't
// re-establish an orphan a same-id recreate would then read.
func (s *Storage) putConfigGuardedKeyed(configBucket, stateBucket, id, value []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if !configExists(tx, configBucket, id) {
			return b.Delete(id)
		}
		return b.Put(id, value)
	})
}
