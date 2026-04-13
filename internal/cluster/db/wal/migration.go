package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// migrateToVersionedBuckets converts flat key=value entries in resource
// buckets to the versioned nested-bucket layout. Idempotent: flat entries
// (v != nil in ForEach) are migrated; sub-buckets (v == nil) are skipped.
// Runs in a single bbolt transaction so a crash mid-migration leaves the
// bucket unchanged (bbolt transactions are atomic).
func migrateToVersionedBuckets(tx *bolt.Tx) error {
	resourceBuckets := [][]byte{typeBucket, databaseBucket, ingestableBucket, syncableBucket}
	for _, bucketName := range resourceBuckets {
		b := tx.Bucket(bucketName)
		if b == nil {
			continue
		}
		if err := migrateBucket(b); err != nil {
			return fmt.Errorf("migrate bucket %s: %w", bucketName, err)
		}
	}
	return nil
}

// migrateBucket migrates a single resource bucket from flat to versioned layout.
func migrateBucket(b *bolt.Bucket) error {
	type entry struct {
		key   []byte
		value []byte
	}
	var flatEntries []entry

	err := b.ForEach(func(k, v []byte) error {
		if v != nil {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			valCopy := make([]byte, len(v))
			copy(valCopy, v)
			flatEntries = append(flatEntries, entry{key: keyCopy, value: valCopy})
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, e := range flatEntries {
		if err := b.Delete(e.key); err != nil {
			return fmt.Errorf("delete flat key %q: %w", e.key, err)
		}
		if _, err := putVersioned(b, e.key, e.value); err != nil {
			return fmt.Errorf("migrate key %q to versioned: %w", e.key, err)
		}
	}

	return nil
}
