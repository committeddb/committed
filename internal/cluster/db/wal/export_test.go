package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
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
	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return walkBucket(string(name), b, &lines)
		})
	})
	if err != nil {
		return nil, err
	}
	return lines, nil
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
