package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// BucketSnapshot returns every (bucket, key, value) triple in this
// storage's bbolt as a slice of formatted "bucket/hex(key)=hex(value)"
// lines, walked in sorted bucket-name order with each bucket's keys
// walked in lexicographic order. Tests pass two snapshots through
// testify's require.Equal to get a per-line diff when apply has
// produced divergent state across nodes.
//
// Defined in export_test.go (package wal, not wal_test) so it's only
// compiled into the test binary — production code does not need this
// and shouldn't carry it. The wal_test external test package can call
// it because Go compiles _test.go files of the package being tested
// into the same binary as the external test package.
func (s *Storage) BucketSnapshot() ([]string, error) {
	var lines []string
	err := s.keyValueStorage.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(k, v []byte) error {
				lines = append(lines, fmt.Sprintf("%s/%x=%x", name, k, v))
				return nil
			})
		})
	})
	if err != nil {
		return nil, err
	}
	return lines, nil
}
