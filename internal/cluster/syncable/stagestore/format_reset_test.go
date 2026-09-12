package stagestore

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// A store stamped with another format — older (an upgrade) or newer (a
// rollback) — resets rather than resuming: the two binaries do not agree on
// what the bytes mean, and a replay from the log is always available.
func TestStoreOtherFormatResets(t *testing.T) {
	for _, stamped := range []uint64{formatVersion - 1, formatVersion + 1} {
		dir := t.TempDir()
		s, _, err := Open(dir, "p1", "fp-v1")
		require.NoError(t, err)
		require.NoError(t, s.Update(func(tx *Tx) error { return tx.SetFrontier(7) }))
		require.NoError(t, s.Close())

		db, err := bolt.Open(FilePath(dir, "p1"), 0o600, nil)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			var fv [8]byte
			binary.BigEndian.PutUint64(fv[:], stamped)
			return tx.Bucket(metaBucket).Put(metaFormat, fv[:])
		}))
		require.NoError(t, db.Close())

		s, reset, err := Open(dir, "p1", "fp-v1")
		require.NoError(t, err)
		require.True(t, reset, "format %d must not resume under format %d", stamped, formatVersion)
		f, err := s.Frontier()
		require.NoError(t, err)
		require.Zero(t, f, "a reset store starts from the log's beginning")
		require.NoError(t, s.Close())
	}
}
