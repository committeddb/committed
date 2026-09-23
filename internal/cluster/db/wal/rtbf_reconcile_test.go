package wal

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestReconcileDeletesAcrossBoltPages(t *testing.T) {
	for _, bound := range []uint64{0, 2048, math.MaxUint64} {
		t.Run(strconv.FormatUint(bound, 10), func(t *testing.T) {
			s, err := Open(t.TempDir(), nil, nil, nil, WithSafeMode())
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })
			const count = 4096
			require.NoError(t, s.update(func(tx *bolt.Tx) error {
				b := tx.Bucket(unhashedDeleteBucket)
				var key [8]byte
				for i := uint64(1); i <= count; i++ {
					binary.BigEndian.PutUint64(key[:], i)
					if err := b.Put(key[:], nil); err != nil {
						return err
					}
				}
				binary.BigEndian.PutUint64(key[:], math.MaxUint64)
				return b.Put(key[:], nil)
			}))
			// Keep one raw delete, erase one by threshold, and remove an earlier
			// snapshot delete superseded by metadata GC. Duplicate entities at one
			// record index still produce just one cadence row.
			var raws []rawDelete
			if bound >= 2048 {
				raws = []rawDelete{{index: 1}, {index: 10}, {index: 20, tk: "superseded"}, {index: 10}}
			}
			require.NoError(t, s.reconcileUnhashedDeletes(bound, 1, raws, map[string]uint64{"superseded": 21}))
			verify := func() {
				t.Helper()
				require.NoError(t, s.view(func(tx *bolt.Tx) error {
					b := tx.Bucket(unhashedDeleteBucket)
					var key [8]byte
					kept := 0
					for i := uint64(1); i <= count; i++ {
						binary.BigEndian.PutUint64(key[:], i)
						expected := i > bound || (bound >= 2048 && i == 10)
						if expected {
							require.NotNil(t, b.Get(key[:]), "retained index %d", i)
							kept++
						} else {
							require.Nil(t, b.Get(key[:]), "removed index %d", i)
						}
					}
					binary.BigEndian.PutUint64(key[:], math.MaxUint64)
					if bound < math.MaxUint64 {
						require.NotNil(t, b.Get(key[:]))
						kept++
					} else {
						require.Nil(t, b.Get(key[:]))
					}
					require.Equal(t, kept, b.Stats().KeyN)
					return nil
				}))
			}
			verify()
			require.NoError(t, s.reconcileUnhashedDeletes(bound, 1, raws, map[string]uint64{"superseded": 21}))
			verify()
		})
	}
}
