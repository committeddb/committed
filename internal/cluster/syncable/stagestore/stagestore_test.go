package stagestore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreLifecycleAndFrontier(t *testing.T) {
	dir := t.TempDir()

	s, reset, err := Open(dir, "p1", "fp-v1")
	require.NoError(t, err)
	require.True(t, reset, "a fresh store reports reset — the caller backfills")

	require.NoError(t, s.Update(func(tx *Tx) error {
		if err := tx.PutOut("sums", []byte("k1"), []byte(`{"n":1}`)); err != nil {
			return err
		}
		return tx.SetFrontier(42)
	}))
	require.NoError(t, s.Close())

	// Reopen with the same fingerprint: state and frontier survive, no reset.
	s, reset, err = Open(dir, "p1", "fp-v1")
	require.NoError(t, err)
	require.False(t, reset, "same config, same store — resume, don't rebuild")
	f, err := s.Frontier()
	require.NoError(t, err)
	require.Equal(t, uint64(42), f)
	require.NoError(t, s.View(func(tx *Tx) error {
		v, err := tx.GetOut("sums", []byte("k1"))
		require.Equal(t, `{"n":1}`, string(v))
		return err
	}))
	require.NoError(t, s.Close())
}

func TestStoreFingerprintMismatchResets(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, "p1", "fp-v1")
	require.NoError(t, err)
	require.NoError(t, s.Update(func(tx *Tx) error { return tx.SetFrontier(7) }))
	require.NoError(t, s.Close())

	// A changed config must NOT resume old state: the store resets loudly.
	s, reset, err := Open(dir, "p1", "fp-v2")
	require.NoError(t, err)
	require.True(t, reset, "a changed pipeline must re-derive")
	f, err := s.Frontier()
	require.NoError(t, err)
	require.Zero(t, f)
	require.NoError(t, s.Close())
}

func TestStoreCorruptDatabaseResets(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(dbDir(dir), []byte("not a database, definitely"), 0o600))

	s, reset, err := Open(dir, "p1", "fp")
	require.NoError(t, err, "a torn store is deleted and rebuilt, never fatal")
	require.True(t, reset)
	require.NoError(t, s.Close())
}

// The one-file-per-syncable stores of earlier formats are removed when the
// shared database opens: nothing can read them, and their syncables
// re-derive.
func TestStoreRemovesLegacyFiles(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "p1.db")
	require.NoError(t, os.WriteFile(legacy, []byte("a format 3 bbolt store"), 0o600))

	s, reset, err := Open(dir, "p1", "fp")
	require.NoError(t, err)
	require.True(t, reset)
	require.NoError(t, s.Close())
	_, err = os.Stat(legacy)
	require.True(t, os.IsNotExist(err))
}

// Syncables share one database but never each other's state: a reset or a
// removal of one leaves the other whole.
func TestStoresShareADatabaseInIsolation(t *testing.T) {
	dir := t.TempDir()
	put := func(s *Store, v string) {
		require.NoError(t, s.Update(func(tx *Tx) error {
			return errors.Join(tx.PutOut("s", []byte("k"), []byte(v)), tx.SetFrontier(9))
		}))
	}
	get := func(s *Store) string {
		var got []byte
		require.NoError(t, s.View(func(tx *Tx) error {
			var err error
			got, err = tx.GetOut("s", []byte("k"))
			return err
		}))
		return string(got)
	}
	a, _, err := Open(dir, "a", "fp")
	require.NoError(t, err)
	b, _, err := Open(dir, "a2", "fp") // a name that extends another's
	require.NoError(t, err)
	put(a, "A")
	put(b, "B")
	require.Equal(t, "A", get(a))
	require.Equal(t, "B", get(b))

	require.NoError(t, a.Close())
	a, reset, err := Open(dir, "a", "fp-changed")
	require.NoError(t, err)
	require.True(t, reset)
	require.Empty(t, get(a), "a's state reset")
	require.Equal(t, "B", get(b), "b's state untouched")
	require.NoError(t, a.Close())

	require.NoError(t, Remove(dir, "a"))
	has, err := HasState(dir, "a")
	require.NoError(t, err)
	require.False(t, has)
	require.Equal(t, "B", get(b))
	require.NoError(t, b.Close())
	has, err = HasState(dir, "a2")
	require.NoError(t, err)
	require.True(t, has, "state survives the database closing")
}

func TestStoreEmptyDirRejected(t *testing.T) {
	_, _, err := Open("", "p1", "fp")
	require.ErrorContains(t, err, "projections dir")
}

func TestRetainedInputsPrefixScan(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, "p1", "fp")
	require.NoError(t, err)
	defer s.Close()

	// Adversarial keys: k1 is a strict prefix of k10, and one output key
	// embeds a NUL — the length framing must keep their input sets apart.
	require.NoError(t, s.Update(func(tx *Tx) error {
		for _, put := range [][3]string{
			{"k1", "a", "v-k1-a"},
			{"k1", "b", "v-k1-b"},
			{"k10", "a", "v-k10-a"},
			{"k\x001", "a", "v-nul-a"},
		} {
			if err := tx.PutIn("s", []byte(put[0]), []byte(put[1]), []byte(put[2])); err != nil {
				return err
			}
		}
		return nil
	}))

	scan := func(outKey string) (got []string) {
		require.NoError(t, s.View(func(tx *Tx) error {
			return tx.InputsFor("s", []byte(outKey), func(inKey, val []byte) error {
				got = append(got, string(inKey)+"="+string(val))
				return nil
			})
		}))
		return
	}
	require.Equal(t, []string{"a=v-k1-a", "b=v-k1-b"}, scan("k1"))
	require.Equal(t, []string{"a=v-k10-a"}, scan("k10"))
	require.Equal(t, []string{"a=v-nul-a"}, scan("k\x001"))

	// Deleting one retained input leaves its siblings.
	require.NoError(t, s.Update(func(tx *Tx) error {
		return tx.DeleteIn("s", []byte("k1"), []byte("a"))
	}))
	require.Equal(t, []string{"b=v-k1-b"}, scan("k1"))
}

func TestReverseIndexDependents(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, "p1", "fp")
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Update(func(tx *Tx) error {
		if err := tx.PutRev("s", "projects", []byte("proj-1"), []byte("wa-1")); err != nil {
			return err
		}
		if err := tx.PutRev("s", "projects", []byte("proj-1"), []byte("wa-2")); err != nil {
			return err
		}
		return tx.PutRev("s", "projects", []byte("proj-2"), []byte("wa-3"))
	}))

	deps := func(dim string) (got []string) {
		require.NoError(t, s.View(func(tx *Tx) error {
			return tx.DependentsOf("s", "projects", []byte(dim), func(outKey []byte) error {
				got = append(got, string(outKey))
				return nil
			})
		}))
		return
	}
	require.Equal(t, []string{"wa-1", "wa-2"}, deps("proj-1"))
	require.Equal(t, []string{"wa-3"}, deps("proj-2"))

	require.NoError(t, s.Update(func(tx *Tx) error {
		return tx.DeleteRev("s", "projects", []byte("proj-1"), []byte("wa-1"))
	}))
	require.Equal(t, []string{"wa-2"}, deps("proj-1"))
}

func TestOutDeleteAndMissingBucketReads(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, "p1", "fp")
	require.NoError(t, err)
	defer s.Close()

	// Reads against never-written stages are nil, never errors.
	require.NoError(t, s.View(func(tx *Tx) error {
		v, err := tx.GetOut("ghost", []byte("k"))
		require.Nil(t, v)
		return err
	}))

	require.NoError(t, s.Update(func(tx *Tx) error {
		return tx.PutOut("s", []byte("k"), []byte("v"))
	}))
	require.NoError(t, s.Update(func(tx *Tx) error {
		return tx.DeleteOut("s", []byte("k"))
	}))
	require.NoError(t, s.View(func(tx *Tx) error {
		v, err := tx.GetOut("s", []byte("k"))
		require.Nil(t, v)
		return err
	}))
}
