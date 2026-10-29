package stagestore

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// Random input writes, deletes and rekeys over a few output keys, with
// every key hashing to one of two ids so each probe chain is long, checked
// against a model after every transaction: inputs per key, src, the
// dictionary's size, and that an emptied key's id is freed when no probe
// passes through it.
func TestKeyDictionaryUnderCollisions(t *testing.T) {
	hash := keyHash
	t.Cleanup(func() { keyHash = hash })
	keyHash = func(k []byte) uint64 { return uint64(len(k) % 2) }

	s, _, err := Open(t.TempDir(), "p", "fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	outKeys := []string{"a", "bb", "ccc", "dddd", "e", "ff"}
	model := map[string]string{} // inKey → outKey
	rng := rand.New(rand.NewSource(3))
	for round := range 400 {
		require.NoError(t, s.Update(func(tx *Tx) error {
			for range 1 + rng.Intn(4) {
				in := fmt.Sprintf("i%02d", rng.Intn(20))
				if prior, ok := model[in]; ok {
					if err := tx.DeleteIn("st", []byte(prior), []byte(in)); err != nil {
						return err
					}
					if err := tx.DeleteSrc("st", []byte(in)); err != nil {
						return err
					}
					delete(model, in)
				}
				if rng.Intn(3) > 0 {
					out := outKeys[rng.Intn(len(outKeys))]
					if err := tx.PutIn("st", []byte(out), []byte(in), []byte(in+"@"+out)); err != nil {
						return err
					}
					if err := tx.PutSrc("st", []byte(in), []byte(out)); err != nil {
						return err
					}
					model[in] = out
				}
			}
			return nil
		}), "round %d", round)

		require.NoError(t, s.View(func(tx *Tx) error {
			live := map[string]bool{}
			for in, out := range model {
				live[out] = true
				got, err := tx.GetSrc("st", []byte(in))
				require.NoError(t, err)
				require.Equal(t, out, string(got), "round %d src %s", round, in)
				v, err := tx.GetIn("st", []byte(out), []byte(in))
				require.NoError(t, err)
				require.Equal(t, in+"@"+out, string(v))
			}
			for _, out := range outKeys {
				var want, got []string
				for in, o := range model {
					if o == out {
						want = append(want, in)
					}
				}
				sort.Strings(want)
				require.NoError(t, tx.InputsFor("st", []byte(out), func(inKey, _ []byte) error {
					got = append(got, string(inKey))
					return nil
				}))
				require.Equal(t, want, got, "round %d inputs of %s", round, out)
			}
			var all int
			require.NoError(t, tx.InputsAll("st", func(outKey, inKey, _ []byte) error {
				require.Equal(t, model[string(inKey)], string(outKey))
				all++
				return nil
			}))
			require.Equal(t, len(model), all)
			// Every live key is in the dictionary; a dead one lingers only
			// where a probe still passes through it.
			var ids int
			require.NoError(t, tx.each(keysBucket("st"), nil, func(_, _ []byte) error {
				ids++
				return nil
			}))
			require.GreaterOrEqual(t, ids, len(live))
			require.LessOrEqual(t, ids, len(outKeys))
			return nil
		}))
	}
	// Emptying every key in one transaction frees every id.
	require.NoError(t, s.Update(func(tx *Tx) error {
		for in, out := range model {
			if err := tx.DeleteIn("st", []byte(out), []byte(in)); err != nil {
				return err
			}
			if err := tx.DeleteSrc("st", []byte(in)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, s.View(func(tx *Tx) error {
		var ids int
		require.NoError(t, tx.each(keysBucket("st"), nil, func(_, _ []byte) error { ids++; return nil }))
		require.Zero(t, ids, "emptying every key frees every id")
		return nil
	}))
}

// KeyAdded names the output keys given an id since the transaction began
// or last forgot them: a key holding inputs from an earlier transaction is
// not one, and a key whose last input left is one again once re-added.
func TestKeyAdded(t *testing.T) {
	s, _, err := Open(t.TempDir(), "p", "fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	put := func(tx *Tx, out, in string) error { return tx.PutIn("st", []byte(out), []byte(in), []byte("v")) }
	require.NoError(t, s.Update(func(tx *Tx) error {
		require.False(t, tx.KeyAdded("st", []byte("x")))
		require.NoError(t, put(tx, "x", "a"))
		require.True(t, tx.KeyAdded("st", []byte("x")))
		require.False(t, tx.KeyAdded("other", []byte("x")), "per stage")
		tx.ForgetAddedKeys()
		require.False(t, tx.KeyAdded("st", []byte("x")))
		require.NoError(t, put(tx, "x", "b"))
		require.False(t, tx.KeyAdded("st", []byte("x")), "x already had an id")
		return nil
	}))
	require.NoError(t, s.Update(func(tx *Tx) error {
		require.NoError(t, put(tx, "x", "c"))
		require.False(t, tx.KeyAdded("st", []byte("x")), "x holds inputs from an earlier transaction")
		for _, in := range []string{"a", "b", "c"} {
			if err := tx.DeleteIn("st", []byte("x"), []byte(in)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, s.Update(func(tx *Tx) error {
		require.NoError(t, put(tx, "x", "d"))
		require.True(t, tx.KeyAdded("st", []byte("x")), "x's id was released with its last input")
		return nil
	}))
}

// The epoch sweep's scan over every input resolves each key without
// keeping the key dictionary in the transaction: on a large stage that
// would hold every key in memory.
func TestInputsAllKeepsNoKeyCache(t *testing.T) {
	s, _, err := Open(t.TempDir(), "p", "fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	want := map[string]string{}
	require.NoError(t, s.Update(func(tx *Tx) error {
		for i := range 200 {
			out, in := fmt.Sprintf("k%d", i%50), fmt.Sprintf("i%d", i)
			want[in] = out
			if err := tx.PutIn("st", []byte(out), []byte(in), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	}))
	got := map[string]string{}
	require.NoError(t, s.View(func(tx *Tx) error {
		if err := tx.InputsAll("st", func(outKey, inKey, _ []byte) error {
			got[string(inKey)] = string(outKey)
			return nil
		}); err != nil {
			return err
		}
		require.Empty(t, tx.keys)
		require.Empty(t, tx.ids)
		return nil
	}))
	require.Equal(t, want, got)
}

// A fold state lives under its output key's id: it is read back by the
// key, needs the key to retain an input, and is dropped with the id, so a
// key given the id next starts without one.
func TestFoldStateFollowsItsKeyID(t *testing.T) {
	hash := keyHash
	t.Cleanup(func() { keyHash = hash })
	keyHash = func([]byte) uint64 { return 7 } // every key probes from one id

	s, _, err := Open(t.TempDir(), "p", "fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	require.NoError(t, s.Update(func(tx *Tx) error {
		require.ErrorContains(t, tx.PutAcc("st", []byte("a"), []byte("state-a")), "retains no input")
		require.NoError(t, tx.PutIn("st", []byte("a"), []byte("i1"), []byte("v")))
		require.NoError(t, tx.PutIn("st", []byte("b"), []byte("i2"), []byte("v")))
		require.NoError(t, tx.PutAcc("st", []byte("a"), []byte("state-a")))
		return tx.PutAcc("st", []byte("b"), []byte("state-b"))
	}))
	require.NoError(t, s.View(func(tx *Tx) error {
		got, err := tx.GetAcc("st", []byte("b"))
		require.NoError(t, err)
		require.Equal(t, "state-b", string(got))
		states := map[string]string{}
		require.NoError(t, tx.EachAcc("st", func(k, v []byte) error {
			states[string(k)] = string(v)
			return nil
		}))
		require.Equal(t, map[string]string{"a": "state-a", "b": "state-b"}, states)
		return nil
	}))

	// a's last input leaves with its state left behind; b's id is the next
	// one, so a's id stays held until b goes too.
	require.NoError(t, s.Update(func(tx *Tx) error {
		require.NoError(t, tx.DeleteIn("st", []byte("b"), []byte("i2")))
		return tx.DeleteIn("st", []byte("a"), []byte("i1"))
	}))
	require.NoError(t, s.Update(func(tx *Tx) error {
		require.NoError(t, tx.PutIn("st", []byte("c"), []byte("i3"), []byte("v")))
		got, err := tx.GetAcc("st", []byte("c"))
		require.NoError(t, err)
		require.Nil(t, got, "c took a's id; a's state went with it")
		return nil
	}))
}
