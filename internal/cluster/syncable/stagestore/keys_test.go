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
