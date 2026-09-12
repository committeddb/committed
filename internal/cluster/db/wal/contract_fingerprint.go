package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/cluster"
)

// handleContractFingerprint applies the validation tripwire's replicated
// dedupe mark: "this divergent shape was announced for this type version"
// (see cluster.ContractFingerprint). Guarded on the announce-typed TYPE's
// config still existing — the write-guard half of the delete-safety contract
// (config_sibling_state.go): an announce committed just after the type's
// delete must not re-establish state a same-id recreate would then read as
// already-announced. The sweep half lives in sweepTypeSiblingState.
func (s *Storage) handleContractFingerprint(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		return s.update(func(tx *bolt.Tx) error {
			b := tx.Bucket(contractFingerprintBucket)
			if b == nil {
				return ErrBucketMissing
			}
			return b.Delete(e.Key)
		})
	}

	f := &cluster.ContractFingerprint{}
	if err := f.Unmarshal(e.Data); err != nil {
		return fmt.Errorf("[wal.contract-fingerprint] unmarshal: %w", err)
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(contractFingerprintBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if !configExists(tx, typeBucket, []byte(f.TypeID)) {
			return b.Delete(e.Key)
		}
		return b.Put(e.Key, e.Data)
	})
}

// HasContractFingerprint implements the db.Storage read: has this divergent
// shape already been announced for this type version?
func (s *Storage) HasContractFingerprint(typeID string, version int, fingerprint string) bool {
	key := (&cluster.ContractFingerprint{TypeID: typeID, Version: version, Fingerprint: fingerprint}).Key()
	found := false
	_ = s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(contractFingerprintBucket); b != nil {
			found = b.Get(key) != nil
		}
		return nil
	})
	return found
}

// sweepContractFingerprints removes every dedupe mark for the deleted type —
// the sweep half of the delete-safety contract, called in the same tx as the
// type's config delete so a same-id recreate starts with a clean announce
// slate. Marks are keyed "typeID\x00version\x00fingerprint", so the type's
// marks are exactly the "typeID\x00" prefix range.
func sweepContractFingerprints(tx *bolt.Tx, typeID []byte) error {
	b := tx.Bucket(contractFingerprintBucket)
	if b == nil {
		return nil
	}
	prefix := append(append([]byte{}, typeID...), 0x00)
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
		if err := b.Delete(k); err != nil {
			return fmt.Errorf("[wal.contract-fingerprint] sweep: %w", err)
		}
	}
	return nil
}
