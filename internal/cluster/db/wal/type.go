package wal

import (
	"bytes"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

func (s *Storage) handleType(e *cluster.Entity, raftIndex uint64) error {
	if e.IsDelete() {
		return s.deleteType(e.Key)
	} else {
		t := &cluster.Type{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveType(t, raftIndex)
	}
}

func (s *Storage) saveType(t *cluster.Type, raftIndex uint64) error {
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}

		// Replay guard (config-version-replay): ApplyCommittedBatch can replay a
		// whole Ready on a crash-window restart. A versioned apply whose entry
		// index already produced a version is a replay — skip it, or the last+1
		// allocator appends a phantom version, diverging history across replicas.
		// The set below rides this same atomic tx, so a failure rolls both back.
		if versionedLastIndex(b, []byte(t.ID)) >= raftIndex {
			return nil
		}
		if err := setVersionedLastIndex(b, []byte(t.ID), raftIndex); err != nil {
			return err
		}

		existing, err := getVersioned(b, []byte(t.ID))
		if err == nil {
			var prev cluster.Type
			if err := prev.Unmarshal(existing); err == nil && prev.Version == t.Version {
				// Same (typeID, version). Schema is immutable but the
				// migration, entity-kind (unspecified→declared adoption
				// only; ProposeType rejects every other change),
				// discriminator, and schema-change-topic fields are mutable
				// in place — operators may need to retroactively fix a
				// forgotten or buggy migration, declare an entity kind on a
				// grandfathered type, or re-point where divergences
				// announce — and a type stored without its document adopts
				// the first one submitted for it. If only those differ,
				// overwrite the current version entry in place (the
				// retained document comes along). If everything is
				// byte-identical (Raft replay), skip silently.
				if bytes.Equal(prev.Schema, t.Schema) &&
					prev.SchemaType == t.SchemaType &&
					prev.Validate == t.Validate &&
					prev.Name == t.Name {
					migrationEdited := !bytes.Equal(prev.Migration, t.Migration)
					changed := migrationEdited ||
						prev.EntityKind != t.EntityKind ||
						prev.Discriminator != t.Discriminator ||
						prev.SchemaChangeTopic != t.SchemaChangeTopic ||
						(len(prev.Document) == 0 && len(t.Document) > 0)
					if changed {
						// An in-place MIGRATION edit moves the interpretation
						// coordinate: record its apply index (same atomic tx)
						// so always-current consumers pinned below it read
						// interpretationStale until re-materialized. Kind
						// adoption, discriminator, and destination edits are
						// inert over history and record nothing.
						if migrationEdited {
							if err := putTypeMigrationEditTx(tx, t.ID, raftIndex); err != nil {
								return err
							}
						}
						s.logger.Info("updating mutable fields for existing type version",
							zap.String("id", t.ID), zap.Int("version", t.Version))
						return overwriteCurrentVersion(b, []byte(t.ID), t)
					}
				}
				// Byte-identical replay: skip.
				return nil
			}
		}

		// NOTE: this re-marshals from THIS binary's struct, so a field the
		// binary does not know is dropped rather than carried through — which
		// is why every field added to this record from 0.8.0 on is gated on
		// the cluster feature level (see db.featureLevelTypeRecord). Add a
		// field here and you must add its gate.
		bs, err := t.Marshal()
		if err != nil {
			return err
		}
		_, err = putVersioned(b, []byte(t.ID), bs)
		return err
	})
	if err != nil {
		return err
	}
	// A save can overwrite the CURRENT version in place (the mutable-fields
	// path above — e.g. an operator fixing a buggy migration), so versioned
	// cache entries are not safe across it. Bumping unconditionally is
	// simpler than detecting the overwrite path and type writes are rare.
	s.typeCacheEpoch.Add(1)
	return nil
}

// overwriteCurrentVersion replaces the data for the current version of
// a type in place. Used when the Migration field on an existing version
// changes — the schema (the identity-bearing part) stays immutable, but
// the migration (a forward-looking transform hint) is mutable.
func overwriteCurrentVersion(resourceBucket *bolt.Bucket, id []byte, t *cluster.Type) error {
	idBucket := resourceBucket.Bucket(id)
	if idBucket == nil {
		return cluster.ErrResourceNotFound
	}
	currentVer := idBucket.Get(currentKey)
	if currentVer == nil {
		return cluster.ErrResourceNotFound
	}
	verBucket := idBucket.Bucket(versionsBucket)
	if verBucket == nil {
		return cluster.ErrResourceNotFound
	}
	bs, err := t.Marshal()
	if err != nil {
		return err
	}
	return verBucket.Put(currentVer, bs)
}

func (s *Storage) deleteType(id []byte) error {
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		if err := deleteVersioned(b, id); err != nil {
			return err
		}
		// Sweep the per-type-id migration dead-letters (kept outside the config
		// sub-bucket, not a delete-bundle tombstone) so a same-id type recreate starts
		// clean. Same tx as the config delete → atomic.
		return sweepTypeSiblingState(tx, id)
	})
	if err != nil {
		return err
	}
	// The whole version history is gone; cached versioned resolutions must
	// not resurrect it (a reader hitting a residual reference should see
	// ErrTypeMissing — that error is the loud consistency signal).
	s.typeCacheEpoch.Add(1)
	return nil
}

// typeCacheEntry is a typeCache value: the resolved type plus the epoch
// observed BEFORE the bolt read that produced it (see the typeCache field
// doc for the invalidation argument).
type typeCacheEntry struct {
	epoch uint64
	t     *cluster.Type
}

// ResolveType dispatches to the latest or specific-version lookup based
// on ref.Version. Zero (constructed via cluster.LatestTypeRef) means
// "whatever is current"; non-zero means the exact historical version.
//
// Explicit-version lookups are served from typeCache when the entry's
// epoch is current — this is the syncable-reader hot path (one lookup per
// decoded entity), and without the cache each lookup costs a bbolt read
// transaction. The returned *cluster.Type is shared between callers and
// must be treated as immutable — the same contract the built-in system
// types (shared package vars) have always had.
func (s *Storage) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	if ref.Version > 0 {
		epoch := s.typeCacheEpoch.Load()
		if v, ok := s.typeCache.Load(ref); ok {
			if e := v.(typeCacheEntry); e.epoch == epoch {
				return e.t, nil
			}
		}
		t, err := s.typeAtVersion(ref.ID, uint64(ref.Version))
		if err != nil {
			return nil, err
		}
		s.typeCache.Store(ref, typeCacheEntry{epoch: epoch, t: t})
		return t, nil
	}
	return s.latestType(ref.ID)
}

func (s *Storage) latestType(id string) (*cluster.Type, error) {
	t := &cluster.Type{}
	return t, s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := getVersioned(b, []byte(id))
		if err != nil {
			return fmt.Errorf("%w: %s", ErrTypeMissing, id)
		}
		return t.Unmarshal(bs)
	})
}

func (s *Storage) typeAtVersion(id string, version uint64) (*cluster.Type, error) {
	t := &cluster.Type{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := getVersion(b, []byte(id), version)
		if err != nil {
			return err
		}
		return t.Unmarshal(bs)
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *Storage) Types() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}

		return forEachCurrent(b, func(id, data []byte) error {
			cfg, err := typeConfiguration(data)
			if err != nil {
				return err
			}
			cfgs = append(cfgs, cfg)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return cfgs, nil
}

func (s *Storage) TypeVersions(id string) ([]cluster.VersionInfo, error) {
	var versions []cluster.VersionInfo
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var err error
		versions, err = listVersions(b, []byte(id))
		return err
	})
	return versions, err
}

func (s *Storage) TypeVersion(id string, version uint64) (*cluster.Configuration, error) {
	var cfg *cluster.Configuration
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		data, err := getVersion(b, []byte(id), version)
		if err != nil {
			return err
		}
		cfg, err = typeConfiguration(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// typeConfiguration is one stored type's read-back (see
// cluster.Type.Configuration): the document the operator submitted, or
// one synthesized from the stored fields for a type written before the
// document was retained.
func typeConfiguration(data []byte) (*cluster.Configuration, error) {
	tipe := &cluster.Type{}
	if err := tipe.Unmarshal(data); err != nil {
		return nil, err
	}
	return tipe.Configuration()
}
