package wal

import (
	"fmt"

	"github.com/philborlin/committed/internal/cluster"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

func (s *Storage) handleType(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteType(e.Key)
	} else {
		t := &cluster.Type{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveType(t)
	}
}

func (s *Storage) saveType(t *cluster.Type) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}

		// Enforce immutability: a (typeID, Version) pair is written once.
		// ProposeType auto-bumps Version when the schema changes and
		// short-circuits when it doesn't, so under normal operation the
		// stored version and the proposed version never collide here. This
		// guard catches Raft replay (the same proposal applied twice) and
		// raw Raft proposals that bypassed ProposeType. We skip rather
		// than error because apply errors are fatal to the state machine.
		existing, err := getVersioned(b, []byte(t.ID))
		if err == nil {
			var prev cluster.Type
			if err := prev.Unmarshal(existing); err == nil && prev.Version == t.Version {
				s.logger.Warn("ignoring type mutation: type+version is immutable",
					zap.String("id", t.ID), zap.Int("version", t.Version))
				return nil
			}
		}

		bs, err := t.Marshal()
		if err != nil {
			return err
		}
		_, err = putVersioned(b, []byte(t.ID), bs)
		return err
	})
}

func (s *Storage) deleteType(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		return deleteVersioned(b, id)
	})
}

// ResolveType dispatches to the latest or specific-version lookup based
// on ref.Version. Zero (constructed via cluster.LatestTypeRef) means
// "whatever is current"; non-zero means the exact historical version.
func (s *Storage) ResolveType(ref cluster.TypeRef) (*cluster.Type, error) {
	if ref.Version > 0 {
		return s.typeAtVersion(ref.ID, uint64(ref.Version))
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
			tipe := &cluster.Type{}
			if err := tipe.Unmarshal(data); err != nil {
				return err
			}

			cfg := &cluster.Configuration{
				ID:       tipe.ID,
				MimeType: "text/toml",
				Data:     []byte(fmt.Sprintf("[type]\nname = \"%s\"", tipe.Name)),
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
	cfg := &cluster.Configuration{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(typeBucket)
		if b == nil {
			return ErrBucketMissing
		}
		data, err := getVersion(b, []byte(id), version)
		if err != nil {
			return err
		}
		// Unmarshal as Type to get the ID/Name, then wrap as Configuration
		// matching the same format used by Types().
		tipe := &cluster.Type{}
		if err := tipe.Unmarshal(data); err != nil {
			return err
		}
		cfg.ID = tipe.ID
		cfg.MimeType = "text/toml"
		cfg.Data = []byte(fmt.Sprintf("[type]\nname = \"%s\"", tipe.Name))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
