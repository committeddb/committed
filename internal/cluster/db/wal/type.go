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

		// Enforce immutability: if a type with this ID already exists and
		// its Version field matches, skip silently. The preflight check in
		// ProposeType rejects this at the API boundary with a clear error;
		// this apply-side guard is a safety net for proposals that bypass
		// the preflight (e.g. raw Raft proposals). We skip rather than
		// error because apply errors are fatal to the Raft state machine.
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

func (s *Storage) Type(id string) (*cluster.Type, error) {
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
