package wal

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/db"
)

func (s *Storage) handleIngestable(e *cluster.Entity) error {
	if e.IsDelete() {
		return s.deleteIngestable(e.Key)
	} else {
		t := &cluster.Configuration{}
		err := t.Unmarshal(e.Data)
		if err != nil {
			return err
		}
		return s.saveIngestable(t)
	}
}

// saveIngestable persists an ingestable Configuration as a new version in
// bbolt and then notifies the consumer channel. See saveSyncable for the
// rationale on why the channel send happens outside the bbolt Update closure.
func (s *Storage) saveIngestable(t *cluster.Configuration) error {
	var ingestable cluster.Ingestable
	var built bool
	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		bs, err := t.Marshal()
		if err != nil {
			return fmt.Errorf("[wal.ingestable] marshal: %w", err)
		}

		// Deterministic state-machine write FIRST: persist the raw config
		// bytes so every replica converges, then attempt the node-local
		// build (which can fail on a missing ${VAR} secret).
		if _, err := putVersioned(b, []byte(t.ID), bs); err != nil {
			return fmt.Errorf("[wal.ingestable] putVersioned: %w", err)
		}

		_, parsed, err := s.parser.ParseIngestable(t.MimeType, t.Data)
		if err != nil {
			// Degrade rather than fail the apply (which would crash the
			// node). The config is persisted; no worker is started until
			// the build succeeds.
			s.recordConfigError("ingestable", t.ID, err)
			s.logger.Error("ingestable config persisted but could not be built on this node (degraded); fix the environment and the config will build on next restart",
				zap.String("id", t.ID), zap.Error(err))
			return nil
		}
		s.clearConfigError("ingestable", t.ID)
		ingestable = parsed
		built = true

		return nil
	})
	if err != nil {
		return err
	}

	if built && s.ingest != nil {
		s.ingest <- &db.IngestableWithID{ID: t.ID, Ingestable: ingestable}
	}

	return nil
}

func (s *Storage) deleteIngestable(id []byte) error {
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		// NB: unlike deleteDatabase there is no in-memory map to clear —
		// ingestables aren't cached on the Storage (they're handed to the
		// supervisor via the ingest channel). The previous
		// `s.databases[id] = nil` here was a copy-paste leftover from
		// deleteDatabase that could nil out a live database connection if
		// an ingestable and database happened to share an id.
		return deleteVersioned(b, id)
	})
}

// restoreIngestableWorkers walks the ingestable bucket and re-sends
// each registered ingestable to the supervisor's ingest channel so a
// restarted node spawns workers for them. See the call site comment in
// Open for the apply-path/restart-replay ordering this fixes.
//
// Errors here are warnings, not fatals: a corrupted single config
// shouldn't stop the rest from running. The dialect will surface a
// real connection or schema error in its own retry loop later.
func (s *Storage) restoreIngestableWorkers() {
	if s.ingest == nil {
		return
	}
	cfgs, err := s.Ingestables()
	if err != nil {
		s.logger.Warn("restoreIngestableWorkers: list ingestables", zap.Error(err))
		return
	}
	for _, cfg := range cfgs {
		_, ingestable, err := s.parser.ParseIngestable(cfg.MimeType, cfg.Data)
		if err != nil {
			// Degraded: record so the build-errors gauge reflects the
			// build path too (validateConfigSecrets uses the cheaper
			// Validate; a config can pass that but fail the full parse).
			s.recordConfigError("ingestable", cfg.ID, err)
			s.logger.Warn("restoreIngestableWorkers: parse (degraded)",
				zap.String("id", cfg.ID), zap.Error(err))
			continue
		}
		s.clearConfigError("ingestable", cfg.ID)
		s.ingest <- &db.IngestableWithID{ID: cfg.ID, Ingestable: ingestable}
	}
}

func (s *Storage) Ingestables() ([]*cluster.Configuration, error) {
	var cfgs []*cluster.Configuration

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}

		return forEachCurrent(b, func(id, data []byte) error {
			cfg := &cluster.Configuration{}
			if err := cfg.Unmarshal(data); err != nil {
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

func (s *Storage) IngestableVersions(id string) ([]cluster.VersionInfo, error) {
	var versions []cluster.VersionInfo
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		var err error
		versions, err = listVersions(b, []byte(id))
		return err
	})
	return versions, err
}

func (s *Storage) IngestableVersion(id string, version uint64) (*cluster.Configuration, error) {
	cfg := &cluster.Configuration{}
	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(ingestableBucket)
		if b == nil {
			return ErrBucketMissing
		}
		data, err := getVersion(b, []byte(id), version)
		if err != nil {
			return err
		}
		return cfg.Unmarshal(data)
	})
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
