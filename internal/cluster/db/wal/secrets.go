package wal

import (
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/config"
)

// validateConfigSecrets re-checks every persisted database, ingestable,
// and syncable config against this node's environment at startup,
// failing fast when a ${VAR} secret reference is unset.
//
// This is deliberately separate from — and stricter than — the lenient
// per-config error handling in restoreIngestableWorkers, where one
// corrupt config must not block the others from running. A missing env
// var is not a single-config defect: it means the operator templated a
// secret and forgot to provide it on this node, which would otherwise
// surface much later as an empty credential and a confusing auth
// failure. Catching it here turns it into a clear, immediate startup
// error.
//
// Only the missing-secret case is treated as fatal. Any other parse
// error (e.g. malformed TOML) is left to the existing load / restore
// paths, which have their own established semantics — this pass must not
// newly reject a config those paths used to tolerate. The check itself
// is side-effect-free: Parser.Validate opens no database connections and
// starts no workers, so it is safe to run before the live restore.
func (s *Storage) validateConfigSecrets() error {
	groups := []struct {
		kind string
		list func() ([]*cluster.Configuration, error)
	}{
		{"database", s.Databases},
		{"ingestable", s.Ingestables},
		{"syncable", s.Syncables},
	}

	for _, g := range groups {
		cfgs, err := g.list()
		if err != nil {
			return fmt.Errorf("validateConfigSecrets: list %ss: %w", g.kind, err)
		}
		for _, cfg := range cfgs {
			err := s.parser.Validate(cfg.MimeType, cfg.Data)
			if err == nil {
				continue
			}
			var missing *config.MissingVarError
			if errors.As(err, &missing) {
				return fmt.Errorf("%s config %q: %w", g.kind, cfg.ID, err)
			}
			s.logger.Warn("validateConfigSecrets: skipping unparseable config",
				zap.String("kind", g.kind),
				zap.String("id", cfg.ID),
				zap.Error(err))
		}
	}

	return nil
}
