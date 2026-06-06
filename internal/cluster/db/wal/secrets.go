package wal

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/config"
)

// validateConfigSecrets re-checks every persisted database, ingestable,
// and syncable config against this node's environment at startup and
// RECORDS — rather than fatal-exits on — any that can't be built here
// (a missing ${VAR} secret, or any other parse error).
//
// This used to fatal-exit the node on a missing secret. That made a
// node-local env gap (operator rolled a new ${SECRET} to some nodes but
// not all) take the node out of quorum, and it was inconsistent with the
// live apply path, which now degrades. So both paths degrade: the raw
// config is valid and persisted cluster-wide; this node simply can't
// construct the live object until the env is fixed. We mark it
// (surfaced via the committed_config_build_errors gauge and a loud log)
// and keep the node serving. The load / restore paths below skip the
// unbuildable config; a fixed env builds it on the next restart.
//
// Parser.Validate is side-effect-free (opens no connections, starts no
// workers), so running it before the live restore is safe. Returning an
// error from here still aborts startup, but only for genuine infra
// failures (listing the configs out of bbolt) — never for a config's
// own content.
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
				s.clearConfigError(g.kind, cfg.ID)
				continue
			}
			s.recordConfigError(g.kind, cfg.ID, err)
			var missing *config.MissingVarError
			if errors.As(err, &missing) {
				s.logger.Error("config persisted but a required ${VAR} secret is unset on this node (degraded); the node stays in quorum, fix the environment and restart to build it",
					zap.String("kind", g.kind),
					zap.String("id", cfg.ID),
					zap.Error(err))
			} else {
				s.logger.Warn("config could not be parsed on this node (degraded)",
					zap.String("kind", g.kind),
					zap.String("id", cfg.ID),
					zap.Error(err))
			}
		}
	}

	return nil
}

// recordConfigError marks a config as failed-to-build on this node — a
// node-local condition (missing ${VAR} secret, parse error). The raw
// config bytes are persisted regardless; only the live-object
// construction is deferred, so the node stays in quorum instead of
// fatal-exiting. A later successful build clears the entry.
func (s *Storage) recordConfigError(kind, id string, err error) {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	s.configErrors[kind+"/"+id] = err
}

// clearConfigError removes a config's recorded build error after a
// successful build (idempotent — a no-op if none was recorded).
func (s *Storage) clearConfigError(kind, id string) {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	delete(s.configErrors, kind+"/"+id)
}

// ConfigBuildErrorCount returns how many configs are currently degraded
// (persisted but not buildable on this node — usually a missing secret).
// db.DB reads this to emit the committed_config_build_errors gauge so the
// condition is visible without the node having left quorum.
func (s *Storage) ConfigBuildErrorCount() int {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	return len(s.configErrors)
}

// ConfigBuildErrors returns a snapshot of the configs currently degraded
// on this node (persisted but not buildable here — usually a missing
// ${VAR} secret), sorted by kind then id so the response is stable. Where
// ConfigBuildErrorCount gives the gauge its "how many", this gives GET
// /node/status its "which, and why".
//
// The error string is taken verbatim from the recorded build error, which
// names the failing ${VAR} but never an interpolated value — interpolation
// failed, so no secret value exists to leak.
func (s *Storage) ConfigBuildErrors() []cluster.ConfigBuildError {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()

	out := make([]cluster.ConfigBuildError, 0, len(s.configErrors))
	for key, err := range s.configErrors {
		// Keys are "kind/id" (recordConfigError); kind is one of
		// database/ingestable/syncable and never contains a slash, so the
		// first separator splits them unambiguously.
		kind, id, _ := strings.Cut(key, "/")
		out = append(out, cluster.ConfigBuildError{Kind: kind, ID: id, Error: err.Error()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out
}
