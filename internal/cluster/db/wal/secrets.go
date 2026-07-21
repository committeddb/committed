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
				s.clearConfigError(g.kind, cfg.ID, configErrValidate)
				continue
			}
			s.recordConfigError(g.kind, cfg.ID, configErrValidate, err)
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

// configErrEvidence ranks how a config's buildability was checked.
// configErrValidate is Parser.Validate — a side-effect-free prediction.
// configErrBuild is the live-object construction (ParseDatabase and friends)
// — the fact. A success clears only records at or below its own strength:
// build success clears everything; validate success never erases a build
// failure. Without the ranking, RestoreSnapshot's synchronous build failure
// was cleared by refreshAfterRestore's ASYNC validateConfigSecrets whenever
// Validate passed but the build didn't (a driver-level constructor error),
// leaving the handle missing while the gauge read healthy.
type configErrEvidence int

const (
	configErrValidate configErrEvidence = iota
	configErrBuild
)

// configErr is one degraded-config record: the failure and the strongest
// evidence class that observed it.
type configErr struct {
	err      error
	evidence configErrEvidence
}

// recordConfigError marks a config as failed-to-build on this node — a
// node-local condition (missing ${VAR} secret, parse error). The raw
// config bytes are persisted regardless; only the live-object
// construction is deferred, so the node stays in quorum instead of
// fatal-exiting. A later successful build clears the entry. A re-record
// keeps the strongest evidence seen: a validate failure on a config whose
// build already failed must still require a build success to clear.
func (s *Storage) recordConfigError(kind, id string, evidence configErrEvidence, err error) {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	key := kind + "/" + id
	if prev, ok := s.configErrors[key]; ok && prev.evidence > evidence {
		evidence = prev.evidence
	}
	s.configErrors[key] = configErr{err: err, evidence: evidence}
}

// clearConfigError removes a config's recorded build error after a success
// of the given evidence strength (idempotent — a no-op if none was
// recorded). A record survives a success of weaker evidence than the
// failure that created it.
func (s *Storage) clearConfigError(kind, id string, evidence configErrEvidence) {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	key := kind + "/" + id
	if prev, ok := s.configErrors[key]; ok && prev.evidence <= evidence {
		delete(s.configErrors, key)
	}
}

// sweepConfigErrorsExcept deletes every degraded-config record of kind whose
// id is NOT in present — the ids remaining in the config bucket. A deleted
// config's record must not outlive it (nothing re-checks a deleted id), and
// the apply-path delete clear cannot cover a delete that arrived compacted
// inside an InstallSnapshot; the reconcile/load paths call this with the
// fresh bucket contents instead.
func (s *Storage) sweepConfigErrorsExcept(kind string, present map[string]struct{}) {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()
	prefix := kind + "/"
	for key := range s.configErrors {
		id, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		if _, live := present[id]; !live {
			delete(s.configErrors, key)
		}
	}
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
// The error string routes through cluster.RedactedMessage — the shared sink
// redaction choke point. A missing-${VAR} or parse error is committed-authored
// and passes through verbatim (naming the failing var, never an interpolated
// value — interpolation failed, so no secret exists). A post-interpolation
// driver error that reached here and implements RedactedError (e.g. one echoing
// the resolved connection string) is reduced to its PII-free classifier, so this
// replicated-status surface can't leak a secret even as new error sources appear.
func (s *Storage) ConfigBuildErrors() []cluster.ConfigBuildError {
	s.configErrMu.Lock()
	defer s.configErrMu.Unlock()

	out := make([]cluster.ConfigBuildError, 0, len(s.configErrors))
	for key, rec := range s.configErrors {
		// Keys are "kind/id" (recordConfigError); kind is one of
		// database/ingestable/syncable and never contains a slash, so the
		// first separator splits them unambiguously.
		kind, id, _ := strings.Cut(key, "/")
		msg, _ := cluster.RedactedMessage(rec.err)
		out = append(out, cluster.ConfigBuildError{Kind: kind, ID: id, Error: msg})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].ID < out[j].ID
	})
	return out
}
