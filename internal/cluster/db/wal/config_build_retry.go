package wal

import (
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// degradedBuildRetryInterval paces the self-heal loop for configs that
// validated at admission but failed their node-local build (typically a
// transient destination condition — the field case: an init DDL that hit
// its deadline while the sink was momentarily saturated). Once per
// interval the loop re-pushes a build for each still-degraded id through
// the same pump and build body as the apply path, so a transient failure
// heals without an operator restart; a persistent one re-records its
// loud degraded evidence every attempt (degraded is a condition to fix,
// not to quiet down).
const degradedBuildRetryInterval = time.Minute

// reportNotAdmissible logs the boot/reconcile aggregate the field asked
// for: how many persisted configs of kind are no longer admissible under
// this binary — surfaced the moment the node cycles instead of as
// background log noise discovered during a health check.
func (s *Storage) reportNotAdmissible(kind string) {
	n := 0
	for _, e := range s.ConfigBuildErrors() {
		if e.Kind == kind && e.NotAdmissible {
			n++
		}
	}
	if n > 0 {
		s.logger.Error("persisted configs are no longer admissible under this binary — each needs a fixed re-POST or deletion (see /node/status for the list)",
			zap.String("kind", kind), zap.Int("count", n))
	}
}

// retryDegradedBuildsLoop runs on its own goroutine from Open until
// close, ticking retryDegradedBuilds. Started only when a pump exists
// (a Storage opened without listener channels has nowhere to push).
func (s *Storage) retryDegradedBuildsLoop() {
	t := time.NewTicker(degradedBuildRetryInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.retryDegradedBuilds()
		case <-s.closeC:
			return
		}
	}
}

// retryDegradedBuilds re-queues a build for every syncable/ingestable id
// currently carrying degraded evidence, reading the CURRENT persisted
// bytes — a config replaced since it degraded retries as its
// replacement, and a deleted one is no longer listed (its record is
// swept separately). Undecodable configs are skipped: no rebuild can
// help bytes that don't decode; only a re-POST clears those. Database
// configs have no pump of their own — a degraded database heals through
// the retry of any syncable/ingestable that references it (their parse
// re-resolves it) or on restart.
func (s *Storage) retryDegradedBuilds() {
	degraded := map[string]map[string]bool{}
	for _, e := range s.ConfigBuildErrors() {
		if e.NotAdmissible {
			// A deterministic admission rejection can never self-heal —
			// the config was invalidated by this binary's rules (the field
			// zombie retried one every 60s forever). It stays loudly
			// degraded in /status until an operator re-POSTs or deletes.
			continue
		}
		if degraded[e.Kind] == nil {
			degraded[e.Kind] = map[string]bool{}
		}
		degraded[e.Kind][e.ID] = true
	}
	if len(degraded["syncable"]) > 0 && s.syncPump != nil {
		raws, _, err := s.listRawConfigs(syncableBucket)
		if err != nil {
			s.logger.Warn("degraded-build retry: list syncables", zap.Error(err))
		} else {
			for _, r := range raws {
				if !degraded["syncable"][r.id] || r.decodeErr != nil {
					continue
				}
				cfg := r.cfg
				s.logger.Info("retrying degraded syncable build", zap.String("id", r.id))
				s.syncPump.push(&db.SyncableWithID{ID: r.id, Build: func() cluster.Syncable {
					return s.buildSyncable(cfg)
				}})
			}
		}
	}
	if len(degraded["ingestable"]) > 0 && s.ingestPump != nil {
		raws, _, err := s.listRawConfigs(ingestableBucket)
		if err != nil {
			s.logger.Warn("degraded-build retry: list ingestables", zap.Error(err))
			return
		}
		for _, r := range raws {
			if !degraded["ingestable"][r.id] || r.decodeErr != nil {
				continue
			}
			cfg := r.cfg
			s.logger.Info("retrying degraded ingestable build", zap.String("id", r.id))
			s.ingestPump.push(&db.IngestableWithID{ID: r.id, Build: func() cluster.Ingestable {
				return s.buildIngestable(cfg)
			}})
		}
	}
}
