package db

import (
	"context"
	"time"
)

// lagScrapeTimeout bounds the per-ingestable source query the ingest-lag gauge
// makes at collection time, so a slow or unreachable source can't stall a
// /metrics scrape.
const lagScrapeTimeout = 3 * time.Second

// SyncableLags implements metrics.LagProvider: the raft-index lag of each
// configured syncable (head − checkpoint, clamped to ≥ 0). Cheap — both indices
// are local apply state, the same the GET /v1/syncable/{id}/status handler
// reports. Reported for every configured syncable, including ones not running
// on this node (the checkpoint is replicated; the head is local apply state).
func (db *DB) SyncableLags() map[string]uint64 {
	out := map[string]uint64{}
	cfgs, err := db.Syncables()
	if err != nil {
		return out
	}
	for _, c := range cfgs {
		checkpoint, head, err := db.SyncableProgress(c.ID)
		if err != nil {
			continue
		}
		lag := uint64(0)
		if head > checkpoint {
			lag = head - checkpoint
		}
		out[c.ID] = lag
	}
	return out
}

// IngestableLags implements metrics.LagProvider: each configured ingestable's
// source byte lag, or nil when unknown (MySQL has no scalar binlog distance,
// the worker is mid-snapshot, or the source was unreachable). Mirrors
// GET /v1/ingestable/{id}/status — it makes the same source query, bounded by
// lagScrapeTimeout per ingestable. An ingestable not configured on this node is
// skipped (no entry).
func (db *DB) IngestableLags() map[string]*uint64 {
	out := map[string]*uint64{}
	cfgs, err := db.Ingestables()
	if err != nil {
		return out
	}
	for _, c := range cfgs {
		ctx, cancel := context.WithTimeout(context.Background(), lagScrapeTimeout)
		st, err := db.IngestableStatus(ctx, c.ID)
		cancel()
		if err != nil {
			continue
		}
		out[c.ID] = st.Lag
	}
	return out
}
