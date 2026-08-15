package wal

import (
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
)

// handleIngestableCensus applies the JSON shape census record the ingest
// worker publishes during its snapshot pass (see cluster.IngestableCensus).
// Keyed by ingestable id, last-writer-wins. Guarded on the ingestable's
// config still existing — the write-guard half of the delete-safety contract
// (config_sibling_state.go): a census published just after the ingestable's
// delete must not linger for a same-id recreate to serve as its own. The
// sweep half rides sweepIngestableSiblingState.
func (s *Storage) handleIngestableCensus(e *cluster.Entity, _ uint64) error {
	if e.IsDelete() {
		return s.update(func(tx *bolt.Tx) error {
			b := tx.Bucket(ingestableCensusBucket)
			if b == nil {
				return ErrBucketMissing
			}
			return b.Delete(e.Key)
		})
	}
	return s.putConfigGuardedKeyed(ingestableBucket, ingestableCensusBucket, e.Key, e.Data)
}

// IngestableCensus implements the db.Storage read: the latest applied census
// for the ingestable, or (nil, false) when none has been published.
func (s *Storage) IngestableCensus(id string) (*cluster.IngestableCensus, bool) {
	var raw []byte
	_ = s.view(func(tx *bolt.Tx) error {
		if b := tx.Bucket(ingestableCensusBucket); b != nil {
			if v := b.Get([]byte(id)); v != nil {
				raw = append([]byte{}, v...)
			}
		}
		return nil
	})
	if raw == nil {
		return nil, false
	}
	c := &cluster.IngestableCensus{}
	if err := c.Unmarshal(raw); err != nil {
		s.logger.Warn("[wal.ingestable-census] unmarshal failed", zap.String("id", id), zap.Error(err))
		return nil, false
	}
	return c, true
}
