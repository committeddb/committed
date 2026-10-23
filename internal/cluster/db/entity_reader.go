package db

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/migration"
)

// scrubBoundReporter is the optional Storage extension naming the scrub
// bound this node's event log content reflects (wal.Storage implements it
// and scrubProgressReporter; the in-memory test doubles implement neither,
// which makes every missing entity an error there).
type scrubBoundReporter interface {
	EventLogGeneration() uint64
}

// scrubbedThrough is the highest raft index a scrub may have erased entities
// at: the highest committed Scrub bound, which is recorded before its rewrite
// starts (a rewrite's content becomes readable before its bound is
// published as the log's generation), or the generation of content adopted
// from a peer, whichever is higher.
func (db *DB) scrubbedThrough() uint64 {
	var bound uint64
	if r, ok := db.storage.(scrubProgressReporter); ok {
		bound, _ = r.ScrubProgress()
	}
	if r, ok := db.storage.(scrubBoundReporter); ok {
		bound = max(bound, r.EventLogGeneration())
	}
	return bound
}

// entityReader is the engine's cluster.EntityReader: a point read of the
// permanent event log.
type entityReader struct {
	db *DB
}

// handEntityReader gives s the engine's entity reader if it reads entities
// back (a staged projection keeping pointers to its inputs).
func (db *DB) handEntityReader(s cluster.Syncable) {
	if r, ok := cluster.SyncableAs[cluster.EntityReaderAware](s); ok {
		r.SetEntityReader(&entityReader{db: db})
	}
}

func (r *entityReader) ReadEntity(index uint64, typeID string, key []byte, from, to int) ([]byte, bool, error) {
	a, err := r.db.storage.ActualAt(index)
	if err != nil && !errors.Is(err, ErrActualNotFound) {
		return nil, false, fmt.Errorf("read the entry at index %d: %w", index, err)
	}
	var last *cluster.Entity
	if a != nil {
		for _, e := range a.Entities {
			if e.Type != nil && e.ID == typeID && bytes.Equal(e.Key, key) {
				last = e
			}
		}
	}
	if last == nil {
		// The pointer's entity is gone. A scrub at or past index removes an
		// entity only when a later entry for its key deletes or supersedes it,
		// an entry the reader has yet to fold; until then the input is absent,
		// as a fold over the scrubbed log would have it. Above every scrub's
		// bound nothing could have removed it.
		if index <= r.db.scrubbedThrough() {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("the %s entity a stage input points to is missing from the entry at index %d, which no scrub has reached (store inconsistency)", typeID, index)
	}
	if last.Variant() != cluster.EntityVariantRow {
		return nil, false, fmt.Errorf("the %s entity a stage input points to at index %d is not a row (store inconsistency)", typeID, index)
	}
	data := last.Data
	if from != 0 && from < to {
		if data, err = migration.Chain(r.db.ctx, r.db.storage, typeID, from, to, data); err != nil {
			return nil, false, fmt.Errorf("migrate the %s entity at index %d from version %d to %d: %w", typeID, index, from, to, err)
		}
	}
	return data, true, nil
}
