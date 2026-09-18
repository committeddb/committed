package segmentlog

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

// verifyMetadata checks logical metadata invariants, not arbitrary damage to
// bbolt's underlying pages. The Log mutex prevents changes between transactions.
func (s *boltCatalog) verifyMetadata(ctx context.Context) (Catalog, error) {
	if s.poison != nil {
		return Catalog{}, s.poison
	}
	head, err := s.validateSelection(ctx)
	if err != nil {
		return head, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(boltRetiredBucket).Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if _, err := decodeRetiredFile(tx, head, k, v); err != nil {
				return err
			}
		}
		return nil
	})
	return head, err
}

// A retirement record must identify a generated data filename at its original
// range start, and cannot name a selected file. Missing retired files are valid:
// deletion may have completed before queue acknowledgement or an orphan sweep.
func decodeRetiredFile(tx *bolt.Tx, head Catalog, key, value []byte) (item retiredFile, err error) {
	if err = boltDecode(value, &item); err != nil {
		return item, err
	}
	start, ok := dataFileStart(item.File)
	if !ok || start != item.Start || string(key) != item.File || item.File == head.Active.File {
		return item, ErrCorrupt
	}
	rkey := rangeKey(item.Start)
	if raw := tx.Bucket(boltRangesBucket).Get(rkey); raw != nil {
		ref, e := decodeBoltRef(rkey, raw)
		if e != nil {
			return item, e
		}
		if ref.File == item.File {
			return item, ErrCorrupt
		}
	}
	return item, nil
}
