package segmentlog

import bolt "go.etcd.io/bbolt"

// publishReset retires the old selection and installs an empty history in one
// transaction. Only metadata is visited; payloads are not read or transformed.
func (s *boltCatalog) publishReset(next Catalog) error {
	if err := validateBoltHeader(boltHeader{Version: 1, Catalog: next}); err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		head, err := readBoltHeader(tx)
		if err != nil {
			return err
		}
		old := head.Catalog
		if old.Revision == ^uint64(0) || next.Revision != old.Revision+1 || next.History == old.History || next.Start != old.Start || next.Generation != old.Generation || next.SegmentBytes != old.SegmentBytes || next.Active.File == old.Active.File || next.Active.Checkpoint != nil {
			return ErrInvalid
		}
		if err := tx.Bucket(boltRangesBucket).ForEach(func(k, v []byte) error {
			ref, err := decodeBoltRef(k, v)
			if err != nil {
				return err
			}
			return retireBoltFile(tx, ref.File, ref.Coverage.Start)
		}); err != nil {
			return err
		}
		if err := retireBoltFile(tx, old.Active.File, old.Active.Start); err != nil {
			return err
		}
		if err := tx.DeleteBucket(boltRangesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(boltRangesBucket); err != nil {
			return err
		}
		return putBoltHeader(tx, boltHeader{Version: 1, Catalog: next})
	})
}
