package segmentlog

import (
	"io"

	bolt "go.etcd.io/bbolt"
)

func (s *boltCatalog) backup(w io.Writer) error {
	return s.db.View(func(tx *bolt.Tx) error { _, err := tx.WriteTo(w); return err })
}

// openBackupCatalog reads the private copy without touching the live catalog.
func openBackupCatalog(path string) (layout, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &boltCatalog{db: db}, nil
}
