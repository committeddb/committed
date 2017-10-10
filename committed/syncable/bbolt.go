package syncable

import (
	"context"

	"github.com/coreos/bbolt"
)

// BBolt represents a sync structure for the BBolt datastore
type BBolt struct {
	Db      *bolt.DB
	KeyName string
	Bucket  string
}

// NewBBolt creates a new BBolt
func NewBBolt(dbName string, KeyName string, bucketName string) (*BBolt, error) {
	db, err := bolt.Open(dbName, 0600, nil)
	if err != nil {
		return nil, err
	}

	bbolt := &BBolt{Db: db, KeyName: KeyName, Bucket: bucketName}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return nil
	})

	return bbolt, nil
}

// Sync performs a sync operation on a BBolt database
func (b *BBolt) Sync(ctx context.Context, bytes []byte) error {
	// TODO We need to use the keyName to parse the key from the bytes
	key := "key"

	b.Db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(b.Bucket))
		err := bucket.Put([]byte(key), bytes)
		return err
	})

	return nil
}

// Close closes the underlying database
func (b *BBolt) Close() error {
	return b.Db.Close()
}
