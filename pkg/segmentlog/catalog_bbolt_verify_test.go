package segmentlog

import (
	"context"
	"errors"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestBoltVerifyRetirementDamage(t *testing.T) {
	for _, damage := range []string{"checksum", "key", "start", "live-closed", "live-active", "temporary"} {
		t.Run(damage, func(t *testing.T) {
			l := newBoltLog(t, 16)
			if err := l.Append([]Record{{0, nil}, {10, nil}}); err != nil {
				t.Fatal(err)
			}
			s := l.catalog.(*boltCatalog)
			c, err := s.Current()
			if err != nil {
				t.Fatal(err)
			}
			item := retiredFile{File: c.Segments[0].File, Start: 0}
			switch damage {
			case "start":
				// Without validating the filename's start, lookup at 100 would
				// miss the selected range and allow deletion of a live file.
				item.Start = 100
			case "live-active":
				item = retiredFile{File: c.Active.File, Start: c.Active.Start}
			case "temporary":
				item.File = ".segmentlog-123"
			}
			value, err := boltEncode(item)
			if err != nil {
				t.Fatal(err)
			}
			key := []byte(item.File)
			switch damage {
			case "checksum":
				value[len(value)-1] ^= 1
			case "key":
				key = []byte("wrong-key")
			}
			if err = s.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(boltRetiredBucket).Put(key, value)
			}); err != nil {
				t.Fatal(err)
			}
			if err = l.Verify(t.Context()); !errors.Is(err, ErrCorrupt) {
				t.Fatal("verification missed retirement damage", err)
			}
			if result, err := l.Reclaim(t.Context()); !errors.Is(err, ErrCorrupt) || result.RemovedFiles != 0 {
				t.Fatal("reclamation accepted retirement damage", result, err)
			}
			// Reopen releases the poisoned handle without clearing the corrupt
			// queue. Reads still demonstrate that no live file was deleted.
			l = reopenBoltLog(t, l)
			for _, id := range []uint64{0, 10} {
				if _, err = l.Read(id); err != nil {
					t.Fatal(id, err)
				}
			}
		})
	}
}

func TestBoltVerifyCompleteRetirementQueue(t *testing.T) {
	l := newBoltLog(t, 16)
	s := l.catalog.(*boltCatalog)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		for i := range uint64(129) {
			name, err := uniqueName("tail", i, ".active")
			if err != nil {
				return err
			}
			if err = retireBoltFile(tx, name, i); err != nil {
				return err
			}
		}
		// Sorts after all valid tail names, beyond a reclamation batch.
		return tx.Bucket(boltRetiredBucket).Put([]byte("zzz"), []byte("damaged"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := l.Verify(t.Context()); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(boltRetiredBucket).Delete([]byte("zzz")) }); err != nil {
		t.Fatal(err)
	}
	if err := l.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := l.Verify(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
