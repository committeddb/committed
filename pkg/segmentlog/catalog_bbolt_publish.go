package segmentlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
)

type retiredFile struct {
	File  string
	Start uint64
}

func retireBoltFile(tx *bolt.Tx, file string, start uint64) error {
	if file == "" {
		return nil
	}
	if fileStart, ok := dataFileStart(file); !ok || fileStart != start {
		return ErrInvalid
	}
	b, err := boltEncode(retiredFile{file, start})
	if err != nil {
		return err
	}
	return tx.Bucket(boltRetiredBucket).Put([]byte(file), b)
}

func (s *boltCatalog) publishRollover(p *preparedRollover) error {
	if p == nil || p.consumed || p.file == nil || p.tail == nil {
		return ErrInvalid
	}
	p.consumed = true
	return s.update(func(tx *bolt.Tx) error {
		h, err := readBoltHeader(tx)
		if err != nil {
			return err
		}
		c := &h.Catalog
		if p.path != s.path || p.history != c.History || p.revision != c.Revision || p.source != c.Active.File {
			return ErrCatalogConflict
		}
		if c.Revision == ^uint64(0) || p.closed.Coverage.Start != c.Active.Start || p.active.Start != p.closed.Coverage.End {
			return ErrInvalid
		}
		if tx.Bucket(boltRangesBucket).Get(rangeKey(p.closed.Coverage.Start)) != nil {
			return ErrCatalogConflict
		}
		if err = putBoltRef(tx, p.closed); err != nil {
			return err
		}
		if p.closed.File == "" {
			if err = retireBoltFile(tx, c.Active.File, c.Active.Start); err != nil {
				return err
			}
		}
		active := p.active
		c.Active = &active
		c.Revision++
		h.Ranges++
		return putBoltHeader(tx, h)
	})
}

// verifiedRewriteFiles is an in-memory, single-use preparation result. The
// maintenance owner keeps these private files stable until catalog publication.
type verifiedRewriteFiles struct {
	owner    *boltCatalog
	changed  []SegmentRef
	active   *TailRef
	consumed bool
}

// verifyRewrite does file I/O only; it neither reads nor changes catalog state.
// The managed caller may release Log.mu while retaining maintenance ownership.
func (s *boltCatalog) verifyRewrite(changed []SegmentRef, active *TailRef) (*verifiedRewriteFiles, error) {
	// Establish integrity and durability before metadata selection.
	// Only changed immutable files and a replacement active tail are checked.
	if err := verifyCatalogFiles(s.path, Catalog{Segments: changed, Active: active}); err != nil {
		return nil, err
	}
	if len(changed) > 0 || active != nil {
		dir, err := durablefs.Open(s.path)
		if err != nil {
			return nil, err
		}
		if err = dir.Sync(); err != nil {
			return nil, err
		}
	}
	return &verifiedRewriteFiles{owner: s, changed: changed, active: active}, nil
}

// publishRewrite selects only files returned by verifyRewrite. A nil active
// leaves the current tail and its checkpoint unchanged. No payload I/O occurs.
func (s *boltCatalog) publishRewrite(expected, generation uint64, files *verifiedRewriteFiles) error {
	return s.publishReplacement(expected, generation, files, false)
}

// publishCompression changes physical representation without advancing logical history.
func (s *boltCatalog) publishCompression(expected uint64, files *verifiedRewriteFiles) error {
	return s.publishReplacement(expected, 0, files, true)
}

func (s *boltCatalog) publishReplacement(expected, generation uint64, files *verifiedRewriteFiles, compression bool) error {
	if files == nil || files.consumed || files.owner != s {
		return ErrInvalid
	}
	files.consumed = true
	changed, active := files.changed, files.active
	c, err := s.head()
	if err != nil {
		return err
	}
	if c.Revision != expected {
		return ErrCatalogConflict
	}
	if compression {
		if active != nil || len(changed) != 1 {
			return ErrInvalid
		}
		generation = c.Generation
	}
	if expected == ^uint64(0) || (!compression && generation <= c.Generation) || (active != nil && active.Start != c.Active.Start) {
		return ErrInvalid
	}

	return s.update(func(tx *bolt.Tx) error {
		h, e := readBoltHeader(tx)
		if e != nil {
			return e
		}
		if h.Catalog.Revision != expected {
			return ErrCatalogConflict
		}
		b := tx.Bucket(boltRangesBucket)
		for _, ref := range changed {
			k := rangeKey(ref.Coverage.Start)
			old, e := decodeBoltRef(k, b.Get(k))
			if e != nil {
				return e
			}
			if compression && (old.TailBytes == 0 || ref.TailBytes != 0 || ref.Count != old.Count) {
				return ErrInvalid
			}
			if ref.Coverage != old.Coverage || ref.File == old.File {
				return ErrInvalid
			}
			if e = putBoltRef(tx, ref); e != nil {
				return e
			}
			if e = retireBoltFile(tx, old.File, old.Coverage.Start); e != nil {
				return e
			}
		}
		if active != nil && active.File != h.Catalog.Active.File {
			if e = retireBoltFile(tx, h.Catalog.Active.File, h.Catalog.Active.Start); e != nil {
				return e
			}
		}
		if active != nil {
			h.Catalog.Active = active
		}
		h.Catalog.Revision++
		h.Catalog.Generation = generation
		return putBoltHeader(tx, h)
	})
}

// reclaim drains committed retirements in bounded metadata batches. It never
// lists the directory or guesses ownership of unpublished orphan files.
func (s *boltCatalog) reclaim(l *Log, ctx context.Context) (result ReclaimResult, err error) {
	for {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		var batch []retiredFile
		err = s.db.View(func(tx *bolt.Tx) error {
			h, e := readBoltHeader(tx)
			if e != nil {
				return e
			}
			cursor := tx.Bucket(boltRetiredBucket).Cursor()
			for k, v := cursor.First(); k != nil && len(batch) < 128; k, v = cursor.Next() {
				item, e := decodeRetiredFile(tx, h.Catalog, k, v)
				if e != nil {
					return e
				}
				batch = append(batch, item)
			}
			return nil
		})
		if err != nil {
			return result, l.fail(err)
		}
		if len(batch) == 0 {
			return result, nil
		}
		for _, item := range batch {
			if err = ctx.Err(); err != nil {
				return result, err
			}
			info, e := os.Lstat(filepath.Join(s.path, item.File)) // #nosec G703 -- decodeRetiredFile validates the queued basename before it enters this batch.
			var size int64
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				return result, l.fail(e)
			}
			if e == nil {
				if !info.Mode().IsRegular() || info.Size() < 0 {
					return result, l.fail(ErrCorrupt)
				}
				size = info.Size()
			}
			// Remove also syncs the directory when the name is already absent, making
			// retries safe after deletion succeeded but queue acknowledgement failed.
			removed, e := l.remover.Remove(item.File)
			if removed {
				result.RemovedFiles++
				result.RemovedBytes += uint64(size)
			} // #nosec G115 -- Size is zero or a checked nonnegative regular-file size above.
			if e != nil {
				return result, l.fail(e)
			}
		}
		err = s.update(func(tx *bolt.Tx) error {
			b := tx.Bucket(boltRetiredBucket)
			for _, item := range batch {
				if e := b.Delete([]byte(item.File)); e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			return result, l.fail(err)
		}
	}
}
