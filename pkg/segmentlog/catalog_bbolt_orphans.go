package segmentlog

import (
	"context"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
)

// dataFileStart recognizes only engine-generated data names, not manifests.
func dataFileStart(name string) (uint64, bool) {
	if !managedArtifact(name) {
		return 0, false
	}
	var digits string
	switch {
	case strings.HasPrefix(name, "segment-"):
		digits = name[len("segment-") : len("segment-")+20]
	case strings.HasPrefix(name, "tail-"):
		digits = name[len("tail-") : len("tail-")+20]
	default:
		return 0, false
	}
	start, err := strconv.ParseUint(digits, 10, 64)
	return start, err == nil
}

// Verify the filename-to-range invariant across the entire selection before
// using point lookups to prove absence. This also rejects gaps, extra entries,
// incorrect counts, and damaged metadata before any file is deleted. No payload
// reads or full in-memory live-name set are needed.
func (s *boltCatalog) validateSweep(ctx context.Context) (head Catalog, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		h, e := readBoltHeader(tx)
		if e != nil {
			return e
		}
		head = h.Catalog
		if start, ok := dataFileStart(head.Active.File); !ok || start != head.Active.Start {
			return ErrCorrupt
		}
		next, count := head.Start, uint64(0)
		c := tx.Bucket(boltRangesBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if e = ctx.Err(); e != nil {
				return e
			}
			ref, e := decodeBoltRef(k, v)
			if e != nil {
				return e
			}
			if ref.Coverage.Start != next || ref.Coverage.End > head.Active.Start {
				return ErrCorrupt
			}
			if ref.File != "" {
				if start, ok := dataFileStart(ref.File); !ok || start != ref.Coverage.Start {
					return ErrCorrupt
				}
			}
			next = ref.Coverage.End
			count++
		}
		if next != head.Active.Start || count != h.Ranges {
			return ErrCorrupt
		}
		return nil
	})
	return head, err
}

func (s *boltCatalog) reclaimOrphans(l *Log, ctx context.Context) (result ReclaimResult, err error) {
	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = l.fail(err)
		}
	}()
	head, err := s.validateSweep(ctx)
	if err != nil {
		return result, err
	}
	dir, err := os.Open(s.path) // #nosec G304 G703 -- Exclusively owned, caller-selected log directory; no filename input is joined here.
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, dir.Close()) }()
	type candidate struct {
		name string
		size int64
	}
	for {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		entries, readErr := dir.ReadDir(128)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return result, readErr
		}
		batch := make([]candidate, 0, len(entries))
		err = s.db.View(func(tx *bolt.Tx) error {
			for _, entry := range entries {
				if e := ctx.Err(); e != nil {
					return e
				}
				name := entry.Name()
				if name == head.Active.File || name == boltCatalogName {
					continue
				}
				start, data := dataFileStart(name)
				temporary := strings.HasPrefix(name, ".segmentlog-") && managedArtifact(name)
				if !data && !temporary {
					result.SkippedEntries++
					continue
				}
				if data {
					key := rangeKey(start)
					if value := tx.Bucket(boltRangesBucket).Get(key); value != nil {
						ref, e := decodeBoltRef(key, value)
						if e != nil {
							return e
						}
						if ref.File == name {
							continue
						}
					}
				}
				info, e := entry.Info()
				if e != nil {
					return e
				}
				if !info.Mode().IsRegular() {
					result.SkippedEntries++
					continue
				}
				if info.Size() < 0 {
					return ErrCorrupt
				}
				batch = append(batch, candidate{name, info.Size()})
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		for _, item := range batch {
			if err = ctx.Err(); err != nil {
				return result, err
			}
			removed, e := l.remover.Remove(item.name)
			if removed {
				result.RemovedFiles++
				result.RemovedBytes += uint64(item.size) // #nosec G115 -- Checked nonnegative regular-file size above.
			}
			if e != nil {
				return result, e
			}
		}
		if errors.Is(readErr, io.EOF) {
			// Also confirm prior uncertain removals whose names are now absent.
			durable, e := durablefs.Open(s.path)
			if e != nil {
				return result, e
			}
			return result, durable.Sync()
		}
	}
}
