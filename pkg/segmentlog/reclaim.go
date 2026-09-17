package segmentlog

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ReclaimResult counts names removed by this call and their logical file sizes.
// RemovedBytes is not a measurement of freed filesystem blocks (hard links and
// snapshots can retain them). On error, removal may be partial; the result never
// by itself establishes durable completion.
type ReclaimResult struct {
	RemovedFiles uint64
	RemovedBytes uint64
	// SkippedEntries includes unrecognized names, directories, and symlinks.
	// Current catalog references are preserved and are not counted as skipped.
	SkippedEntries uint64
}

type fileRemover interface{ Remove(string) (bool, error) }

// Reclaim removes obsolete files under exclusive log ownership. Bbolt catalogs
// drain committed retirement records in bounded batches; they do not discover
// unpublished orphans or verify all live history. The protocol below describes
// the complete-catalog backend.
// It validates and confirms CURRENT and all references before any deletion, then
// removes unreferenced managed segment/tail/catalog/temp files and syncs each
// removal. It preserves unknown names, directories, and symbolic links.
//
// Log reads/appends and Close are excluded by the mutex. There are no public
// pinned views yet; callers must not hold external file handles or run lower-level
// readers/maintenance outside this ownership protocol. Existing in-memory Record
// payloads are unaffected. Reclaim is explicit, never automatic during open.
//
// Cancellation can return a durably removed prefix without poisoning the log.
// Filesystem or consistency errors poison it; Close/reopen and retry. After a
// crash, CURRENT remains authoritative and another call finds remaining orphans.
// This cleans managed files only, not old backups, snapshots, or device cells.
func (l *Log) Reclaim(ctx context.Context) (result ReclaimResult, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err = l.usable(); err != nil {
		return result, err
	}
	if ctx == nil {
		return result, ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	return l.catalog.reclaim(l, ctx)
}

func (s *CatalogStore) reclaim(l *Log, ctx context.Context) (result ReclaimResult, err error) {
	current, err := l.catalog.Current()
	if err != nil {
		return result, l.fail(err)
	}
	disk, name, err := loadCatalog(l.path)
	if err != nil {
		return result, l.fail(err)
	}
	expected, err := encodeCatalog(current)
	if err != nil {
		return result, l.fail(err)
	}
	actual, err := encodeCatalog(disk)
	if err != nil {
		return result, l.fail(err)
	}
	if !bytes.Equal(expected, actual) {
		return result, l.fail(ErrCatalogConflict)
	}
	// Refuse destructive cleanup when the selected history cannot be verified.
	// This full verification is intentionally conservative and currently expensive.
	if err = verifyCatalogFiles(l.path, disk); err != nil {
		return result, l.fail(err)
	}
	for _, file := range []string{name, "CURRENT"} {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if err = syncRegular(filepath.Join(l.path, file)); err != nil {
			return result, l.fail(err)
		}
	}
	if err = s.pub.Sync(); err != nil {
		return result, l.fail(err)
	}
	live := map[string]bool{"CURRENT": true, name: true}
	for _, ref := range disk.Segments {
		if ref.File != "" {
			live[ref.File] = true
		}
	}
	if disk.Active != nil {
		live[disk.Active.File] = true
	}
	entries, err := os.ReadDir(l.path)
	if err != nil {
		return result, l.fail(err)
	}
	type candidate struct {
		name string
		size int64
	}
	var obsolete []candidate
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if live[entry.Name()] {
			continue
		}
		if !managedArtifact(entry.Name()) {
			result.SkippedEntries++
			continue
		}
		info, e := entry.Info()
		if e != nil {
			return result, l.fail(e)
		}
		if !info.Mode().IsRegular() {
			result.SkippedEntries++
			continue
		}
		obsolete = append(obsolete, candidate{entry.Name(), info.Size()})
	}
	for _, file := range obsolete {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if file.size < 0 {
			return result, l.fail(ErrCorrupt)
		}
		removed, e := l.remover.Remove(file.name)
		if removed {
			result.RemovedFiles++
			result.RemovedBytes += uint64(file.size)
		}
		if e != nil {
			return result, l.fail(fmt.Errorf("segmentlog: reclaim %s: %w", file.name, e))
		}
	}
	return result, nil
}

func managedArtifact(name string) bool {
	for _, kind := range []struct {
		prefix, suffix string
		hexLength      int
	}{
		{"segment-", ".seg", 32}, {"tail-", ".active", 32}, {"catalog-", ".manifest", 64},
	} {
		if !strings.HasPrefix(name, kind.prefix) || !strings.HasSuffix(name, kind.suffix) {
			continue
		}
		body := strings.TrimSuffix(strings.TrimPrefix(name, kind.prefix), kind.suffix)
		if len(body) != 21+kind.hexLength || body[20] != '-' {
			return false
		}
		n, err := strconv.ParseUint(body[:20], 10, 64)
		if err != nil || fmt.Sprintf("%020d", n) != body[:20] {
			return false
		}
		digest := body[21:]
		if digest != strings.ToLower(digest) {
			return false
		}
		_, err = hex.DecodeString(digest)
		return err == nil
	}
	// os.CreateTemp in durablefs uses this reserved prefix and a decimal random
	// suffix. Restrict reclamation to that shape rather than all hidden files.
	const prefix = ".segmentlog-"
	if strings.HasPrefix(name, prefix) {
		suffix := strings.TrimPrefix(name, prefix)
		if len(suffix) == 0 || len(suffix) > 10 {
			return false
		}
		for _, c := range suffix {
			if c < '0' || c > '9' {
				return false
			}
		}
		_, err := strconv.ParseUint(suffix, 10, 32)
		return err == nil
	}
	return false
}
