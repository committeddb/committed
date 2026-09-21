package segmentlog

import (
	"context"
	"encoding/hex"
	"fmt"
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

// Reclaim drains committed retirement records in bounded batches. Each obsolete
// file is durably removed before its queue entry is acknowledged. It holds the
// Log mutex and exclusive directory ownership; reads and writes cannot overlap.
// It does not discover unpublished files or verify all live payloads. Use
// ReclaimOrphans for a directory sweep and Verify for a full integrity check.
// Cancellation can leave durable partial progress. Other failures poison the
// handle until Close/reopen; retry also handles already-removed queued names.
// This cleans local files only, not backups, snapshots, or device cells.
func (l *Log) Reclaim(ctx context.Context) (result ReclaimResult, err error) {
	l.maintenanceMu.Lock()
	defer l.maintenanceMu.Unlock()
	l.mutationMu.Lock()
	defer l.mutationMu.Unlock()
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

func managedArtifact(name string) bool {
	for _, kind := range []struct {
		prefix, suffix string
		hexLength      int
	}{
		{"segment-", ".seg", 32}, {"tail-", ".active", 32},
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
