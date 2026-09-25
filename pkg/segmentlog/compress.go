package segmentlog

import (
	"crypto/sha256"
	"errors"
	"io"
)

// CompressNextSealed converts at most one closed append-format segment to the
// configured indexed encoding. It leaves the active tail, IDs, history and
// logical generation unchanged. NoCompression disables this maintenance.
// Preparation permits reads and appends; publication is atomic. Old files
// remain until Reclaim. Preparation/publication failures require Close/reopen.
func (l *Log) CompressNextSealed() (bool, error) {
	l.maintenanceMu.Lock()
	defer l.maintenanceMu.Unlock()
	l.mutationMu.RLock()
	defer l.mutationMu.RUnlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usable(); err != nil {
		return false, err
	}
	if l.encoding.Compression == NoCompression {
		return false, nil
	}
	c, err := l.catalog.head()
	if err != nil {
		return false, l.fail(err)
	}
	if l.compressionHistory != c.History {
		l.compressionHistory, l.compressionNext = c.History, c.Start
	}
	for l.compressionNext < c.Active.Start {
		ref, err := nextRewriteRange(l.catalog, l.compressionNext, c.Active.Start)
		if err != nil {
			return false, l.fail(err)
		}
		if ref.TailBytes == 0 {
			l.compressionNext = ref.Coverage.End
			// Reopening scans indexed descriptors once, without retaining a
			// catalog snapshot or blocking appends for the entire scan.
			l.mu.Unlock()
			l.mu.Lock()
			continue
		}
		if err := l.catalog.preflight(); err != nil {
			return false, l.fail(err)
		}
		files, err := l.prepareCompression(ref)
		if err != nil {
			return false, l.fail(err)
		}
		// Rollovers may have published while preparation released mu.
		latest, err := l.catalog.head()
		if err != nil {
			return false, l.fail(err)
		}
		if err := l.catalog.publishCompression(latest.Revision, files); err != nil {
			return false, l.fail(err)
		}
		// Existing cursors own immutable decoded bytes and can keep using them.
		if l.cache != nil {
			l.cache.discard(ref)
		}
		l.compressionNext = ref.Coverage.End
		return true, nil
	}
	return false, nil
}

func (l *Log) prepareCompression(ref SegmentRef) (files *verifiedRewriteFiles, err error) {
	// Maintenance ownership keeps the selected file and installer alive.
	l.mu.Unlock()
	defer l.mu.Lock()
	segment, release, err := acquireCachedRange(l.path, l.cache, ref)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, release()) }()
	name, err := uniqueName("segment", ref.Coverage.Start, ".seg")
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	if _, err := l.dir.Install(name, func(w io.Writer) error {
		return WriteSegment(io.MultiWriter(w, hash), ref.Coverage, segment.Records(), l.encoding)
	}); err != nil {
		return nil, err
	}
	replacement := SegmentRef{Coverage: ref.Coverage, File: name, Count: ref.Count}
	copy(replacement.SHA256[:], hash.Sum(nil))
	return l.catalog.verifyRewrite([]SegmentRef{replacement}, nil)
}
