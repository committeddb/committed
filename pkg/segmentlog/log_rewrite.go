package segmentlog

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"iter"
	"os"
	"path/filepath"
)

// SealedRewriteResult describes a sealed-only transaction. SealedEnd is the
// exclusive end of its scope; the active tail is untouched. ChangedSegments
// includes EmptiedSegments. Counts describe prepared replacements, even on error.
// Published is true only after catalog durability is confirmed; false with an
// error does not prove CURRENT stayed unchanged. Reopen to resolve uncertainty.
type SealedRewriteResult struct {
	SealedEnd       uint64
	ChangedSegments uint64
	EmptiedSegments uint64
	Published       bool
}

// RewriteSealed atomically publishes transformations of the current sealed
// ranges at a strictly newer logical generation. IDs and original coverage stay
// fixed; unchanged files retain their names and bytes. Entirely erased ranges
// become empty catalog descriptors without payload files. Even a no-op can
// publish the requested generation, writing only catalog metadata.
//
// This is deliberately a sealed-only operation, not a whole-log scrub. The active
// tail, append frontier, and rotation accounting are unchanged. It cannot complete
// a request whose scope includes active records. Generation describes this scoped
// transaction; Committed's full scrub-generation protocol is not integrated yet.
//
// The mutex excludes reads, appends, rotation, reclamation, and other rewrites.
// Transform runs once per examined record and must not call back into this Log.
// Old files remain until Reclaim. After preparation begins, any failure poisons
// the handle conservatively (including callback failure/cancellation); Close and
// reopen before retrying or reclaiming unpublished replacements.
func (l *Log) RewriteSealed(ctx context.Context, generation uint64, transform Transform) (result SealedRewriteResult, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err = l.usable(); err != nil {
		return result, err
	}
	if ctx == nil || transform == nil {
		return result, ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	c, err := l.catalog.Current()
	if err != nil {
		return result, l.fail(err)
	}
	result.SealedEnd = catalogEnd(c)
	if generation <= c.Generation || c.Revision == ^uint64(0) {
		return result, ErrInvalid
	}
	if err = verifyCatalogFiles(l.path, c); err != nil {
		return result, l.fail(err)
	}
	for i, ref := range c.Segments {
		if err = ctx.Err(); err != nil {
			return result, l.fail(err)
		}
		if ref.Count == 0 {
			continue
		}
		replacement, changed, e := l.prepareSealed(ctx, ref, transform)
		if e != nil {
			return result, l.fail(e)
		}
		if changed {
			result.ChangedSegments++
			if replacement.Count == 0 {
				result.EmptiedSegments++
			}
			c.Segments[i] = replacement
		}
	}
	if err = ctx.Err(); err != nil {
		return result, l.fail(err)
	}
	c.Revision++
	c.Generation = generation
	if err = l.catalog.Publish(c.Revision-1, c); err != nil {
		return result, l.fail(err)
	}
	result.Published = true
	return result, nil
}

func (l *Log) prepareSealed(ctx context.Context, ref SegmentRef, transform Transform) (replacement SegmentRef, changed bool, err error) {
	f, err := os.Open(filepath.Join(l.path, ref.File))
	if err != nil {
		return replacement, false, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	info, err := f.Stat()
	if err != nil {
		return replacement, false, err
	}
	segment, err := OpenSegment(f, info.Size())
	if err != nil {
		return replacement, false, err
	}
	if segment.Coverage() != ref.Coverage || segment.Count() != ref.Count {
		return replacement, false, ErrCorrupt
	}
	replacement = ref
	changed, err = segment.prepareRewrite(ctx, transform, func(records iter.Seq2[Record, error]) error {
		// Peek at the transformed stream: an entirely erased range needs no file.
		// Pulling is bounded by decoded blocks and never repeats the transformation.
		next, stop := iter.Pull2(records)
		defer stop()
		first, e, ok := next()
		if e != nil {
			return e
		}
		if !ok {
			replacement = SegmentRef{Coverage: ref.Coverage}
			return nil
		}
		name, e := uniqueName("segment", ref.Coverage.Start, ".seg")
		if e != nil {
			return e
		}
		var count uint64
		counted := func(yield func(Record, error) bool) {
			count++
			if !yield(first, nil) {
				return
			}
			for {
				r, e, ok := next()
				if !ok {
					return
				}
				if e != nil {
					yield(Record{}, e)
					return
				}
				count++
				if !yield(r, nil) {
					return
				}
			}
		}
		hash := sha256.New()
		if _, e = l.dir.Install(name, func(w io.Writer) error {
			return WriteSegment(io.MultiWriter(w, hash), ref.Coverage, counted, l.encoding)
		}); e != nil {
			return e
		}
		var digest [32]byte
		copy(digest[:], hash.Sum(nil))
		replacement = SegmentRef{Coverage: ref.Coverage, File: name, SHA256: digest, Count: count}
		return nil
	})
	return replacement, changed, err
}
