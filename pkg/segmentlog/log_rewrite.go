package segmentlog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// SealedRewriteResult describes a sealed-only transaction. SealedEnd is the
// exclusive end of its scope; the active tail is untouched. ChangedSegments
// includes EmptiedSegments. Counts describe prepared replacements, even on error.
// Published is true only after catalog durability is confirmed; false with an
// error does not prove the previous catalog remained selected. Reopen to resolve uncertainty.
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
	resultAll, err := l.rewrite(ctx, generation, transform, false)
	return resultAll.SealedRewriteResult, err
}

// RewriteResult describes a whole-log transaction. TailChanged reports completed
// tail preparation, including on failure. Published has the same uncertainty
// semantics as SealedRewriteResult.Published.
type RewriteResult struct {
	SealedRewriteResult
	TailChanged bool
}

// Rewrite atomically transforms every surviving record in the captured log,
// including the active tail. It preserves original append progress and rotation
// accounting. All operations are serialized until publication finishes; callbacks
// must not reenter this Log. Old payload files remain until explicit Reclaim.
// Invalid input leaves the handle usable; preparation/publication failures require
// Close and reopen. This storage transaction does not update application metadata.
func (l *Log) Rewrite(ctx context.Context, generation uint64, transform Transform) (RewriteResult, error) {
	return l.rewrite(ctx, generation, transform, true)
}

func (l *Log) rewrite(ctx context.Context, generation uint64, transform Transform, includeTail bool) (result RewriteResult, err error) {
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
	c, err := l.catalog.head()
	if err != nil {
		return result, l.fail(err)
	}
	result.SealedEnd = c.Active.Start
	if generation <= c.Generation || c.Revision == ^uint64(0) {
		return result, ErrInvalid
	}
	if err = l.catalog.preflight(); err != nil {
		return result, l.fail(err)
	}
	var changedRefs []SegmentRef
	var retiredCacheRefs []SegmentRef
	for ref, e := range l.catalog.ranges(Coverage{c.Start, c.Active.Start}) {
		if e != nil {
			return result, l.fail(e)
		}
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
			if l.cache != nil {
				retiredCacheRefs = append(retiredCacheRefs, ref)
			}
			result.ChangedSegments++
			if replacement.Count == 0 {
				result.EmptiedSegments++
			}
			changedRefs = append(changedRefs, replacement)
		}
	}
	var newFile *os.File
	var newTail *Tail
	var replacementActive *TailRef
	if includeTail {
		ref, changed, e := l.prepareTail(ctx, *c.Active, c.SegmentBytes, transform)
		if e != nil {
			return result, l.fail(e)
		}
		result.TailChanged = changed
		if changed {
			newFile, e = os.OpenFile(filepath.Join(l.path, ref.File), os.O_RDWR, 0)
			if e != nil {
				return result, l.fail(e)
			}
			defer func() {
				if newFile != nil {
					err = errors.Join(err, newFile.Close())
				}
			}()
			newTail, e = openTail(newFile, ref.Checkpoint)
			if e != nil {
				return result, l.fail(e)
			}
			replacementActive = &ref
		}
	}
	if err = ctx.Err(); err != nil {
		return result, l.fail(err)
	}
	c.Revision++
	c.Generation = generation
	if err = l.catalog.publishRewrite(c.Revision-1, c.Generation, changedRefs, replacementActive); err != nil {
		return result, l.fail(err)
	}
	result.Published = true
	for _, ref := range retiredCacheRefs {
		l.cache.discard(ref)
	}
	if newFile != nil {
		old := l.file
		l.file, l.tail = newFile, newTail
		newFile = nil
		if e := old.Close(); e != nil {
			return result, l.fail(e)
		}
	}
	return result, nil
}

func (l *Log) prepareSealed(ctx context.Context, ref SegmentRef, transform Transform) (replacement SegmentRef, changed bool, err error) {
	segment, release, err := l.acquireRange(ref)
	if err != nil {
		return replacement, false, err
	}
	defer func() { err = errors.Join(err, release()) }()
	replacement = ref
	changed, err = prepareRewrite(ctx, segment.Records(), transform, func(records iter.Seq2[Record, error]) error {
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

func (l *Log) prepareTail(ctx context.Context, ref TailRef, target uint64, transform Transform) (replacement TailRef, changed bool, err error) {
	state, err := l.tail.State()
	if err != nil {
		return ref, false, err
	}
	replacement = ref
	changed, err = prepareRewrite(ctx, tailRecords(l.file, state.End), transform, func(records iter.Seq2[Record, error]) error {
		name, e := uniqueName("tail", ref.Start, ".active")
		if e != nil {
			return e
		}
		// Reserve enough index capacity for future appends up to the original
		// rotation target, even if replacement payloads grew substantially.
		blockSize := l.encoding.BlockSize
		if blockSize == 0 {
			blockSize = 256 << 10
		}
		limit := uint64(blockSize/2) * uint64(format.MaxBlocks-1)
		remaining := target - min(target, state.Framed)
		var physical uint64
		end := int64(tailHeaderSize)
		if _, e = l.dir.Install(name, func(w io.Writer) error {
			// Flush every encoded byte before the installer syncs and publishes.
			buffered := bufio.NewWriterSize(w, 256<<10)
			if e := WriteTailHeader(buffered, ref.Start); e != nil {
				return e
			}
			const groupTarget = 256 << 10
			group := make([]byte, groupHeaderSize, groupHeaderSize+groupTarget+groupTrailerSize)
			var count int
			var last uint64
			flushGroup := func() error {
				if count == 0 {
					return nil
				}
				group = finishTailGroup(group, count, last)
				if end > int64(^uint64(0)>>1)-int64(len(group)) {
					return ErrInvalid
				}
				if e := writeFull(buffered, group); e != nil {
					return e
				}
				end += int64(len(group))
				group = group[:groupHeaderSize]
				count = 0
				return nil
			}
			for r, e := range records {
				if e != nil {
					return e
				}
				if len(r.Payload) > format.MaxPayload {
					return ErrInvalid
				}
				framed := len(r.Payload) + format.FrameOverhead
				n := uint64(framed)
				if n > limit-remaining-physical {
					return ErrInvalid
				}
				physical += n
				if len(group)-groupHeaderSize+framed > groupTarget {
					if e := flushGroup(); e != nil {
						return e
					}
				}
				// Copy frames now: transforms may reuse their payload buffer.
				group = format.AppendFrame(group, r.ID, r.Payload)
				count++
				last = r.ID
			}
			if e := flushGroup(); e != nil {
				return e
			}
			return buffered.Flush()
		}); e != nil {
			return e
		}
		replacement = TailRef{File: name, Start: ref.Start, Checkpoint: &TailCheckpoint{End: end, Last: state.Last, Count: state.OriginalCount, Framed: state.Framed}}
		return nil
	})
	return replacement, changed, err
}
