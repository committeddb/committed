package segmentlog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"io"
	"iter"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

// rewriteWriter produces unpublished replacement files from caller-owned inputs.
// It has no access to the live log, catalog, cache, cursor state, or tail handle.
// The caller keeps input storage alive and stable until the method returns.
// Each input must support replay of an unchanged prefix; transforms run once.
type rewriteWriter struct {
	dir      fileInstaller
	encoding Options
}

func (writer rewriteWriter) sealed(ctx context.Context, ref SegmentRef, input iter.Seq2[Record, error], transform Transform) (replacement SegmentRef, changed bool, err error) {
	replacement = ref
	changed, err = prepareRewrite(ctx, input, transform, func(records iter.Seq2[Record, error]) error {
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
		if _, e = writer.dir.Install(name, func(w io.Writer) error {
			return WriteSegment(io.MultiWriter(w, hash), ref.Coverage, counted, writer.encoding)
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

func (writer rewriteWriter) tail(ctx context.Context, ref TailRef, target uint64, state TailState, input iter.Seq2[Record, error], transform Transform) (replacement TailRef, changed bool, err error) {
	replacement = ref
	changed, err = prepareRewrite(ctx, input, transform, func(records iter.Seq2[Record, error]) error {
		name, e := uniqueName("tail", ref.Start, ".active")
		if e != nil {
			return e
		}
		// Reserve enough index capacity for future appends up to the original
		// rotation target, even if replacement payloads grew substantially.
		blockSize := writer.encoding.BlockSize
		if blockSize == 0 {
			blockSize = 256 << 10
		}
		limit := uint64(blockSize/2) * uint64(format.MaxBlocks-1)
		remaining := target - min(target, state.Framed)
		var physical uint64
		end := int64(tailHeaderSize)
		if _, e = writer.dir.Install(name, func(w io.Writer) error {
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
