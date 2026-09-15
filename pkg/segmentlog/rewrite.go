package segmentlog

import (
	"bytes"
	"context"
	"io"
	"iter"
)

// Transform returns the replacement payload and whether the record survives.
// Returning the original bytes with keep=true is a no-op. A transform can modify
// its input payload; IDs cannot change. Each examined record is transformed once.
// Callbacks must return before cancellation can be observed.
type Transform func(Record) (payload []byte, keep bool, err error)

// Rewrite prepares a replacement for this segment's original coverage. It scans
// for the first change before calling create, so a no-op writes no bytes and
// creates no output. Subsequent encoding re-reads the unchanged prefix without
// invoking transform again. Memory is bounded by decoded blocks, not history.
//
// changed reports that a semantic change was found, even if output fails. On any
// error the caller must discard partial output. The caller owns the output's
// close, sync, validation, publication, and eventual retirement of the source.
// Rewrite never changes or deletes the source. A fully erased segment currently
// produces an empty segment; the future catalog can instead store an empty range.
func (s *Segment) Rewrite(ctx context.Context, create func() (io.Writer, error), transform Transform, opts Options) (changed bool, err error) {
	if ctx == nil || create == nil || transform == nil {
		return false, ErrInvalid
	}
	next, stop := iter.Pull2(s.Records())
	defer stop()
	apply := func(rec Record) (Record, bool, bool, error) {
		if err := ctx.Err(); err != nil {
			return Record{}, false, false, err
		}
		// Retain original bytes to detect in-place mutation by the callback.
		original := bytes.Clone(rec.Payload)
		payload, keep, err := transform(rec)
		if err != nil {
			return Record{}, false, false, err
		}
		if err := ctx.Err(); err != nil {
			return Record{}, false, false, err
		}
		return Record{rec.ID, payload}, keep, !keep || !bytes.Equal(original, payload), nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		rec, err, ok := next()
		if !ok {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		replacement, keep, differs, err := apply(rec)
		if err != nil {
			return false, err
		}
		if !differs {
			continue
		}
		w, err := create()
		if err != nil {
			return true, err
		}
		if w == nil {
			return true, ErrInvalid
		}
		records := func(yield func(Record, error) bool) {
			for prefix, err := range s.Records() {
				if err != nil {
					yield(Record{}, err)
					return
				}
				if err := ctx.Err(); err != nil {
					yield(Record{}, err)
					return
				}
				if prefix.ID >= rec.ID {
					break
				}
				if !yield(prefix, nil) {
					return
				}
			}
			if keep && !yield(replacement, nil) {
				return
			}
			for {
				if err := ctx.Err(); err != nil {
					yield(Record{}, err)
					return
				}
				rec, err, ok := next()
				if !ok {
					return
				}
				if err != nil {
					yield(Record{}, err)
					return
				}
				replacement, keep, _, err := apply(rec)
				if err != nil {
					yield(Record{}, err)
					return
				}
				if keep && !yield(replacement, nil) {
					return
				}
			}
		}
		return true, WriteSegment(w, s.coverage, records, opts)
	}
}
