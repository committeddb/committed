package segmentlog

import (
	"context"
	"os"
	"path/filepath"
)

// preparedLogRewrite owns unpublished replacements and the replacement tail
// handle. prepare does not change the selected layout or resident tail. publish
// commits all replacements in one catalog transaction before adopting live state.
// The caller holds maintenanceMu, mutationMu (shared for sealed-only), and mu.
// Preparation temporarily releases mu for sealed source acquisition and replacement writing. Whole-log
// rewrites hold mutationMu exclusively to keep the captured tail stable.
// close releases any tail handle that publication did not transfer to the Log.
type preparedLogRewrite struct {
	baseRevision, generation uint64
	result                   RewriteResult
	changed, retired         []SegmentRef
	active                   *TailRef
	file                     *os.File
	tail                     *Tail
	resident                 *segmentBuilder
}

func (p *preparedLogRewrite) prepare(l *Log, ctx context.Context, c Catalog, transform Transform, includeTail bool) error {
	for next := c.Start; next < c.Active.Start; {
		if err := l.usable(); err != nil {
			return err
		}
		ref, err := nextRewriteRange(l.catalog, next, c.Active.Start)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		next = ref.Coverage.End
		if ref.Count == 0 {
			continue
		}
		replacement, changed, err := l.prepareSealed(ctx, ref, transform)
		if err != nil {
			return err
		}
		if changed {
			if l.cache != nil {
				p.retired = append(p.retired, ref)
			}
			p.result.ChangedSegments++
			if replacement.Count == 0 {
				p.result.EmptiedSegments++
			}
			p.changed = append(p.changed, replacement)
		}
	}
	if !includeTail {
		return nil
	}
	ref, changed, err := l.prepareTail(ctx, *c.Active, c.SegmentBytes, transform)
	if err != nil {
		return err
	}
	p.result.TailChanged = changed
	if !changed {
		return nil
	}
	p.file, err = os.OpenFile(filepath.Join(l.path, ref.File), os.O_RDWR, 0) // #nosec G304 G703 -- prepareTail generates this basename via uniqueName inside the exclusively owned log directory.
	if err != nil {
		return err
	}
	p.tail, p.resident, err = recoverResidentTail(p.file, ref.Checkpoint, l.resident != nil)
	if err != nil {
		return err
	}
	p.active = &ref
	return nil
}

func (p *preparedLogRewrite) publish(l *Log, ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := l.catalog.publishRewrite(p.baseRevision, p.generation, p.changed, p.active); err != nil {
		return err
	}
	l.cursorEpoch = new(byte)
	p.result.Published = true
	for _, ref := range p.retired {
		l.cache.discard(ref)
	}
	if p.file != nil {
		old := l.file
		l.file, l.tail, l.resident = p.file, p.tail, p.resident
		p.file = nil
		if err := old.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *preparedLogRewrite) close() error {
	if p.file == nil {
		return nil
	}
	file := p.file
	p.file = nil
	return file.Close()
}

// Fetch one descriptor and finish its read transaction before the caller drops
// mu. A long-lived bbolt read transaction could block rollover remapping while
// the appender holds mu, preventing preparation from reacquiring that same lock.
// maintenanceMu keeps the captured prefix stable; memory is independent of the
// total number of ranges in the catalog.
func nextRewriteRange(catalog layout, start, end uint64) (SegmentRef, error) {
	for ref, err := range catalog.ranges(Coverage{start, end}) {
		if err != nil {
			return SegmentRef{}, err
		}
		if ref.Coverage.Start != start || ref.Coverage.End > end {
			return SegmentRef{}, ErrCorrupt
		}
		return ref, nil
	}
	return SegmentRef{}, ErrCorrupt
}
