package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// dryRunWindows is how many evenly-spaced log regions a dry run samples
// by default. One window from index 0 would fold zero events for topics
// whose activity clusters late in the log (the field shape) and report
// misleading zeros; spreading the budget samples every region.
const dryRunWindows = 8

// dryRunDefaultEntries is the sampling budget when the caller names none.
const dryRunDefaultEntries = 100_000

// DryRunSyncable rehearses a syncable config against a bounded sample
// of the committed log: full admission-level parsing (nothing stored),
// then the kind's DryRun over windows of committed Actuals. Never
// proposes, never writes — a node-local diagnostic read.
func (db *DB) DryRunSyncable(ctx context.Context, mimeType string, data []byte, opts cluster.DryRunOptions) (*cluster.DryRunReport, error) {
	parseStart := time.Now()
	_, s, _, err := db.parser.ParseSyncable(mimeType, data, db.storage)
	if err != nil {
		return nil, err
	}
	parseMs := time.Since(parseStart).Milliseconds()
	dr, ok := s.(cluster.DryRunner)
	if !ok {
		return nil, fmt.Errorf("this syncable kind does not support dry-run")
	}
	if opts.MaxEntries <= 0 {
		opts.MaxEntries = dryRunDefaultEntries
	}
	applied := db.storage.AppliedIndex()
	var starts []uint64
	if opts.FromIndex > 0 || applied == 0 {
		starts = []uint64{opts.FromIndex}
	} else {
		for i := 0; i < dryRunWindows; i++ {
			starts = append(starts, applied*uint64(i)/dryRunWindows)
		}
	}
	var windows []cluster.DryRunWindow
	var readMs int64
	feed := func(yield func(*cluster.Actual) error) error {
		remaining := opts.MaxEntries
		for wi, start := range starts {
			// Split what's left across the windows still to run, so an
			// early window ending at a region boundary donates its
			// unused budget to the later (usually denser) ones.
			budget := remaining / (len(starts) - wi)
			r := db.storage.ReaderAt(start)
			n := 0
			winStart := time.Now()
			stop := "budget"
			finish := func() {
				windows = append(windows, cluster.DryRunWindow{FromIndex: start, Entries: n, Ms: time.Since(winStart).Milliseconds(), Stop: stop})
			}
			for n < budget {
				if ctx.Err() != nil {
					// Deadline mid-window: report what we have — the
					// window row + phase timings say where time went.
					stop = "deadline"
					finish()
					return cluster.ErrDryRunTruncated
				}
				readStart := time.Now()
				a, err := r.Read()
				readMs += time.Since(readStart).Milliseconds()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						// A real read failure must not masquerade as
						// end-of-log: report partial, carrying the words.
						stop = "error"
						finish()
						return fmt.Errorf("%w: log read failed in window %d (fromIndex %d): %v", cluster.ErrDryRunTruncated, wi+1, start, err)
					}
					stop = "eof"
					break
				}
				if wi+1 < len(starts) && a.Index >= starts[wi+1] {
					stop = "boundary"
					break
				}
				n++
				if err := yield(a); err != nil {
					stop = "error"
					if ctx.Err() != nil {
						stop = "deadline"
					}
					finish()
					return err
				}
			}
			remaining -= n
			finish()
		}
		return nil
	}
	rep, err := dr.DryRun(ctx, feed, opts)
	if err != nil {
		return nil, err
	}
	rep.Windows = windows
	rep.ParseMs = parseMs
	rep.ReadMs = readMs
	// Belt and braces: whatever path the deadline took, an expired
	// context NEVER yields a report that looks complete (the field
	// discovered a silent time-bound only by comparing entries against
	// maxEntries).
	if rep.Truncated == "" && ctx.Err() != nil {
		rep.Truncated = "deadline reached during the run — entry counts are partial; raise ?timeoutSeconds or lower ?maxEntries"
	}
	// An EOF stop is GOOD news worth saying out loud: the window read to
	// the end of the log, so an under-filled entry count means complete
	// coverage of the requested region, not a bound (the field inferred
	// a silent time-limit from exactly this shape).
	complete := len(rep.Windows) > 0 && rep.Windows[0].FromIndex == 0 && rep.Truncated == ""
	for wi := range rep.Windows {
		if rep.Windows[wi].Stop == "eof" {
			rep.Findings = append(rep.Findings, fmt.Sprintf("note: window %d (fromIndex %d) read to END OF LOG — its %d entries are complete coverage from that index", wi+1, rep.Windows[wi].FromIndex, rep.Windows[wi].Entries))
		}
		if rep.Windows[wi].Stop != "eof" && rep.Windows[wi].Stop != "boundary" {
			complete = false
		}
	}
	if complete {
		rep.Coverage = "complete"
	} else if rep.Coverage == "" {
		rep.Coverage = "partial"
	}
	// The unseen-topic interpretation lives HERE because only this layer
	// knows whether coverage was complete: under complete coverage a
	// never-seen topic is a config fault (a mistyped id), not a note.
	rep.Findings = append(rep.Findings, cluster.CoverageFindings(rep.TopicsUnseen, complete)...)
	return rep, nil
}
