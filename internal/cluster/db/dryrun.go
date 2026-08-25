package db

import (
	"context"
	"fmt"

	"github.com/committeddb/committed/internal/cluster"
)

// dryRunWindows is how many evenly-spaced log regions a dry run samples
// by default. One window from index 0 would fold zero events for topics
// whose activity clusters late in the log (the field shape) and report
// misleading zeros; spreading the budget samples every region.
const dryRunWindows = 4

// dryRunDefaultEntries is the sampling budget when the caller names none.
const dryRunDefaultEntries = 100_000

// DryRunSyncable rehearses a syncable config against a bounded sample
// of the committed log: full admission-level parsing (nothing stored),
// then the kind's DryRun over windows of committed Actuals. Never
// proposes, never writes — a node-local diagnostic read.
func (db *DB) DryRunSyncable(ctx context.Context, mimeType string, data []byte, opts cluster.DryRunOptions) (*cluster.DryRunReport, error) {
	_, s, _, err := db.parser.ParseSyncable(mimeType, data, db.storage)
	if err != nil {
		return nil, err
	}
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
	feed := func(yield func(*cluster.Actual) error) error {
		remaining := opts.MaxEntries
		for wi, start := range starts {
			// Split what's left across the windows still to run, so an
			// early window ending at a region boundary donates its
			// unused budget to the later (usually denser) ones.
			budget := remaining / (len(starts) - wi)
			r := db.storage.ReaderAt(start)
			n := 0
			for n < budget {
				a, err := r.Read()
				if err != nil {
					break // EOF — end of log
				}
				if wi+1 < len(starts) && a.Index >= starts[wi+1] {
					break // the next window covers from here
				}
				n++
				if err := yield(a); err != nil {
					return err
				}
			}
			remaining -= n
			windows = append(windows, cluster.DryRunWindow{FromIndex: start, Entries: n})
		}
		return nil
	}
	rep, err := dr.DryRun(ctx, feed, opts)
	if err != nil {
		return nil, err
	}
	rep.Windows = windows
	return rep, nil
}
