package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/pkg/segmentlog"
)

// ErrTidwallEventLog refuses a tidwall event log where the segmented engine
// is selected: the node no longer runs tidwall, and `committed wal convert`
// rewrites the log for it.
var ErrTidwallEventLog = errors.New("the event log was written by the tidwall engine")

// refuseTidwallEventDirectory names the conversion for a non-empty event
// directory holding none of the segmented engine's files. A damaged segmented
// log still has some of them, so it fails its own open instead.
func refuseTidwallEventDirectory(dir string) error {
	recognized, err := segmentlog.RecognizeDirectory(dir)
	if err != nil || recognized {
		return err
	}
	return fmt.Errorf("%w (%s), which this release no longer runs: stop every node of the cluster, run `committed wal convert --data %s` on each, then start them (see docs/operations/upgrade.md): %w",
		ErrTidwallEventLog, dir, filepath.Dir(dir), eventlog.ErrUnsupported)
}

func segmentedEventLogOpener(opts segmentlog.LogOptions) eventLogOpener {
	return func(dir string, m *metrics.Metrics) (*eventLogBinding, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		var log *segmented.Log
		if len(entries) == 0 {
			log, err = segmented.Create(dir, 1, opts)
		} else if err = refuseTidwallEventDirectory(dir); err != nil {
			return nil, err
		} else {
			// Never turn a missing catalog into permission to create: legacy data,
			// a damaged catalog, and interrupted creation all require an explicit error.
			log, err = segmented.Open(dir, opts.Encoding, opts.Cache)
		}
		if err != nil {
			return nil, logOpenError(dir, "event_log", m, err)
		}
		return bindEventLog(log), nil
	}
}
