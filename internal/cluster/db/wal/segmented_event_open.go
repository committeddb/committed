package wal

import (
	"fmt"
	"os"

	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/tidwall"
	"github.com/committeddb/committed/internal/cluster/metrics"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func rejectSegmentedEventDirectory(dir string) error {
	recognized, err := segmentlog.RecognizeDirectory(dir)
	if err != nil {
		return err
	}
	if recognized {
		return fmt.Errorf("event log at %q contains segmented storage; select the segmented backend to open it", dir)
	}
	return nil
}

func segmentedEventLogOpener(opts segmentlog.LogOptions) eventLogOpener {
	return func(dir string, m *metrics.Metrics, _ tidwall.LegacyOptions) (*eventLogBinding, error) {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		var log *segmented.Log
		if len(entries) == 0 {
			log, err = segmented.Create(dir, 1, opts)
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
