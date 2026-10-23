package wal

import (
	"context"
	"os"

	twal "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/datadir"
	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
)

// RewriteEventLogAsTidwallForTest replaces a stopped node's segmented event
// log with the same records in the tidwall format a node before 0.8.3 wrote,
// sealed segments compressed as its sealer left them, so the converter has a
// log to read. bbolt already holds the scrub generation, where tidwall kept it.
func RewriteEventLogAsTidwallForTest(dir string, segmentBytes int) error {
	events := datadir.EventsDir(dir)
	src, err := segmented.Open(events, segmentedLogOptions().Encoding)
	if err != nil {
		return err
	}
	var records []eventlog.Record
	err = src.Scan(context.Background(), eventlog.Coverage{End: ^uint64(0)}, func(r eventlog.Record) error {
		records = append(records, r)
		return nil
	})
	if cerr := src.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	tmp := events + ".tidwall"
	log, err := twal.Open(tmp, &twal.Options{SegmentSize: segmentBytes, SealedSegmentCompression: twal.CompressionZstd, NoSync: true})
	if err != nil {
		return err
	}
	for i, r := range records {
		if err := log.Write(uint64(i+1), frame(r.Payload)); err != nil {
			_ = log.Close()
			return err
		}
	}
	for {
		did, err := log.CompressNextSealed()
		if err != nil {
			_ = log.Close()
			return err
		}
		if !did {
			break
		}
	}
	if err := log.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(events); err != nil {
		return err
	}
	return os.Rename(tmp, events)
}
