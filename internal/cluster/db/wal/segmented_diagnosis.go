package wal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"

	bolterrors "go.etcd.io/bbolt/errors"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func diagnoseSegmentedLog(dir string) (*Diagnosis, error) {
	inspected, err := segmentlog.InspectDirectory(context.Background(), dir)
	if inspected.Records > math.MaxInt {
		return nil, fmt.Errorf("segmented record count exceeds diagnostic capacity")
	}
	d := &Diagnosis{Dir: dir, Records: int(inspected.Records), segmentedSegment: inspected.DamagedSegment}
	switch {
	case err == nil:
		d.Status = LogClean
		d.Detail = fmt.Sprintf("segmented log: %d surviving records, catalog and selected files valid", d.Records)
	case errors.Is(err, segmentlog.ErrIncompleteTail):
		d.Status = LogIncompleteTail
		d.Detail = "segmented log has an incomplete append group; truncation requires establishing the durable event boundary"
	case errors.Is(err, segmentlog.ErrCorrupt), errors.Is(err, os.ErrNotExist), errors.Is(err, bolterrors.ErrInvalid), errors.Is(err, bolterrors.ErrChecksum):
		d.Status = LogCorrupt
		message, _ := cluster.RedactedMessage(err)
		d.Detail = "segmented log: " + message
		if !inspected.CatalogVerified {
			d.segmentedCatalogUnavailable = true
			d.Detail = "segmented catalog is missing or corrupt; current file selection cannot be established; restore a complete backup into an empty directory or rebuild from a healthy peer"
		}
	default:
		// Ownership, permissions, I/O failures, and unknown format versions are
		// inspection errors, not evidence that the stored content is corrupt.
		return nil, err
	}
	return d, nil
}
