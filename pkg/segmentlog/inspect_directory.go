package segmentlog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
)

// Inspection describes verified surviving records in an offline managed log.
// On failure, Records counts only the verified prefix. Tail is populated only
// after the catalog and all closed ranges have passed verification.
// An incomplete tail is evidence of a partial group, not permission to truncate;
// the application must establish its durability requirements before repair.
type Inspection struct {
	tailFile      string
	checkpointEnd int64
	Records       uint64
	// DamagedSegment identifies a selected immutable file that failed validation.
	// It is nil for catalog and active-tail failures.
	DamagedSegment *SegmentRef
	Tail           TailState
}

// InspectDirectory verifies a stopped managed log without recovery or writes.
// It acquires the same directory lock as OpenLog and reads bbolt in read-only
// mode. Metadata is streamed rather than materialized for the whole history.
// Unselected files are not part of the log and are neither scanned nor removed.
// Cancellation is checked between files and during metadata traversal.
func InspectDirectory(ctx context.Context, path string) (result Inspection, retErr error) {
	if ctx == nil {
		return result, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	lock, err := durablefs.Lock(path)
	if err != nil {
		return result, err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	return inspectDirectoryLocked(ctx, path)
}

// Caller holds the directory lock for the entire inspection and any repair.
func inspectDirectoryLocked(ctx context.Context, path string) (result Inspection, retErr error) {
	catalogPath := filepath.Join(path, boltCatalogName)
	info, err := os.Lstat(catalogPath) // #nosec G703 -- Fixed metadata basename in the caller-selected log directory.
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return result, ErrCorrupt
	}
	db, err := bolt.Open(catalogPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return result, err
	}
	defer func() { retErr = errors.Join(retErr, db.Close()) }()
	catalog := &boltCatalog{path: path, db: db}
	head, err := catalog.verifyMetadata(ctx)
	if err != nil {
		return result, err
	}
	for ref, err := range catalog.ranges(Coverage{head.Start, head.Active.Start}) {
		if err != nil {
			return result, err
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if err := checkCatalogFiles(path, Catalog{Segments: []SegmentRef{ref}}, false); err != nil {
			result.DamagedSegment = &ref
			return result, err
		}
		result.Records += ref.Count
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.tailFile = head.Active.File
	if head.Active.Checkpoint != nil {
		result.checkpointEnd = head.Active.Checkpoint.End
	}
	tailPath := filepath.Join(path, head.Active.File)
	info, err = os.Lstat(tailPath) // #nosec G703 -- Catalog validation checks the selected tail basename.
	if err != nil {
		return result, err
	}
	if !info.Mode().IsRegular() {
		return result, ErrCorrupt
	}
	f, err := os.Open(tailPath) // #nosec G304 G703 -- Validated catalog basename within the locked log directory.
	if err != nil {
		return result, err
	}
	defer func() { retErr = errors.Join(retErr, f.Close()) }()
	result.Tail, err = scanManagedTail(f, info.Size(), *head.Active, nil)
	result.Records += result.Tail.Count
	if err != nil {
		return result, fmt.Errorf("segmentlog: tail %s: %w", head.Active.File, err)
	}
	return result, nil
}

// RecognizeDirectory identifies existing segmented storage artifacts for offline
// tooling. Recognition does not establish validity: a missing or damaged catalog
// must still be diagnosed by InspectDirectory. It does not select a live backend.
func RecognizeDirectory(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if entry.Name() == boltCatalogName {
			return true, nil
		}
		if _, ok := dataFileStart(entry.Name()); ok {
			return true, nil
		}
	}
	return false, nil
}
