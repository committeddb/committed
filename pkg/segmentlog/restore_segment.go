package segmentlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"

	"github.com/committeddb/committed/internal/durablefs"
)

// RestoreSegment validates a backup copy against a currently selected immutable
// reference and, if commit is true, atomically replaces that file. It never
// changes metadata or restores an active tail. The exact catalog digest prevents
// restoring an earlier revision erased by a rewrite. Data must remain immutable
// throughout the call. Dry runs do not create files.
func RestoreSegment(ctx context.Context, path string, ref SegmentRef, data []byte, commit bool) (retErr error) {
	if ctx == nil || ref.Count == 0 {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	lock, err := durablefs.Lock(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	catalog, err := openBackupCatalog(filepath.Join(path, boltCatalogName))
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, catalog.Close()) }()
	if _, err := catalog.verifyMetadata(ctx); err != nil {
		return err
	}
	selected := false
	for current, err := range catalog.ranges(ref.Coverage) {
		if err != nil {
			return err
		}
		if current == ref {
			selected = true
		}
	}
	if !selected {
		return ErrCatalogConflict
	}
	if err := verifySegmentDigest(bytes.NewReader(data), int64(len(data)), ref); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !commit {
		return nil
	}
	dir, err := durablefs.Open(path)
	if err != nil {
		return err
	}
	_, err = dir.Replace(ref.File, func(w io.Writer) error { return writeFull(w, data) })
	return err
}
