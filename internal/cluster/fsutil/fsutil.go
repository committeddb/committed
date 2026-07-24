// Package fsutil provides the minimal fsync helpers that make a file or directory
// durable around a rename-into-place, so a write-then-rename survives an immediate
// power loss. It is a leaf package (only os) so the crash-durability discipline the
// WAL storage engine already applies internally can be reused by the backup/restore
// and wal-repair tooling without those leaf tools importing the storage engine.
package fsutil

import "os"

// SyncFile fsyncs the file at path to stable storage. Use it on a file written with
// io.Copy / os.WriteFile (neither of which fsyncs) BEFORE renaming it over live
// state, so a crash after the rename cannot surface a torn or zero-length file. A
// failure means the content is not durable, so callers abort rather than proceed.
func SyncFile(path string) error {
	f, err := os.Open(path) //nolint:gosec // G304: path is a caller-controlled data-dir file, never a request value.
	if err != nil {
		return err
	}
	// Sync first, then always close; report the first error.
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// SyncDir fsyncs a directory's entry list so a rename into or out of it is
// crash-durable — a rename is atomic for a concurrent reader's visibility but is
// not persisted until the parent directory is fsync'd. Call it on the destination's
// parent AFTER a rename to persist the swap, and on the source directory (before or
// after) when the renamed entry was just created there.
func SyncDir(path string) error {
	d, err := os.Open(path) //nolint:gosec // G304: path is a caller-controlled data-dir directory, never a request value.
	if err != nil {
		return err
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
