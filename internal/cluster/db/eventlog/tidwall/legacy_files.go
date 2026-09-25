package tidwall

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// MoveLegacySegment renames src to dst, or copies and removes it when the two are on
// different filesystems. dst must not exist.
func MoveLegacySegment(src, dst string) error {
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("%s already exists", filepath.Base(dst))
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	if err := CopyLegacySegment(src, dst); err != nil {
		return err
	}
	return os.Remove(src)
}

// CopyLegacySegment copies a staged native segment to an exclusive destination
// and syncs its contents before success. The caller validates the source and
// owns directory synchronization and any adoption rollback.
func CopyLegacySegment(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // G304: a staged fetch file this node wrote
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600) //nolint:gosec // G304: a segment name under this node's own events dir
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(dst)
		return err
	}
	return out.Close()
}
