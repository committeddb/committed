package tidwall

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// CheckLegacyAdoption checks native sequence alignment before any files change.
func CheckLegacyAdoption(last uint64, files []LegacySegment) error {
	if len(files) == 0 {
		return nil
	}
	if last == ^uint64(0) || files[0].FirstSeq != last+1 {
		return fmt.Errorf("this log ends at seq %d, the first file starts at %d", last, files[0].FirstSeq)
	}
	for i := 1; i < len(files); i++ {
		if files[i].FirstSeq <= files[i-1].FirstSeq {
			return fmt.Errorf("files out of order at %d", i)
		}
	}
	return nil
}

// LegacyAdoption tracks only files moved by one unpublished native adoption.
// It is in-memory rollback bookkeeping, not a durable completion marker.
type LegacyAdoption struct{ moved []string }

// InstallLegacySegments installs already-inspected and aligned files. The owner
// holds layout exclusion and has closed the native log. It owns directory sync,
// reopen, boundary checks, and final acceptance. Even on error, the returned
// attempt must be rolled back before reopening the original log.
func InstallLegacySegments(dir string, tail LegacyLayout, files []LegacySegment) (*LegacyAdoption, error) {
	attempt := &LegacyAdoption{}
	if len(files) == 0 {
		return attempt, nil
	}
	// An empty native tail occupies the first incoming file's sequence name.
	if tail.TailLen == 0 {
		if err := os.Remove(tail.TailPath); err != nil && !os.IsNotExist(err) {
			return attempt, fmt.Errorf("remove empty tail: %w", err)
		}
	}
	for _, file := range files {
		dst := filepath.Join(dir, filepath.Base(file.Path))
		if err := MoveLegacySegment(file.Path, dst); err != nil {
			return attempt, fmt.Errorf("adopt %s: %w", filepath.Base(file.Path), err)
		}
		attempt.moved = append(attempt.moved, dst)
	}
	return attempt, nil
}

// Rollback removes only this attempt's installed files. Reopening the original
// native log recreates any removed empty tail. Missing files are already undone.
func (a *LegacyAdoption) Rollback() error {
	var result error
	for _, path := range a.moved {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			result = errors.Join(result, err)
		}
	}
	return result
}
