package tidwall

import (
	"fmt"
	"os"
)

// LegacyResetError identifies whether directory removal completed before a reset
// failed. Removed=true means recreating the directory failed; the owner must not
// resume using the retired handle or treat the reset as complete.
type LegacyResetError struct {
	Cause   error
	Removed bool
}

func (e *LegacyResetError) Error() string {
	if e.Removed {
		return fmt.Sprintf("recreate event log for reset: %v", e.Cause)
	}
	return fmt.Sprintf("remove event log for reset: %v", e.Cause)
}
func (e *LegacyResetError) Unwrap() error { return e.Cause }

// ResetLegacyDirectory removes the native event directory and recreates it empty.
// The owner has authorized a whole-log reset, excluded readers/layout users, and
// closed the log. It owns reopen, generation changes, and failure policy.
func ResetLegacyDirectory(path string) error {
	return resetLegacyDirectory(path, os.RemoveAll, os.MkdirAll)
}

func resetLegacyDirectory(path string, remove func(string) error, mkdir func(string, os.FileMode) error) error {
	if err := remove(path); err != nil {
		return &LegacyResetError{Cause: err}
	}
	if err := mkdir(path, 0o700); err != nil {
		return &LegacyResetError{Cause: err, Removed: true}
	}
	return nil
}
