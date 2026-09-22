package tidwall

import (
	"fmt"
	"os"
)

// LegacySwapError reports a failed directory publication. A non-nil Rollback
// means the original live directory could not be restored: the owner must not
// reopen a missing live path, because native Open would create an empty log.
type LegacySwapError struct {
	Cause    error
	Rollback error
}

func (e *LegacySwapError) Error() string {
	if e.Rollback != nil {
		return fmt.Sprintf("%v (rollback failed: %v)", e.Cause, e.Rollback)
	}
	return e.Cause.Error()
}

func (e *LegacySwapError) Unwrap() error { return e.Cause }

// SwapLegacyDirectories publishes a prepared native replacement. The owner
// excludes readers/layout users, closes handles, and removes a stale retired
// directory BEFORE calling. On success it owns directory sync, reopening, and
// retirement. A recoverable failure restores the original live path; a rollback
// failure is reported explicitly. This does not establish directory durability.
func SwapLegacyDirectories(live, replacement, retired string) error {
	return swapLegacyDirectories(live, replacement, retired, os.Rename)
}

func swapLegacyDirectories(live, replacement, retired string, rename func(string, string) error) error {
	if err := rename(live, retired); err != nil {
		return &LegacySwapError{Cause: fmt.Errorf("move events aside for scrub swap: %w", err)}
	}
	if err := rename(replacement, live); err != nil {
		rollback := rename(retired, live)
		return &LegacySwapError{Cause: fmt.Errorf("rename scrubbed event log into place: %w", err), Rollback: rollback}
	}
	return nil
}
