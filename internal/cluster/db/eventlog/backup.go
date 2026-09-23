package eventlog

import "io"

// BackupSource captures files that reopen in the same backend, preserving IDs,
// append progress and the selected generation. Names are relative to the log
// directory. Only selected payload files are included, not retired data.
// visit must consume write synchronously and must not reenter the backend.
// A failure invalidates the capture. The owner excludes external directory
// replacement, native compression and Close throughout the call.
type BackupSource interface {
	CaptureBackup(visit func(string, int64, func(io.Writer) error) error) error
}
