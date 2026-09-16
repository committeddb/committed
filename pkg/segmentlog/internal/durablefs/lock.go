package durablefs

import (
	"errors"
	"os"
	"sync"
)

// ErrLocked reports an existing cooperating owner. Acquisition never waits.
var ErrLocked = errors.New("durablefs: directory already locked")

// DirectoryLock holds an advisory exclusive lock on the directory inode itself.
// No lock file is created or removed. Aliases of the same directory contend for
// the same lock. The directory must not be replaced while an owner is using it.
// Cooperating maintenance tools must acquire this same lock before mutation.
type DirectoryLock struct {
	mu   sync.Mutex
	file *os.File
}

// Lock acquires nonblocking ownership on supported local filesystems. The open
// directory descriptor is close-on-exec. A process exit releases its lock; no
// stale PID file or explicit force-unlock operation is needed. This is advisory:
// it does not prevent direct filesystem access by a non-cooperating writer.
func Lock(path string) (*DirectoryLock, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && !info.IsDir() {
		err = ErrInvalid
	}
	if err == nil {
		err = lockDirectory(f)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &DirectoryLock{file: f}, nil
}

// Close releases ownership after closing its descriptor. It is idempotent and
// does not retry a failed close on a descriptor that may have been reused.
func (l *DirectoryLock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	f := l.file
	l.file = nil
	return f.Close()
}
