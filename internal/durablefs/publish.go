// Package durablefs supplies local filesystem publication primitives. It does
// not choose catalogs, recover history, or decide when obsolete files may retire.
package durablefs

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

var (
	// ErrUncertain means a namespace change succeeded but its durability was
	// not confirmed. The owning log must stop mutation and recover from disk.
	ErrUncertain   = errors.New("durablefs: publication durability uncertain")
	ErrUnsupported = errors.New("durablefs: unsupported platform")
	ErrInvalid     = errors.New("durablefs: invalid filename or writer")
)

// Result describes observed progress, not recovery policy. Installed means the
// final name became visible in this process; Durable means its directory sync
// succeeded. Temp is nonempty only when a temporary name could not
// be removed. Never remove the final name to roll back an uncertain publication.
type Result struct {
	Installed bool
	Durable   bool
	Temp      string
}

type file interface {
	io.Writer
	Sync() error
	Close() error
	Name() string
}

// operations is deliberately limited to this protocol for fault injection.
type operations interface {
	createTemp(string) (file, error)
	link(string, string) error
	rename(string, string) error
	remove(string) error
	syncDir(string) error
}

type local struct{}

func (local) createTemp(dir string) (file, error) { return os.CreateTemp(dir, ".segmentlog-*") }
func (local) link(a, b string) error              { return os.Link(a, b) }
func (local) rename(a, b string) error            { return os.Rename(a, b) }
func (local) remove(path string) error            { return os.Remove(path) }
func (local) syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

// Dir publishes files into an existing, durably created, exclusively managed
// directory on a local filesystem with POSIX link/rename and directory fsync.
// Callers serialize conflicting publications and prevent concurrent directory
// replacement. Successful syscalls still rely on the filesystem/storage honoring
// their durability contract. Linux and macOS are enabled; other platforms fail
// before creating output rather than silently omitting directory synchronization.
type Dir struct {
	path string
	ops  operations
}

func Open(path string) (*Dir, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return nil, ErrUnsupported
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("durablefs: %s is not a directory", abs)
	}
	return &Dir{abs, local{}}, nil
}

// Install creates a new immutable final name without overwriting an existing
// entry. It writes and syncs a temporary file in the same directory, hard-links
// it to the final name, removes the temporary name, and syncs the directory.
// A collision is an error, even if the existing content might be identical.
func (d *Dir) Install(name string, write func(io.Writer) error) (Result, error) {
	return d.publish(name, write, false)
}

// Replace atomically replaces a small pointer file (such as CURRENT) after its
// new content is synced. The caller must first durably install all referenced
// files. This is one namespace operation, not a multi-file transaction.
func (d *Dir) Replace(name string, write func(io.Writer) error) (Result, error) {
	return d.publish(name, write, true)
}

func (d *Dir) publish(name string, write func(io.Writer) error, replace bool) (result Result, err error) {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || write == nil {
		return result, ErrInvalid
	}
	f, err := d.ops.createTemp(d.path)
	if err != nil {
		return result, err
	}
	temp := f.Name()
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, f.Close())
		}
		if temp != "" {
			if cleanup := d.ops.remove(temp); cleanup != nil {
				result.Temp = temp
				err = errors.Join(err, fmt.Errorf("durablefs: remove temporary file: %w", cleanup))
			} else {
				// Persist removal of sensitive partial output as well as reporting
				// the original failure. Recovery must still sweep crash orphans.
				err = errors.Join(err, d.ops.syncDir(d.path))
			}
		}
	}()
	w := &strictWriter{Writer: f}
	err = errors.Join(write(w), w.err)
	if err != nil {
		return result, err
	}
	if err = f.Sync(); err != nil {
		return result, err
	}
	err = f.Close()
	closed = true // A failed close must not be retried on a possibly reused fd.
	if err != nil {
		return result, err
	}
	destination := filepath.Join(d.path, name)
	if replace {
		err = d.ops.rename(temp, destination)
	} else {
		err = d.ops.link(temp, destination)
	}
	if err != nil {
		return result, err
	}
	result.Installed = true
	if replace {
		temp = "" // Rename consumed the temporary name.
	} else {
		if err = d.ops.remove(temp); err != nil {
			// Confirm durability of the installed file even if alias cleanup
			// fails, and expose the alias for caller-driven cleanup/recovery.
			result.Temp = temp
			temp = ""
			syncErr := d.ops.syncDir(d.path)
			result.Durable = syncErr == nil
			if syncErr != nil {
				err = errors.Join(err, ErrUncertain, syncErr)
			}
			return result, err
		}
		temp = ""
	}
	if err = d.ops.syncDir(d.path); err != nil {
		return result, errors.Join(ErrUncertain, err)
	}
	result.Durable = true
	return result, nil
}

// Convert a broken Writer's short success into a failure before publication.
type strictWriter struct {
	io.Writer
	err error
}

func (w *strictWriter) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.Writer.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

// Sync confirms existing directory entries after their files have been synced.
// It does not create the directory or sync its parent.
func (d *Dir) Sync() error { return d.ops.syncDir(d.path) }

// Remove deletes one caller-selected entry and syncs its directory. It never
// recursively removes a tree. removed describes observed unlink success; an
// error after unlink includes ErrUncertain and must not be treated as rollback.
// A missing entry is idempotent, but its absence is still directory-synced.
func (d *Dir) Remove(name string) (removed bool, err error) {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name {
		return false, ErrInvalid
	}
	err = d.ops.remove(filepath.Join(d.path, name))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	removed = err == nil
	if err = d.ops.syncDir(d.path); err != nil {
		return removed, errors.Join(ErrUncertain, err)
	}
	return removed, nil
}
