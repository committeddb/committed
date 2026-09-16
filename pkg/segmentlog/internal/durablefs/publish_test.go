package durablefs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

var errInjected = errors.New("injected failure")

type faultOps struct {
	local
	calls    []string
	fail     string
	failures map[string]bool
	short    bool
}

func (o *faultOps) hit(name string) error {
	o.calls = append(o.calls, name)
	if o.failures[name] {
		delete(o.failures, name)
		return errInjected
	}
	if o.fail == name {
		o.fail = ""
		return errInjected
	}
	return nil
}

func (o *faultOps) createTemp(dir string) (file, error) {
	if err := o.hit("create"); err != nil {
		return nil, err
	}
	f, err := o.local.createTemp(dir)
	if err != nil {
		return nil, err
	}
	return &faultFile{f, o}, nil
}

func (o *faultOps) link(a, b string) error {
	if err := o.hit("link"); err != nil {
		return err
	}
	return o.local.link(a, b)
}

func (o *faultOps) rename(a, b string) error {
	if err := o.hit("rename"); err != nil {
		return err
	}
	return o.local.rename(a, b)
}

func (o *faultOps) remove(a string) error {
	if err := o.hit("remove"); err != nil {
		return err
	}
	return o.local.remove(a)
}

func (o *faultOps) syncDir(a string) error {
	if err := o.hit("dirsync"); err != nil {
		return err
	}
	return o.local.syncDir(a)
}

type faultFile struct {
	file
	ops *faultOps
}

func (f *faultFile) Write(b []byte) (int, error) {
	if err := f.ops.hit("write"); err != nil {
		return 0, err
	}
	if f.ops.short {
		return f.file.Write(b[:len(b)/2])
	}
	return f.file.Write(b)
}

func (f *faultFile) Sync() error {
	if err := f.ops.hit("filesync"); err != nil {
		return err
	}
	return f.file.Sync()
}
func (f *faultFile) Close() error { return errors.Join(f.ops.hit("close"), f.file.Close()) }

func directory(t *testing.T) (*Dir, *faultOps) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("unsupported durability platform")
	}
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ops := &faultOps{}
	d.ops = ops
	return d, ops
}

func content(value string) func(io.Writer) error {
	return func(w io.Writer) error { _, err := io.WriteString(w, value); return err }
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	items, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item.Name())
	}
	return names
}

func TestPublicationOrder(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "install", true: "replace"}[replace], func(t *testing.T) {
			d, ops := directory(t)
			var result Result
			var err error
			if replace {
				if err := os.WriteFile(filepath.Join(d.path, "CURRENT"), []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
				result, err = d.Replace("CURRENT", content("new"))
			} else {
				result, err = d.Install("CURRENT", content("new"))
			}
			if err != nil || result != (Result{Installed: true, Durable: true}) {
				t.Fatal(result, err)
			}
			want := []string{"create", "write", "filesync", "close", "link", "remove", "dirsync"}
			if replace {
				want = []string{"create", "write", "filesync", "close", "rename", "dirsync"}
			}
			if !reflect.DeepEqual(ops.calls, want) {
				t.Fatalf("operations %v want %v", ops.calls, want)
			}
			if read(t, filepath.Join(d.path, "CURRENT")) != "new" {
				t.Fatal("wrong contents")
			}
			if got := entries(t, d.path); !reflect.DeepEqual(got, []string{"CURRENT"}) {
				t.Fatal("orphan", got)
			}
		})
	}
}

func TestFailureBoundaries(t *testing.T) {
	for _, replace := range []bool{false, true} {
		stages := []string{"create", "write", "filesync", "close", "link", "remove", "dirsync"}
		if replace {
			stages = []string{"create", "write", "filesync", "close", "rename", "dirsync"}
		}
		for _, stage := range stages {
			t.Run(map[bool]string{false: "install/", true: "replace/"}[replace]+stage, func(t *testing.T) {
				d, ops := directory(t)
				path := filepath.Join(d.path, "final")
				if replace {
					if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				ops.fail = stage
				var result Result
				var err error
				if replace {
					result, err = d.Replace("final", content("new"))
				} else {
					result, err = d.Install("final", content("new"))
				}
				if !errors.Is(err, errInjected) {
					t.Fatal("lost original error", err)
				}
				installed := stage == "dirsync" || stage == "remove"
				if result.Installed != installed {
					t.Fatal("incorrect installation state", result)
				}
				if result.Durable != (stage == "remove") {
					t.Fatal("incorrect durability state", result)
				}
				if errors.Is(err, ErrUncertain) != (stage == "dirsync") {
					t.Fatal("incorrect uncertainty", err)
				}
				if installed {
					if read(t, path) != "new" {
						t.Fatal("rolled back installed file")
					}
				} else if replace {
					if read(t, path) != "old" {
						t.Fatal("changed pointer before publication")
					}
				} else if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("unexpected destination", err)
				}
				if stage == "remove" {
					if result.Temp == "" || read(t, result.Temp) != "new" {
						t.Fatal("missing cleanup information")
					}
				} else if result.Temp != "" {
					t.Fatal("unexpected temporary file", result)
				}
			})
		}
	}
}

func TestCollisionAndInvalidNames(t *testing.T) {
	d, ops := directory(t)
	if _, err := d.Install("existing", content("original")); err != nil {
		t.Fatal(err)
	}
	result, err := d.Install("existing", content("replacement"))
	if !errors.Is(err, os.ErrExist) || result != (Result{}) {
		t.Fatal(result, err)
	}
	if read(t, filepath.Join(d.path, "existing")) != "original" {
		t.Fatal("overwrote immutable file")
	}
	for _, name := range []string{"", ".", "..", "../outside", "nested/file", "/absolute"} {
		ops.calls = nil
		if _, err := d.Replace(name, content("bad")); !errors.Is(err, ErrInvalid) {
			t.Fatal(name, err)
		}
		if len(ops.calls) != 0 {
			t.Fatal("invalid name touched filesystem")
		}
	}
	if _, err := d.Install("nil", nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if got := entries(t, d.path); !reflect.DeepEqual(got, []string{"existing"}) {
		t.Fatal(got)
	}
}

func TestPartialWriteAndCallbackFailure(t *testing.T) {
	for _, short := range []bool{false, true} {
		d, ops := directory(t)
		ops.short = short
		result, err := d.Install("final", func(w io.Writer) error {
			if _, err := w.Write([]byte("sensitive partial contents")); err != nil {
				return err
			}
			return errInjected
		})
		want := errInjected
		if short {
			want = io.ErrShortWrite
		}
		if !errors.Is(err, want) || result != (Result{}) {
			t.Fatal(result, err)
		}
		if len(entries(t, d.path)) != 0 {
			t.Fatal("partial output remained")
		}
		for _, call := range ops.calls {
			if call == "link" || call == "filesync" {
				t.Fatal("published failed write", ops.calls)
			}
		}
	}
}

func TestCleanupFailurePreservesBothErrors(t *testing.T) {
	d, ops := directory(t)
	ops.fail = "remove"
	original := errors.New("callback failed")
	result, err := d.Install("final", func(w io.Writer) error { _, _ = w.Write([]byte("partial")); return original })
	if !errors.Is(err, original) || !errors.Is(err, errInjected) || result.Installed || result.Durable || result.Temp == "" {
		t.Fatal(result, err)
	}
	if read(t, result.Temp) != "partial" {
		t.Fatal("missing orphan")
	}
}

func TestCleanupSyncFailure(t *testing.T) {
	d, ops := directory(t)
	ops.fail = "dirsync"
	original := errors.New("callback failed")
	result, err := d.Replace("CURRENT", func(w io.Writer) error { return original })
	if !errors.Is(err, original) || !errors.Is(err, errInjected) || result != (Result{}) {
		t.Fatal(result, err)
	}
	// Cleanup uncertainty is not a published-pointer uncertainty; the original
	// pointer was never touched. The caller still receives the cleanup failure.
	if errors.Is(err, ErrUncertain) {
		t.Fatal("reported a publication that never happened")
	}
}

func TestInstalledAliasAndSyncFailure(t *testing.T) {
	d, ops := directory(t)
	ops.failures = map[string]bool{"remove": true, "dirsync": true}
	result, err := d.Install("final", content("complete"))
	if !result.Installed || result.Durable || result.Temp == "" || !errors.Is(err, ErrUncertain) || !errors.Is(err, errInjected) {
		t.Fatal(result, err)
	}
	if read(t, filepath.Join(d.path, "final")) != "complete" || read(t, result.Temp) != "complete" {
		t.Fatal("lost installed file or alias")
	}
}

func TestIgnoredWriteFailureCannotPublish(t *testing.T) {
	d, ops := directory(t)
	ops.short = true
	result, err := d.Install("final", func(w io.Writer) error {
		_, _ = w.Write([]byte("truncated"))
		_, _ = w.Write([]byte("should not continue"))
		return nil
	})
	if result != (Result{}) || !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(result, err)
	}
	if len(entries(t, d.path)) != 0 {
		t.Fatal("published a truncated file")
	}
	writes := 0
	for _, call := range ops.calls {
		if call == "write" {
			writes++
		}
	}
	if writes != 1 {
		t.Fatal("continued after failed write", ops.calls)
	}
}

func TestDurableRemoval(t *testing.T) {
	for _, stage := range []string{"", "remove", "dirsync"} {
		d, ops := directory(t)
		path := filepath.Join(d.path, "obsolete")
		if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
		ops.fail = stage
		removed, err := d.Remove("obsolete")
		if stage == "" {
			if err != nil || !removed || !reflect.DeepEqual(ops.calls, []string{"remove", "dirsync"}) {
				t.Fatal(removed, err, ops.calls)
			}
			if removed, err := d.Remove("obsolete"); removed || err != nil {
				t.Fatal("missing removal not idempotent", removed, err)
			}
		} else {
			if !errors.Is(err, errInjected) || removed != (stage == "dirsync") || errors.Is(err, ErrUncertain) != (stage == "dirsync") {
				t.Fatal(removed, err)
			}
		}
		_, statErr := os.Stat(path)
		if stage == "remove" {
			if statErr != nil {
				t.Fatal("removed despite failed unlink", statErr)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			t.Fatal(statErr)
		}
	}
}

func TestRemovalRejectsPaths(t *testing.T) {
	d, ops := directory(t)
	for _, name := range []string{"", ".", "..", "../outside", "nested/file"} {
		if _, err := d.Remove(name); !errors.Is(err, ErrInvalid) {
			t.Fatal(name, err)
		}
	}
	if len(ops.calls) != 0 {
		t.Fatal("invalid removal performed I/O")
	}
}
