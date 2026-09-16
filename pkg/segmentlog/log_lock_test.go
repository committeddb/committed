package segmentlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/durablefs"
)

func TestLogLockContentionDoesNotTouchHistory(t *testing.T) {
	log := newLog(t, 40)
	if err := log.Append([]Record{{1, []byte("one")}, {2, []byte("two")}, {3, []byte("three")}}); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	entries, err := os.ReadDir(log.path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		b, err := os.ReadFile(filepath.Join(log.path, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		before[entry.Name()] = b
	}
	for range 3 {
		if _, err := OpenLog(log.path, Options{}); !errors.Is(err, ErrLocked) {
			t.Fatal(err)
		}
		if _, err := CreateLog(log.path, 0, LogOptions{}); !errors.Is(err, ErrLocked) {
			t.Fatal(err)
		}
	}
	after, err := os.ReadDir(log.path)
	if err != nil || len(after) != len(before) {
		t.Fatal("contender changed file set", err)
	}
	for _, entry := range after {
		b, err := os.ReadFile(filepath.Join(log.path, entry.Name()))
		if err != nil || !bytes.Equal(b, before[entry.Name()]) {
			t.Fatal("contender changed history", entry.Name(), err)
		}
	}
	log = reopenLog(t, log)
	if err := log.Append([]Record{{4, []byte("four")}}); err != nil {
		t.Fatal(err)
	}
}

func TestFailedLogOpenAndCreateReleaseLock(t *testing.T) {
	log := newLog(t, 40)
	dir := log.path
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	// Failure after acquiring ownership, but before attaching an appender.
	if _, err := OpenLog(dir, Options{Compression: 255}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	owner, err := durablefs.Lock(dir)
	if err != nil {
		t.Fatal("invalid encoding leaked ownership", err)
	}
	_ = owner.Close()
	if _, err := CreateLog(dir, 0, LogOptions{}); !errors.Is(err, ErrCatalogConflict) {
		t.Fatal(err)
	}
	owner, err = durablefs.Lock(dir)
	if err != nil {
		t.Fatal("failed create leaked ownership", err)
	}
	_ = owner.Close()
	current := filepath.Join(dir, "CURRENT")
	old, err := os.ReadFile(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(current, []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLog(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	owner, err = durablefs.Lock(dir)
	if err != nil {
		t.Fatal("failed recovery leaked ownership", err)
	}
	_ = owner.Close()
	if err := os.WriteFile(current, old, 0600); err != nil {
		t.Fatal(err)
	}
	next, err := OpenLog(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	// Closing the old instance again must not release the new instance's lock.
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLog(dir, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatal(err)
	}
}

func TestPoisonedLogRetainsOwnershipUntilClose(t *testing.T) {
	log := newLog(t, 100)
	log.tail.file = &faultyTail{File: log.file, syncErr: errors.New("sync failed")}
	if err := log.Append([]Record{{1, []byte("one")}}); !errors.Is(err, ErrLogPoisoned) {
		t.Fatal(err)
	}
	if _, err := OpenLog(log.path, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatal("poisoned owner released lock early", err)
	}
	log = reopenLog(t, log)
	if _, err := log.Read(1); err != nil {
		t.Fatal(err)
	}
}
