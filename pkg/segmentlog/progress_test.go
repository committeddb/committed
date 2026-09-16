package segmentlog

import (
	"errors"
	"testing"
)

func assertProgress(t *testing.T, log *Log, want uint64, has bool) {
	t.Helper()
	id, ok, err := log.LastAppended()
	if err != nil || id != want || ok != has {
		t.Fatal(id, ok, err, want, has)
	}
}

func TestLastAppendedSurvivesErasureAndRotation(t *testing.T) {
	log := newLog(t, 40)
	assertProgress(t, log, 0, false)
	if err := log.Append([]Record{{0, nil}}); err != nil {
		t.Fatal(err)
	}
	assertProgress(t, log, 0, true)
	if _, err := log.Rewrite(t.Context(), 1, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	assertProgress(t, log, 0, true)
	if err := log.Append([]Record{{100, []byte("original")}, {200, []byte("original")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Rewrite(t.Context(), 2, func(Record) ([]byte, bool, error) { return nil, false, nil }); err != nil {
		t.Fatal(err)
	}
	assertProgress(t, log, 200, true)
	// Rotation can publish an empty new tail before the next append succeeds.
	log.mu.Lock()
	err := log.rotate()
	log.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	assertProgress(t, log, 200, true)
	if _, err := log.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertProgress(t, log, 200, true)
	if _, err := log.Seek(0); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := log.Append([]Record{{500, nil}}); err != nil {
		t.Fatal(err)
	}
	assertProgress(t, log, 500, true)
}

func TestLastAppendedEmptyNonzeroStart(t *testing.T) {
	log, err := CreateLog(t.TempDir(), 100, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	assertProgress(t, log, 0, false)
	log = reopenLog(t, log)
	assertProgress(t, log, 0, false)
}

func TestLastAppendedFailureRequiresRecovery(t *testing.T) {
	log := newLog(t, 100)
	if err := log.Append([]Record{{1, nil}}); err != nil {
		t.Fatal(err)
	}
	log.tail.file = &faultyTail{File: log.file, syncErr: errors.New("sync failed")}
	if err := log.Append([]Record{{2, nil}}); !errors.Is(err, ErrLogPoisoned) {
		t.Fatal(err)
	}
	if _, _, err := log.LastAppended(); !errors.Is(err, ErrLogPoisoned) {
		t.Fatal(err)
	}
	log = reopenLog(t, log)
	assertProgress(t, log, 2, true)
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := log.LastAppended(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
