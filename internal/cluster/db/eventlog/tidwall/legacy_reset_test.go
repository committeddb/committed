package tidwall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyResetDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	if err := os.MkdirAll(filepath.Join(path, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "nested", "old"), []byte("old record"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := ResetLegacyDirectory(path); err != nil {
			t.Fatal(err)
		}
		files, err := os.ReadDir(path)
		if err != nil || len(files) != 0 {
			t.Fatal("reset did not leave an empty directory", files, err)
		}
	}
}

func TestLegacyResetFailurePhase(t *testing.T) {
	errRemove := errors.New("remove failed")
	errCreate := errors.New("create failed")
	created := false
	err := resetLegacyDirectory("events", func(string) error { return errRemove }, func(string, os.FileMode) error {
		created = true
		return nil
	})
	var failure *LegacyResetError
	if !errors.Is(err, errRemove) || !errors.As(err, &failure) || failure.Removed || created {
		t.Fatal(failure, created, err)
	}
	removed := false
	err = resetLegacyDirectory("events", func(string) error { removed = true; return nil }, func(_ string, mode os.FileMode) error {
		if !removed || mode != 0o700 {
			t.Fatal("creation order or permissions changed")
		}
		return errCreate
	})
	if !errors.Is(err, errCreate) || !errors.As(err, &failure) || !failure.Removed {
		t.Fatal(failure, err)
	}
}
