package tidwall

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLegacySwapDirectoryOutcomes(t *testing.T) {
	errRename := errors.New("injected rename failure")
	errRollback := errors.New("injected rollback failure")
	for _, tc := range []struct {
		name         string
		failAt       int
		failRollback bool
	}{
		{"publish", 0, false},
		{"retire-failure", 1, false},
		{"publish-failure", 2, false},
		{"rollback-failure", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			live := filepath.Join(dir, "events")
			replacement := filepath.Join(dir, "replacement")
			retired := filepath.Join(dir, "retired")
			for path, contents := range map[string]string{live: "original", replacement: "rewritten"} {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, "contents"), []byte(contents), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			calls := 0
			err := swapLegacyDirectories(live, replacement, retired, func(from, to string) error {
				calls++
				if calls == tc.failAt {
					return errRename
				}
				if calls == 3 && tc.failRollback {
					return errRollback
				}
				return os.Rename(from, to)
			})
			read := func(path, want string) {
				t.Helper()
				raw, err := os.ReadFile(filepath.Join(path, "contents"))
				if err != nil || string(raw) != want {
					t.Fatal(path, string(raw), err)
				}
			}
			if tc.failAt == 0 {
				if err != nil || calls != 2 {
					t.Fatal(calls, err)
				}
				read(live, "rewritten")
				read(retired, "original")
				if _, err := os.Stat(replacement); !os.IsNotExist(err) {
					t.Fatal("replacement path remained", err)
				}
				return
			}
			var failure *LegacySwapError
			if !errors.Is(err, errRename) || !errors.As(err, &failure) {
				t.Fatal(err)
			}
			read(replacement, "rewritten")
			if tc.failRollback {
				if !errors.Is(failure.Rollback, errRollback) {
					t.Fatal(failure)
				}
				if _, err := os.Stat(live); !os.IsNotExist(err) {
					t.Fatal("rollback failure hid missing live path", err)
				}
				read(retired, "original")
			} else {
				if failure.Rollback != nil {
					t.Fatal(failure)
				}
				read(live, "original")
				if _, err := os.Stat(retired); !os.IsNotExist(err) {
					t.Fatal("restored original remained retired", err)
				}
			}
		})
	}
}
