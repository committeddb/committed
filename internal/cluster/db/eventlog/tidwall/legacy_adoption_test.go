package tidwall

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLegacyAdoptionPartialFailureRollback(t *testing.T) {
	dir := t.TempDir()
	stage := t.TempDir()
	first := "00000000000000000001"
	second := "00000000000000000005"
	tail := filepath.Join(dir, first)
	if err := os.WriteFile(tail, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{first: "incoming-first", second: "incoming-second"} {
		if err := os.WriteFile(filepath.Join(stage, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	conflict := filepath.Join(dir, second)
	if err := os.WriteFile(conflict, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []LegacySegment{{Path: filepath.Join(stage, first), FirstSeq: 1}, {Path: filepath.Join(stage, second), FirstSeq: 5}}
	if err := CheckLegacyAdoption(0, files); err != nil {
		t.Fatal(err)
	}
	attempt, err := InstallLegacySegments(dir, LegacyLayout{TailPath: tail}, files)
	if err == nil || attempt == nil {
		t.Fatal("expected partial installation failure", err)
	}
	if raw, err := os.ReadFile(tail); err != nil || string(raw) != "incoming-first" {
		t.Fatal("first file was not installed", err)
	}
	for range 2 {
		if err := attempt.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(tail); !os.IsNotExist(err) {
		t.Fatal("rollback retained incoming file", err)
	}
	if raw, err := os.ReadFile(conflict); err != nil || string(raw) != "existing" {
		t.Fatal("rollback removed unrelated file", err)
	}
	if raw, err := os.ReadFile(filepath.Join(stage, second)); err != nil || string(raw) != "incoming-second" {
		t.Fatal("failed move consumed staged file", err)
	}
}

func TestLegacyAdoptionRetainsCommittedTail(t *testing.T) {
	dir := t.TempDir()
	tail := filepath.Join(dir, "00000000000000000001")
	if err := os.WriteFile(tail, []byte("committed"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(t.TempDir(), "00000000000000000005")
	if err := os.WriteFile(src, []byte("incoming"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []LegacySegment{{Path: src, FirstSeq: 5}}
	if err := CheckLegacyAdoption(4, files); err != nil {
		t.Fatal(err)
	}
	attempt, err := InstallLegacySegments(dir, LegacyLayout{TailPath: tail, TailLen: 9}, files)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(tail); err != nil || string(raw) != "committed" {
		t.Fatal("installation changed committed tail", err)
	}
	if err := attempt.Rollback(); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(tail); err != nil || string(raw) != "committed" {
		t.Fatal("rollback changed committed tail", err)
	}
}

func TestLegacyAdoptionAlignment(t *testing.T) {
	for _, tc := range []struct {
		last  uint64
		ids   []uint64
		valid bool
	}{
		{0, nil, true},
		{0, []uint64{1, 5}, true},
		{4, []uint64{5, 9}, true},
		{4, []uint64{6}, false},
		{4, []uint64{5, 5}, false},
		{4, []uint64{5, 3}, false},
		{^uint64(0), []uint64{1}, false},
	} {
		files := make([]LegacySegment, 0, len(tc.ids))
		for _, id := range tc.ids {
			files = append(files, LegacySegment{FirstSeq: id})
		}
		err := CheckLegacyAdoption(tc.last, files)
		if (err == nil) != tc.valid {
			t.Fatal(tc, err)
		}
	}
}
