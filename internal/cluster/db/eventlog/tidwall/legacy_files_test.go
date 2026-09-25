package tidwall

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLegacySegmentFileOperations(t *testing.T) {
	for name, operation := range map[string]func(string, string) error{
		"copy": CopyLegacySegment,
		"move": MoveLegacySegment,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			src := filepath.Join(dir, "staged")
			dst := filepath.Join(dir, "adopted")
			data := []byte{0, 1, 2, 0xff, 3}
			if err := os.WriteFile(src, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := operation(src, dst); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(dst)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal(got, err)
			}
			source, err := os.ReadFile(src)
			if name == "copy" {
				if err != nil || !bytes.Equal(source, data) {
					t.Fatal("copy consumed source", err)
				}
			} else if !os.IsNotExist(err) {
				t.Fatal("move retained source", err)
			}
			if err := os.WriteFile(src, []byte("replacement"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := operation(src, dst); err == nil {
				t.Fatal("overwrote destination")
			}
			got, err = os.ReadFile(dst)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatal("collision changed destination", err)
			}
			source, err = os.ReadFile(src)
			if err != nil || string(source) != "replacement" {
				t.Fatal("collision consumed source", err)
			}
		})
	}
}

func TestLegacySegmentCopyFailureRemovesPartialDestination(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "directory-source")
	dst := filepath.Join(dir, "destination")
	if err := os.Mkdir(src, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := CopyLegacySegment(src, dst); err == nil {
		t.Fatal("copied directory as segment")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("failed copy left destination", err)
	}
	if _, err := os.Stat(src); err != nil {
		t.Fatal("failed copy removed source", err)
	}
}
