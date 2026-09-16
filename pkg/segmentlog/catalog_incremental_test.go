package segmentlog

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPublicationFilesRequiresExactConfirmedReference(t *testing.T) {
	_, current := catalogFixture(t)
	original := current.Segments[0]
	for _, field := range []string{"unchanged", "coverage", "file", "digest", "count", "new"} {
		t.Run(field, func(t *testing.T) {
			next := cloneCatalog(current)
			next.Revision++
			switch field {
			case "coverage":
				next.Segments[0].Coverage.End++
			case "file":
				next.Segments[0].File = "different.seg"
			case "digest":
				next.Segments[0].SHA256[0] ^= 1
			case "count":
				next.Segments[0].Count--
			case "new":
				current := Catalog{}
				got := publicationFiles(current, next)
				if !reflect.DeepEqual(got.Segments, next.Segments) {
					t.Fatal(got)
				}
				return
			}
			next.Active = &TailRef{Start: next.Segments[0].Coverage.End, File: "tail.active"}
			got := publicationFiles(current, next)
			if got.Active != next.Active {
				t.Fatal("active tail omitted")
			}
			if field == "unchanged" {
				if len(got.Segments) != 0 {
					t.Fatal("unchanged segment reverified")
				}
			} else if !reflect.DeepEqual(got.Segments, next.Segments) {
				t.Fatal("changed reference trusted", got)
			}
			if current.Segments[0] != original {
				t.Fatal("modified confirmed catalog")
			}
		})
	}
}

func TestPublicationDoesNotReadConfirmedImmutableFiles(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		dir, c := catalogFixture(t)
		store, err := CreateCatalogStore(dir, c)
		if err != nil {
			t.Fatal(err)
		}
		if reopen {
			store, err = OpenCatalogStore(dir)
			if err != nil {
				t.Fatal(err)
			}
		}
		// Deliberately violate exclusive immutable ownership after verification.
		// Publication must not access this path; recovery must still reject it.
		path := filepath.Join(dir, c.Segments[0].File)
		hidden := filepath.Join(dir, "hidden.seg")
		if err := os.Rename(path, hidden); err != nil {
			t.Fatal(err)
		}
		next := cloneCatalog(c)
		next.Revision++
		if err := store.Publish(c.Revision, next); err != nil {
			t.Fatal("revisited immutable payload", err)
		}
		if _, err := OpenCatalogStore(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("recovery skipped verification", err)
		}
		if err := os.Rename(hidden, path); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenCatalogStore(dir); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPublicationVerifiesChangedReferenceAndActiveTail(t *testing.T) {
	for _, mode := range []string{"digest", "count", "file", "tail"} {
		t.Run(mode, func(t *testing.T) {
			dir, c := catalogFixture(t)
			if mode == "tail" {
				var header bytes.Buffer
				if err := WriteTailHeader(&header, 1000); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "tail.active"), header.Bytes(), 0o600); err != nil {
					t.Fatal(err)
				}
				c.Active = &TailRef{File: "tail.active", Start: 1000}
			}
			store, err := CreateCatalogStore(dir, c)
			if err != nil {
				t.Fatal(err)
			}
			next := cloneCatalog(c)
			next.Revision++
			switch mode {
			case "digest":
				next.Segments[0].SHA256[0] ^= 1
			case "count":
				next.Segments[0].Count--
			case "file":
				next.Segments[0].File = "missing.seg"
			case "tail":
				if err := os.WriteFile(filepath.Join(dir, "tail.active"), []byte("bad"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Publish(c.Revision, next); !errors.Is(err, ErrCatalogPoisoned) {
				t.Fatal("accepted unverified candidate", err)
			}
			got, err := store.Current()
			if !errors.Is(err, ErrCatalogPoisoned) || got.Revision != c.Revision {
				t.Fatal("advanced after failure", got, err)
			}
		})
	}
}
