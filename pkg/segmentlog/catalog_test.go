package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func catalogFixture(t testing.TB) (string, Catalog) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("unsupported durability platform")
	}
	dir := t.TempDir()
	data := encode(t, Record{1, []byte("payload")}, Record{999, []byte("last")})
	if err := os.WriteFile(filepath.Join(dir, "one.seg"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir, Catalog{History: [16]byte{1}, Revision: 1, Segments: []SegmentRef{{Coverage: Coverage{0, 1000}, File: "one.seg", SHA256: sha256.Sum256(data), Count: 2}}}
}

func TestCatalogValidation(t *testing.T) {
	_, c := catalogFixture(t)
	for _, mutate := range []func(*Catalog){
		func(c *Catalog) { c.Revision = 0 }, func(c *Catalog) { c.History = [16]byte{} },
		func(c *Catalog) { c.Segments[0].Coverage.Start = 1 }, func(c *Catalog) { c.Segments[0].Coverage.End = 0 },
		func(c *Catalog) { c.Segments[0].File = "../one.seg" }, func(c *Catalog) { c.Segments[0].File = "CURRENT" },
		func(c *Catalog) { c.Segments[0].SHA256 = [32]byte{} }, func(c *Catalog) { c.Segments[0].Count = 0 },
		func(c *Catalog) { c.Segments[0].TailBytes = -1 },
		func(c *Catalog) { c.Segments[0].TailBytes = 1 },
		func(c *Catalog) { c.Segments[0].TailBytes = 4096 },    // Indexed filename cannot name an append file.
		func(c *Catalog) { c.Segments[0].File = "one.active" }, // Append filename requires frozen size.
		func(c *Catalog) { c.Segments[0].Count = 1001 }, func(c *Catalog) { c.Active = &TailRef{File: "tail.active", Start: 99} },
		func(c *Catalog) {
			c.Segments = append(c.Segments, SegmentRef{Coverage: Coverage{1000, 2000}, File: "one.seg", Count: 1, SHA256: c.Segments[0].SHA256})
		},
	} {
		bad := c
		bad.Segments = append([]SegmentRef(nil), c.Segments...)
		mutate(&bad)
		if err := validateCatalog(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
}

func TestCatalogReferencesFailClosed(t *testing.T) {
	for _, mode := range []string{"missing", "digest", "count", "range", "symlink", "tail-incomplete"} {
		t.Run(mode, func(t *testing.T) {
			dir, c := catalogFixture(t)
			switch mode {
			case "missing":
				if err := os.Remove(filepath.Join(dir, "one.seg")); err != nil {
					t.Fatal(err)
				}
			case "digest":
				c.Segments[0].SHA256[0] ^= 1
			case "count":
				c.Segments[0].Count++
			case "range":
				c.Segments[0].Coverage.End++
			case "symlink":
				if err := os.Rename(filepath.Join(dir, "one.seg"), filepath.Join(dir, "other.seg")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("other.seg", filepath.Join(dir, "one.seg")); err != nil {
					t.Fatal(err)
				}
			case "tail-incomplete":
				var h bytes.Buffer
				_ = WriteTailHeader(&h, 1000)
				h.WriteByte(1)
				if err := os.WriteFile(filepath.Join(dir, "tail.active"), h.Bytes(), 0o600); err != nil {
					t.Fatal(err)
				}
				c.Active = &TailRef{File: "tail.active", Start: 1000}
			}
			if err := verifyCatalogFiles(dir, c); err == nil {
				t.Fatal("accepted invalid reference")
			}
		})
	}
}
