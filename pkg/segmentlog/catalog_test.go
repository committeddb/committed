package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/committeddb/committed/internal/durablefs"
	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
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

func TestCatalogPublicationAndEmptyRanges(t *testing.T) {
	dir, c := catalogFixture(t)
	before, err := os.Stat(filepath.Join(dir, "one.seg"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := CreateCatalogStore(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	// Returned snapshots cannot mutate the store's cached view.
	snapshot, err := store.Current()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Segments[0].File = "wrong.seg"
	snapshot, _ = store.Current()
	if snapshot.Segments[0].File != "one.seg" {
		t.Fatal("snapshot aliases current state")
	}
	next := cloneCatalog(c)
	next.Revision = 2
	next.Generation = 5
	next.Segments = append(next.Segments, SegmentRef{Coverage: Coverage{1000, 2000}})
	var h bytes.Buffer
	if err := WriteTailHeader(&h, 2000); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tail.active"), h.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	next.Active = &TailRef{File: "tail.active", Start: 2000}
	if err := store.Publish(1, next); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCatalogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Current()
	if err != nil || !reflect.DeepEqual(got, next) {
		t.Fatal(got, err)
	}
	got.Active.File = "wrong.active"
	again, _ := reopened.Current()
	if again.Active.File != "tail.active" {
		t.Fatal("tail snapshot aliases store")
	}
	after, err := os.Stat(filepath.Join(dir, "one.seg"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("publication rewrote sealed file")
	}
	if err := store.Publish(1, next); !errors.Is(err, ErrCatalogConflict) {
		t.Fatal(err)
	}
	if _, err := CreateCatalogStore(dir, c); !errors.Is(err, ErrCatalogConflict) {
		t.Fatal(err)
	}
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
		bad := cloneCatalog(c)
		mutate(&bad)
		if _, err := encodeCatalog(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal(bad, err)
		}
	}
	b, err := encodeCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := range b {
		bad := bytes.Clone(b)
		bad[i] ^= 1
		if _, err := decodeCatalog(bad); err == nil {
			t.Fatalf("accepted corrupt byte %d", i)
		}
	}
	for i := range b {
		if _, err := decodeCatalog(b[:i]); err == nil {
			t.Fatalf("accepted truncation %d", i)
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
			if _, err := CreateCatalogStore(dir, c); err == nil {
				t.Fatal("accepted invalid reference")
			}
			if _, err := os.Stat(filepath.Join(dir, "CURRENT")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("published invalid references", err)
			}
		})
	}
}

type failingPublisher struct {
	catalogPublisher
	at    string
	calls []string
	boom  error
}

func (p *failingPublisher) Install(n string, w func(io.Writer) error) (durablefs.Result, error) {
	p.calls = append(p.calls, "install")
	if p.at == "install" {
		return durablefs.Result{}, p.boom
	}
	return p.catalogPublisher.Install(n, w)
}

func (p *failingPublisher) Replace(n string, w func(io.Writer) error) (durablefs.Result, error) {
	p.calls = append(p.calls, "replace")
	if p.at == "replace-before" {
		return durablefs.Result{}, p.boom
	}
	r, err := p.catalogPublisher.Replace(n, w)
	if err != nil {
		return r, err
	}
	if p.at == "replace-after" {
		return durablefs.Result{Installed: true}, errors.Join(durablefs.ErrUncertain, p.boom)
	}
	return r, nil
}

func (p *failingPublisher) Sync() error {
	p.calls = append(p.calls, "sync")
	if p.at == "sync" {
		return p.boom
	}
	return p.catalogPublisher.Sync()
}

func TestCatalogPublicationFailures(t *testing.T) {
	for _, stage := range []string{"sync", "install", "replace-before", "replace-after"} {
		t.Run(stage, func(t *testing.T) {
			dir, c := catalogFixture(t)
			store, err := CreateCatalogStore(dir, c)
			if err != nil {
				t.Fatal(err)
			}
			boom := errors.New("injected publication failure")
			pub := &failingPublisher{catalogPublisher: store.pub, at: stage, boom: boom}
			store.pub = pub
			next := cloneCatalog(c)
			next.Revision = 2
			next.Generation = 1
			if err := store.Publish(1, next); !errors.Is(err, ErrCatalogPoisoned) || !errors.Is(err, boom) {
				t.Fatal(err)
			}
			got, err := store.Current()
			if !errors.Is(err, ErrCatalogPoisoned) || got.Revision != 1 {
				t.Fatal("exposed unconfirmed revision", got, err)
			}
			count := len(pub.calls)
			if err := store.Publish(1, next); !errors.Is(err, ErrCatalogPoisoned) || len(pub.calls) != count {
				t.Fatal("continued after failure", err)
			}
			recovered, err := OpenCatalogStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			got, err = recovered.Current()
			want := uint64(1)
			if stage == "replace-after" {
				want = 2
			}
			if err != nil || got.Revision != want {
				t.Fatal("recovery ignored CURRENT", got, err)
			}
			if stage == "replace-before" {
				// Retry reuses the identical orphaned catalog, not a guessed newer state.
				if err := recovered.Publish(1, next); err != nil {
					t.Fatal("orphan reuse", err)
				}
			}
		})
	}
}

func TestCatalogNeverFallsBack(t *testing.T) {
	for _, mode := range []string{"missing-current", "corrupt-current", "missing-catalog", "corrupt-catalog", "corrupt-segment"} {
		t.Run(mode, func(t *testing.T) {
			dir, c := catalogFixture(t)
			store, err := CreateCatalogStore(dir, c)
			if err != nil {
				t.Fatal(err)
			}
			next := cloneCatalog(c)
			next.Revision = 2
			if err := store.Publish(1, next); err != nil {
				t.Fatal(err)
			}
			_, name, err := loadCatalog(dir)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "CURRENT")
			switch mode {
			case "missing-current":
				err = os.Remove(path)
			case "corrupt-current":
				err = os.WriteFile(path, []byte("bad"), 0o600)
			case "missing-catalog":
				err = os.Remove(filepath.Join(dir, name))
			case "corrupt-catalog":
				err = os.WriteFile(filepath.Join(dir, name), []byte("bad"), 0o600)
			case "corrupt-segment":
				err = os.WriteFile(filepath.Join(dir, "one.seg"), []byte("bad"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := OpenCatalogStore(dir); err == nil {
				t.Fatal("fell back to older state")
			}
			if _, err := CreateCatalogStore(dir, c); !errors.Is(err, ErrCatalogConflict) {
				t.Fatal("reinitialized damaged history", err)
			}
		})
	}
}

func TestUnpublishedCatalogIgnored(t *testing.T) {
	dir, c := catalogFixture(t)
	if _, err := CreateCatalogStore(dir, c); err != nil {
		t.Fatal(err)
	}
	next := cloneCatalog(c)
	next.Revision = 900
	next.Generation = 900
	b, err := encodeCatalog(next)
	if err != nil {
		t.Fatal(err)
	}
	name := catalogName(next.Revision, sha256.Sum256(b))
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenCatalogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reopened.Current()
	if got.Revision != 1 || got.Generation != 0 {
		t.Fatal("selected unpublished revision")
	}
}

func TestCatalogMonotonicity(t *testing.T) {
	dir, c := catalogFixture(t)
	c.Generation = 10
	store, err := CreateCatalogStore(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Catalog){func(c *Catalog) { c.Revision = 3 }, func(c *Catalog) { c.History[0]++ }, func(c *Catalog) { c.Start++ }, func(c *Catalog) { c.Generation-- }, func(c *Catalog) { c.Segments = nil }} {
		next := cloneCatalog(c)
		next.Revision = 2
		mutate(&next)
		if err := store.Publish(1, next); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if _, err := store.Current(); err != nil {
			t.Fatal("invalid input poisoned store", err)
		}
	}
}

func FuzzCatalog(f *testing.F) {
	b, err := encodeCatalog(Catalog{History: [16]byte{1}, Revision: 1})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := decodeCatalog(b)
		if err != nil {
			return
		}
		encoded, err := encodeCatalog(c)
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatal("catalog roundtrip mismatch")
		}
	})
}

func TestCatalogCanonicalEncoding(t *testing.T) {
	_, c := catalogFixture(t)
	b, err := encodeCatalog(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{`{"Revision":99,`, `{"Unknown":true,`} {
		payload := append([]byte(prefix), b[17:len(b)-4]...)
		bad := append([]byte(nil), b[:16]...)
		format.LE.PutUint32(bad[12:], uint32(len(payload)))
		bad = append(bad, payload...)
		bad = format.LE.AppendUint32(bad, format.CRC(bad))
		if _, err := decodeCatalog(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatal("accepted ambiguous/unknown fields", err)
		}
	}
	b[8] = 1
	format.LE.PutUint32(b[len(b)-4:], format.CRC(b[:len(b)-4]))
	if _, err := decodeCatalog(b); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
}

func TestCatalogErasedRangeAndPublicationOrder(t *testing.T) {
	dir, c := catalogFixture(t)
	store, err := CreateCatalogStore(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	pub := &failingPublisher{catalogPublisher: store.pub}
	store.pub = pub
	next := cloneCatalog(c)
	next.Revision = 2
	next.Generation = 1
	next.Segments[0] = SegmentRef{Coverage: c.Segments[0].Coverage}
	if err := store.Publish(1, next); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pub.calls, []string{"sync", "install", "replace"}) {
		t.Fatal("wrong publication order", pub.calls)
	}
	reopened, err := OpenCatalogStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reopened.Current()
	if got.Segments[0].Count != 0 || got.Segments[0].Coverage != c.Segments[0].Coverage {
		t.Fatal("lost empty coverage", got)
	}
	// Publication does not claim physical erasure; pin-aware retirement is pending.
	if _, err := os.Stat(filepath.Join(dir, "one.seg")); err != nil {
		t.Fatal("premature retirement", err)
	}
}
