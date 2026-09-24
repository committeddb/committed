package segmentlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/committeddb/committed/internal/durablefs"
)

func compressionFixture(t *testing.T) (*Log, []Record) {
	t.Helper()
	l := cachedLog(t, 8192)
	l.encoding.Compression = ZstdDefault
	records := []Record{{1, bytes.Repeat([]byte("a"), 8192)}, {10, bytes.Repeat([]byte("b"), 8192)}, {30, []byte("tail")}}
	if err := l.Append(records); err != nil {
		t.Fatal(err)
	}
	return l, records
}

func TestCompressSealedPreservesHistoryAndReaders(t *testing.T) {
	l, records := compressionFixture(t)
	if _, err := l.Rewrite(t.Context(), 7, func(r Record) ([]byte, bool, error) { return r.Payload, true, nil }); err != nil {
		t.Fatal(err)
	}
	before, err := l.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	cursor := l.NewCursor()
	defer func() { _ = cursor.Close() }()
	if _, err := cursor.Seek(1); err != nil {
		t.Fatal(err)
	}
	entry := cursor.entry
	for i, old := range before.Segments {
		did, err := l.CompressNextSealed()
		if err != nil || !did {
			t.Fatal(did, err)
		}
		after, err := l.catalog.Current()
		if err != nil {
			t.Fatal(err)
		}
		if after.Generation != before.Generation || after.History != before.History || !reflect.DeepEqual(after.Active, before.Active) || after.Revision != before.Revision+uint64(i)+1 {
			t.Fatal("compression changed logical state", after)
		}
		ref := after.Segments[i]
		if ref.TailBytes != 0 || ref.Count != old.Count || ref.Coverage != old.Coverage || ref.File == old.File {
			t.Fatal("invalid replacement", ref)
		}
		for j := i + 1; j < len(before.Segments); j++ {
			if after.Segments[j] != before.Segments[j] {
				t.Fatal("compressed more than one segment")
			}
		}
		info, err := os.Stat(filepath.Join(l.path, ref.File))
		if err != nil || info.Size() >= old.TailBytes {
			t.Fatal("expected smaller compressed file", info, err)
		}
		if _, err := l.Reclaim(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(l.path, old.File)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("old file retained", err)
		}
	}
	if did, err := l.CompressNextSealed(); did || err != nil {
		t.Fatal(did, err)
	}
	if _, err := cursor.Seek(1); err != nil || cursor.entry != entry {
		t.Fatal("physical replacement invalidated the retained reader", err)
	}
	for _, want := range records {
		got, err := cursor.Seek(want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(got, err)
		}
	}
	l = reopenLog(t, l)
	for _, want := range records {
		got, err := l.Read(want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(got, err)
		}
	}
	if did, err := l.CompressNextSealed(); did || err != nil {
		t.Fatal("recompressed after reopen", did, err)
	}
	// A reset starts a new history at the original IDs.
	if err := l.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(records); err != nil {
		t.Fatal(err)
	}
	if did, err := l.CompressNextSealed(); !did || err != nil {
		t.Fatal("skipped reset history", did, err)
	}
}

type compressionInstaller struct {
	fileInstaller
	entered chan struct{}
	release chan struct{}
}

func (p compressionInstaller) Install(name string, write func(io.Writer) error) (durablefs.Result, error) {
	if strings.HasSuffix(name, ".seg") {
		close(p.entered)
		<-p.release
	}
	return p.fileInstaller.Install(name, write)
}

func TestCompressionAllowsConcurrentRollover(t *testing.T) {
	l, records := compressionFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	l.dir = compressionInstaller{l.dir, entered, release}
	done := make(chan error, 1)
	go func() {
		_, err := l.CompressNextSealed()
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("compression did not reach preparation")
	}
	appended := make(chan error, 1)
	go func() {
		appended <- l.Append([]Record{{40, bytes.Repeat([]byte("c"), 8192)}, {50, []byte("new tail")}})
	}()
	if err := awaitOperation(t, appended); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Read(1); err != nil || !bytes.Equal(got.Payload, records[0].Payload) {
		t.Fatal(got, err)
	}
	unblock()
	if err := awaitOperation(t, done); err != nil {
		t.Fatal(err)
	}
	if id, ok, err := l.LastAppended(); err != nil || !ok || id != 50 {
		t.Fatal(id, ok, err)
	}
	l.dir = l.dir.(compressionInstaller).fileInstaller
	for {
		did, err := l.CompressNextSealed()
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			break
		}
	}
	c, err := l.catalog.Current()
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range c.Segments {
		if ref.TailBytes != 0 {
			t.Fatal("missed concurrent rollover")
		}
	}
}

func TestCompressionDisabledAndFailure(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		l, _ := compressionFixture(t)
		l.encoding.Compression = NoCompression
		if did, err := l.CompressNextSealed(); did || err != nil {
			t.Fatal(did, err)
		}
	})
	t.Run("install failure", func(t *testing.T) {
		l, records := compressionFixture(t)
		errInjected := errors.New("install failure")
		l.dir = &rotationInstaller{fileInstaller: l.dir, failAt: 1, after: true, boom: errInjected}
		if did, err := l.CompressNextSealed(); did || !errors.Is(err, errInjected) {
			t.Fatal(did, err)
		}
		if _, err := l.Read(1); !errors.Is(err, ErrLogPoisoned) {
			t.Fatal(err)
		}
		l = reopenLog(t, l)
		for _, want := range records {
			got, err := l.Read(want.ID)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal(got, err)
			}
		}
		if did, err := l.CompressNextSealed(); !did || err != nil {
			t.Fatal(did, err)
		}
	})
}

func TestCompressionPublicationFailure(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			l, records := compressionFixture(t)
			before, err := l.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			catalog := l.catalog.(*boltCatalog)
			errInjected := errors.New("publication failure")
			catalog.commit = func(fn func(*bolt.Tx) error) error {
				if committed {
					if err := catalog.db.Update(fn); err != nil {
						return err
					}
				}
				return errInjected
			}
			if did, err := l.CompressNextSealed(); did || !errors.Is(err, errInjected) {
				t.Fatal(did, err)
			}
			l = reopenLog(t, l)
			after, err := l.catalog.Current()
			if err != nil {
				t.Fatal(err)
			}
			if after.Generation != before.Generation || after.History != before.History {
				t.Fatal("changed logical history")
			}
			if changed := after.Segments[0].File != before.Segments[0].File; changed != committed {
				t.Fatal("unexpected selected replacement")
			}
			if _, err := l.ReclaimOrphans(t.Context()); err != nil {
				t.Fatal(err)
			}
			for _, want := range records {
				got, err := l.Read(want.ID)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatal(got, err)
				}
			}
		})
	}
}

func TestCompressionWithoutCache(t *testing.T) {
	l, records := compressionFixture(t)
	l.cache = nil
	l.resident = nil
	for {
		did, err := l.CompressNextSealed()
		if err != nil {
			t.Fatal(err)
		}
		if !did {
			break
		}
	}
	if _, err := l.Reclaim(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, want := range records {
		got, err := l.Read(want.ID)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatal(got, err)
		}
	}
}
