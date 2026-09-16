package segmentlog

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/durablefs"
)

func TestInstallSegmentFile(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("unsupported durability platform")
	}
	path := t.TempDir()
	dir, err := durablefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := dir.Install("example.seg", func(w io.Writer) error {
		return WriteSegment(w, Coverage{0, 100}, sequence(Record{1, []byte("first")}, Record{50, []byte("last")}), Options{Compression: ZstdDefault})
	})
	if err != nil || !result.Durable {
		t.Fatal(result, err)
	}
	f, err := os.Open(filepath.Join(path, "example.seg"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	segment, err := OpenSegment(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if err := segment.Verify(); err != nil {
		t.Fatal(err)
	}
	r, err := segment.Seek(2)
	if err != nil || r.ID != 50 {
		t.Fatal(r, err)
	}
}

func TestInstallAndAppendTail(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("unsupported durability platform")
	}
	path := t.TempDir()
	dir, err := durablefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := dir.Install("events.active", func(w io.Writer) error { return WriteTailHeader(w, 10) }); err != nil || !result.Durable {
		t.Fatal(result, err)
	}
	f, err := os.OpenFile(filepath.Join(path, "events.active"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tail, err := OpenTail(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := tail.Append([]Record{{10, []byte("ten")}, {100, []byte("hundred")}}); err != nil {
		t.Fatal(err)
	}
	state, err := tail.State()
	if err != nil || state.Count != 2 || state.Last != 100 {
		t.Fatal(state, err)
	}
}
