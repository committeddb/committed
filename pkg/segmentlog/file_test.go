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
