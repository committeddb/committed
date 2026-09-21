package segmentlog

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"sync"
	"testing"

	"github.com/committeddb/committed/internal/durablefs"
)

// Independent preparations may share immutable input without sharing transform
// buffers or output names. This exercises the writer without a managed Log.
func TestRewriteWriterSharedImmutableSource(t *testing.T) {
	path := t.TempDir()
	dir, err := durablefs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := rewriteWriter{dir: dir, encoding: Options{Compression: ZstdDefault}}
	source := cacheFixture(t, 0)
	original := bytes.Clone(source.data)
	type result struct {
		ref     SegmentRef
		changed bool
		calls   int
		err     error
	}
	results := make([]result, 2)
	var workers sync.WaitGroup
	for i := range results {
		workers.Go(func() {
			out := &results[i]
			out.ref, out.changed, out.err = writer.sealed(t.Context(), source.ref, source.Records(), func(r Record) ([]byte, bool, error) {
				out.calls++
				if r.ID == 7 {
					r.Payload[0] = byte('A' + i)
				}
				return r.Payload, true, nil
			})
		})
	}
	workers.Wait()
	if !bytes.Equal(source.data, original) {
		t.Fatal("transform changed shared input")
	}
	if results[0].ref.File == results[1].ref.File {
		t.Fatal("preparations reused an output name")
	}
	for i, out := range results {
		if out.err != nil || !out.changed || out.calls != 2 {
			t.Fatal(out)
		}
		data := readBytes(t, filepath.Join(path, out.ref.File))
		if sha256.Sum256(data) != out.ref.SHA256 {
			t.Fatal("digest mismatch")
		}
		segment, err := OpenSegment(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		first, err := segment.Read(0)
		if err != nil || string(first.Payload) != "original" {
			t.Fatal(first, err)
		}
		last, err := segment.Read(7)
		if err != nil || last.Payload[0] != byte('A'+i) {
			t.Fatal(last, err)
		}
	}
}
