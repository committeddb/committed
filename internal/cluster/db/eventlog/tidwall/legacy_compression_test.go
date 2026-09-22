package tidwall

import (
	"bytes"
	"errors"
	"testing"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

func TestLegacyCompressionPreservesRecordsAndRetiredHandle(t *testing.T) {
	path := t.TempDir()
	opts := *native.DefaultOptions
	opts.SegmentSize = 64
	opts.SealedSegmentCompression = native.CompressionZstd
	log, err := native.Open(path, &opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	for id := uint64(1); id <= 12; id++ {
		if err := log.Write(id, bytes.Repeat([]byte{byte(id)}, 24)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := log.LayoutSnapshot()
	if err != nil || len(before.Sealed) == 0 {
		t.Fatal("fixture did not seal", err)
	}
	var compressor eventlog.SealedCompressor = LegacyCompression{Log: log}
	for i := 0; i < len(before.Sealed); i++ {
		did, err := compressor.CompressNextSealed()
		if err != nil || !did {
			t.Fatal(i, did, err)
		}
	}
	if did, err := compressor.CompressNextSealed(); err != nil || did {
		t.Fatal(did, err)
	}
	after, err := log.LayoutSnapshot()
	if err != nil || after.Tail.Path != before.Tail.Path || after.TailLen != before.TailLen {
		t.Fatal("compression changed active tail", err)
	}
	for _, segment := range after.Sealed {
		if !native.IsCompressedSegmentPath(segment.Path) {
			t.Fatal("sealed file remained plain")
		}
	}
	for id := uint64(1); id <= 12; id++ {
		raw, err := log.Read(id)
		if err != nil || !bytes.Equal(raw, bytes.Repeat([]byte{byte(id)}, 24)) {
			t.Fatal(id, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := compressor.CompressNextSealed(); !errors.Is(err, eventlog.ErrClosed) || !errors.Is(err, native.ErrClosed) {
		t.Fatal("retired handle lost error identity", err)
	}
	reopened, err := native.Open(path, &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	compressor = LegacyCompression{Log: reopened}
	if did, err := compressor.CompressNextSealed(); err != nil || did {
		t.Fatal("replacement handle unusable", did, err)
	}
}
