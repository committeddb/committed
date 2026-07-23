package replication

import (
	"errors"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func zstdEncodeAll(t *testing.T, b []byte) []byte {
	t.Helper()
	w, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	out := w.EncodeAll(b, nil)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestTransactionPayload_DecompressionBounded pins this committeddb fork patch (security
// B4 / finding S2): a compressed TransactionPayloadEvent must not decompress
// unbounded into memory. Without the bound a large or zstd-bomb transaction
// OOM-kills the process (an OOM recover() cannot catch) and re-OOMs at the same
// binlog coordinate on restart — a crash-loop. Re-apply this test with the patch
// on any go-mysql bump.
func TestTransactionPayload_DecompressionBounded(t *testing.T) {
	t.Run("declared oversize is rejected before decompressing", func(t *testing.T) {
		e := &TransactionPayloadEvent{
			CompressionType:     ZSTD,
			UncompressedSize:    2000,
			maxDecompressedSize: 1000,
			Payload:             zstdEncodeAll(t, []byte("small")),
		}
		err := e.decodePayload()
		if err == nil {
			t.Fatal("expected rejection: declared uncompressed size over the limit")
		}
		if !strings.Contains(err.Error(), "exceeds the configured limit") {
			t.Fatalf("expected the declared-size limit error, got: %v", err)
		}
	})

	t.Run("a bomb understating its size is stopped by the decoder memory cap", func(t *testing.T) {
		// 64 MiB of zeros compresses tiny but decompresses far past the 16 MiB cap.
		// UncompressedSize is a small lie so the cheap pre-check passes and the
		// decoder's memory cap is what must stop it — asserted specifically as
		// ErrDecoderSizeExceeded so that removing the cap (which would decompress
		// the whole 64 MiB and then fail downstream at event-parse with a DIFFERENT
		// error) fails this test rather than passing on the masking parse error.
		bomb := zstdEncodeAll(t, make([]byte, 64<<20))
		e := &TransactionPayloadEvent{
			CompressionType:     ZSTD,
			UncompressedSize:    64, // lie
			maxDecompressedSize: 16 << 20,
			Payload:             bomb,
		}
		if err := e.decodePayload(); !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
			t.Fatalf("expected ErrDecoderSizeExceeded from the decoder memory cap, got: %v", err)
		}
	})

	t.Run("unbounded (0) preserves upstream behavior: no pre-check rejection", func(t *testing.T) {
		// maxDecompressedSize == 0 must skip the pre-check even for a huge declared
		// size (committed always sets a bound; other fork users keep upstream behavior).
		e := &TransactionPayloadEvent{
			CompressionType:     ZSTD,
			UncompressedSize:    1 << 40,
			maxDecompressedSize: 0,
			Payload:             zstdEncodeAll(t, []byte("small")),
		}
		err := e.decodePayload()
		if err != nil && strings.Contains(err.Error(), "exceeds the configured limit") {
			t.Fatalf("pre-check must not fire when the bound is disabled (0), got: %v", err)
		}
	})
}
