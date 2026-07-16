package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

// TestPlanForCharset classifies each charset family into the right plan:
// passthrough for the UTF-8/ascii/binary set (no transcoding), a transcode plan
// for a charset with a Go decoder, and unsupported (error, never silent U+FFFD)
// for anything else.
func TestPlanForCharset(t *testing.T) {
	t.Run("utf8mb4 is a supported passthrough", func(t *testing.T) {
		p := planForCharset("utf8mb4")
		require.True(t, p.supported)
		require.Nil(t, p.enc, "already UTF-8 — nothing to transcode")
	})

	t.Run("binary is a passthrough (BLOB must not be reinterpreted as text)", func(t *testing.T) {
		p := planForCharset("binary")
		require.True(t, p.supported)
		require.Nil(t, p.enc)
	})

	t.Run("ascii is a passthrough", func(t *testing.T) {
		p := planForCharset("ascii")
		require.True(t, p.supported)
		require.Nil(t, p.enc)
	})

	t.Run("latin1 transcodes via Windows-1252", func(t *testing.T) {
		p := planForCharset("latin1")
		require.True(t, p.supported)
		require.Equal(t, charmap.Windows1252, p.enc, "MySQL latin1 is Windows-1252, not ISO-8859-1")
	})

	t.Run("an unknown charset is unsupported (loud, not silent corruption)", func(t *testing.T) {
		p := planForCharset("dec8")
		require.False(t, p.supported)
		require.Nil(t, p.enc)
		require.Equal(t, "dec8", p.charset, "the charset name is retained for the error message")
	})
}

// TestPlanFor maps a collation id to its plan, treating "no collation" (id 0, a
// non-character column) and an id absent from the catalog as passthroughs — there
// is nothing to transcode — while a catalogued id keeps its resolved plan.
func TestPlanFor(t *testing.T) {
	plans := map[uint64]charsetPlan{
		8:  planForCharset("latin1"),  // latin1_swedish_ci
		45: planForCharset("utf8mb4"), // utf8mb4_general_ci
		99: planForCharset("dec8"),    // unsupported
	}

	t.Run("collation 0 is a passthrough (non-character column)", func(t *testing.T) {
		p := planFor(plans, 0)
		require.True(t, p.supported)
		require.Nil(t, p.enc)
	})

	t.Run("an absent collation is a passthrough", func(t *testing.T) {
		p := planFor(plans, 12345)
		require.True(t, p.supported)
		require.Nil(t, p.enc)
	})

	t.Run("a latin1 collation resolves to its transcoder", func(t *testing.T) {
		p := planFor(plans, 8)
		require.True(t, p.supported)
		require.Equal(t, charmap.Windows1252, p.enc)
	})

	t.Run("an unsupported charset's collation stays unsupported", func(t *testing.T) {
		require.False(t, planFor(plans, 99).supported)
	})
}

// TestTranscodeToUTF8 is the byte-level heart of the fix: a latin1 "café"
// (…0xE9) becomes valid UTF-8 "café" (…0xC3 0xA9) so it matches the snapshot
// path, a nil encoding is an identity passthrough, and a byte sequence a
// multibyte decoder rejects errors (so the caller can skip, never emit U+FFFD).
func TestTranscodeToUTF8(t *testing.T) {
	t.Run("latin1 café to UTF-8 café", func(t *testing.T) {
		latin1 := []byte{'c', 'a', 'f', 0xE9}
		out, err := transcodeToUTF8(charmap.Windows1252, latin1)
		require.NoError(t, err)
		require.Equal(t, "café", string(out))
		require.Equal(t, []byte{'c', 'a', 'f', 0xC3, 0xA9}, out, "valid UTF-8 bytes")
	})

	t.Run("nil encoding is a passthrough", func(t *testing.T) {
		in := []byte{'c', 'a', 'f', 0xE9}
		out, err := transcodeToUTF8(nil, in)
		require.NoError(t, err)
		require.Equal(t, in, out)
	})

	t.Run("ascii is unchanged through a latin1 decoder", func(t *testing.T) {
		out, err := transcodeToUTF8(charmap.Windows1252, []byte("plain"))
		require.NoError(t, err)
		require.Equal(t, "plain", string(out))
	})

	t.Run("a valid multibyte sequence round-trips", func(t *testing.T) {
		// Shift-JIS 'あ' (hiragana A) is the two-byte sequence 0x82 0xA0; it must
		// decode to the UTF-8 'あ' so a Japanese CHAR/VARCHAR matches the snapshot.
		enc := charsetEncodings["sjis"]
		require.NotNil(t, enc)
		out, err := transcodeToUTF8(enc, []byte{0x82, 0xA0})
		require.NoError(t, err)
		require.Equal(t, "あ", string(out))
	})
}
