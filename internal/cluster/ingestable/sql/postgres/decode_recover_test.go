package postgres

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

// TestDecodeLogicalMessage_MalformedFrameRecovered is the pgoutput-decode-panic
// regression: a truncated frame that panics the pglogrepl decoder must come back
// as an error (so stream reconnects), never a panic that unwinds the ingest
// goroutine and crashes the whole node.
func TestDecodeLogicalMessage_MalformedFrameRecovered(t *testing.T) {
	// An empty WALData: pglogrepl.Parse reads the message-type byte data[0] with no
	// length check, so a truncated/empty frame indexes past the end and panics.
	malformed := []byte{}

	require.Panics(t, func() { _, _ = pglogrepl.Parse(malformed) },
		"precondition: this frame must still raw-panic pglogrepl.Parse, else the recover proves nothing")

	_, err := decodeLogicalMessage(malformed)
	require.Error(t, err, "the decoder panic must be converted to an error, not propagated")
	require.Contains(t, err.Error(), "panic parsing pgoutput")
}
