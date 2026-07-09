package http

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

type fakeRedacted struct{ full, safe string }

func (e *fakeRedacted) Error() string           { return e.full }
func (e *fakeRedacted) RedactedMessage() string { return e.safe }

// TestRedactedDetail: a response-body detail derived from a Sync/apply error must
// expose only the RedactedMessage when the chain carries a cluster.RedactedError
// (a driver/migration error that may echo entity PII), and the full committed
// error text otherwise. Reproduces the exact shape the replay path builds.
func TestRedactedDetail(t *testing.T) {
	// As db/replay.go builds it: ErrReplaySyncFailed %w-wrapping the inner error,
	// which %w-wraps a RedactedError.
	inner := &fakeRedacted{full: `cannot iterate: string ("123-45-6789")`, safe: "transform failed (detail in node logs)"}
	replayErr := fmt.Errorf("%w: %w", cluster.ErrReplaySyncFailed, inner)

	got := redactedDetail(replayErr)
	require.NotContains(t, got, "123-45-6789", "entity PII must not reach the response body")
	require.Equal(t, "transform failed (detail in node logs)", got)

	// A committed-authored error (no RedactedError in the chain) is used verbatim.
	require.Equal(t, "not a dead letter for this syncable",
		redactedDetail(errors.New("not a dead letter for this syncable")))
}
