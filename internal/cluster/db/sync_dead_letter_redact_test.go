package db

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// fakeRedacted is a cluster.RedactedError: Error() carries full (PII-bearing)
// detail, RedactedMessage() the PII-free form.
type fakeRedacted struct{ full, safe string }

func (e *fakeRedacted) Error() string           { return e.full }
func (e *fakeRedacted) RedactedMessage() string { return e.safe }

func TestSafeDeadLetterMessage(t *testing.T) {
	t.Run("redacted error yields its safe message, hides the full detail", func(t *testing.T) {
		e := &fakeRedacted{full: "driver: Key (id)=(pii@example.com)", safe: "exec failed (detail in node logs)"}
		// Through cluster.Permanent's double-wrap, as the real sink returns it.
		msg, redacted := safeDeadLetterMessage(cluster.Permanent(e))
		require.True(t, redacted)
		require.Equal(t, "exec failed (detail in node logs)", msg)
		require.NotContains(t, msg, "pii@example.com")
	})

	t.Run("plain (committed-authored) error is used verbatim", func(t *testing.T) {
		msg, redacted := safeDeadLetterMessage(errors.New("[sql.apply] cannot honor delete: no keyColumn or primaryKey configured"))
		require.False(t, redacted)
		require.Equal(t, "[sql.apply] cannot honor delete: no keyColumn or primaryKey configured", msg)
	})

	t.Run("nil error yields the operator default", func(t *testing.T) {
		msg, redacted := safeDeadLetterMessage(nil)
		require.False(t, redacted)
		require.Equal(t, "operator dead-letter", msg)
	})
}
