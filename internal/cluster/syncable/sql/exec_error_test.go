package sql

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestExecError_RedactsDriverDetail: a failed-exec error must keep driver detail
// (which can echo a bound entity key/data) out of the replicated dead-letter
// message, while retaining it in Error() for node-local logs, and stay reachable
// as a cluster.RedactedError through cluster.Permanent's double-wrap.
func TestExecError_RedactsDriverDetail(t *testing.T) {
	// A driver error that echoes a bound value, the way a Postgres FK violation does.
	driverErr := errors.New(`ERROR: violates foreign key constraint; Key (id)=(subject-pii@example.com) is still referenced`)
	label := "[sql.apply] exec [DELETE FROM t WHERE k = $1]"

	err := execFailure(label, driverErr, true) // permanent

	var red cluster.RedactedError
	require.True(t, errors.As(err, &red), "reachable as RedactedError through cluster.Permanent")

	// The replicated message carries the classifier + placeholder SQL, never the value.
	require.NotContains(t, red.RedactedMessage(), "subject-pii@example.com")
	require.Contains(t, red.RedactedMessage(), "DELETE FROM t WHERE k = $1")

	// Error() — node-local logs — keeps the full driver detail.
	require.Contains(t, err.Error(), "subject-pii@example.com")

	// Still classified permanent.
	require.True(t, errors.Is(err, cluster.ErrPermanent))
}

// TestExecFailure_TransientNotPermanent: a retryable exec keeps the redaction but
// is not marked permanent, so the worker retries rather than dead-lettering.
func TestExecFailure_TransientNotPermanent(t *testing.T) {
	err := execFailure("[sql.apply] exec [INSERT ...]", errors.New("connection reset"), false)
	require.False(t, errors.Is(err, cluster.ErrPermanent))
	var red cluster.RedactedError
	require.True(t, errors.As(err, &red))
	require.NotContains(t, red.RedactedMessage(), "connection reset")
}
