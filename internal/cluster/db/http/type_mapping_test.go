package http

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestTypeMigrationErrorsResponse pins the listing rendering: the failing
// chain step (fromVersion → toVersion), the RFC3339 timestamp, and the
// []-not-null guarantee.
func TestTypeMigrationErrorsResponse(t *testing.T) {
	out := typeMigrationErrorsResponse([]cluster.TypeMigrationDeadLetter{{
		Index: 7, FromVersion: 1, ToVersion: 2,
		TimestampUnixNano: 1_700_000_000_000_000_000,
		Message:           "jq: cannot index string with number",
	}})
	require.Len(t, out, 1)
	require.Equal(t, uint64(7), out[0].Index)
	require.Equal(t, 1, out[0].FromVersion)
	require.Equal(t, 2, out[0].ToVersion)
	require.Equal(t, "2023-11-14T22:13:20Z", out[0].Timestamp, "nanos must render as RFC3339 UTC")
	require.Contains(t, out[0].Message, "jq")

	require.NotNil(t, typeMigrationErrorsResponse(nil), "empty must be [] not null")
	require.Empty(t, typeMigrationErrorsResponse(nil))
}

// TestWriteTypeMigrationReplayResult pins the retry outcome mapping,
// including the still-failing 502 with the (redacted) cause — the leg a
// clean engine cannot produce without a really-failing jq chain.
func TestWriteTypeMigrationReplayResult(t *testing.T) {
	t.Run("nil is 200", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeTypeMigrationReplayResult(w, nil)
		require.Equal(t, 200, w.Code)
	})
	t.Run("not dead-lettered is 404", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeTypeMigrationReplayResult(w, cluster.ErrNotDeadLettered)
		require.Equal(t, 404, w.Code)
		require.Contains(t, w.Body.String(), "not_dead_lettered")
	})
	t.Run("still failing is 502 with the cause", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeTypeMigrationReplayResult(w, fmt.Errorf("%w: jq: null has no field x", cluster.ErrReplayMigrationFailed))
		require.Equal(t, 502, w.Code)
		require.Contains(t, w.Body.String(), "migration_retry_failed")
		require.Contains(t, w.Body.String(), "no field x", "the cause should surface in details")
	})
	t.Run("unknown failure is 500", func(t *testing.T) {
		w := httptest.NewRecorder()
		writeTypeMigrationReplayResult(w, fmt.Errorf("boom"))
		require.Equal(t, 500, w.Code)
	})
}
