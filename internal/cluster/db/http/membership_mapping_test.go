package http

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestWriteMembershipError pins the conf-change outcome mapping — including
// the unconfirmed 503, which a live single-node engine (always quorate with
// itself) cannot produce.
func TestWriteMembershipError(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{"invalid member is 400", fmt.Errorf("wrap: %w", cluster.ErrInvalidMember), 400, "invalid_member"},
		{"not a learner is 400", fmt.Errorf("wrap: %w", cluster.ErrNotLearner), 400, "not_a_learner"},
		{"last voter is 409", fmt.Errorf("wrap: %w", cluster.ErrWouldRemoveLastVoter), 409, "would_remove_last_voter"},
		{"deadline is 503", context.DeadlineExceeded, 503, "membership_unconfirmed"},
		{"cancellation is 503", context.Canceled, 503, "membership_unconfirmed"},
		{"unknown failure is 500", fmt.Errorf("boom"), 500, "internal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			writeMembershipError(w, tc.err, "add")
			require.Equal(t, tc.wantStatus, w.Code)
			require.Contains(t, w.Body.String(), tc.wantCode)
		})
	}
}
