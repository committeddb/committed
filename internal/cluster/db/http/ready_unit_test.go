package http

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReady_Unit covers the four handler outcomes against a fake
// cluster: 503 with no leader, 503 with leader but applied=0, 200
// when both checks pass, and the body fields each case writes. This
// is the unit-level half of the readiness coverage; TestReady_RealRaft
// below exercises the same handler against an actual db.DB.
func TestReady_Unit(t *testing.T) {
	tests := []struct {
		name           string
		leader         uint64
		applied        uint64
		applyStalled   bool
		expectedStatus int
		expectedBody   ReadyResponse
	}{
		{
			name:           "no leader yet",
			leader:         0,
			applied:        0,
			expectedStatus: 503,
			expectedBody:   ReadyResponse{Status: "not ready"},
		},
		{
			name:           "leader elected but nothing applied",
			leader:         1,
			applied:        0,
			expectedStatus: 503,
			expectedBody:   ReadyResponse{Status: "not ready"},
		},
		{
			name:           "leader elected and applied advanced",
			leader:         1,
			applied:        7,
			expectedStatus: 200,
			expectedBody:   ReadyResponse{Status: "ok"},
		},
		{
			// The silent-while-green field incident: leader elected,
			// applied>0 — but apply has FROZEN with committed work
			// pending. The node can't confirm proposals and serves
			// stale reads; /ready must take it out of rotation.
			name:           "apply stalled",
			leader:         1,
			applied:        7,
			applyStalled:   true,
			expectedStatus: 503,
			expectedBody:   ReadyResponse{Status: "not ready"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &HTTP{view: &viewStub{leader: tc.leader, applied: tc.applied, stalled: tc.applyStalled}}

			req := httptest.NewRequest("GET", "http://localhost/ready", nil)
			w := httptest.NewRecorder()
			h.Ready(w, req)

			resp := w.Result()
			require.Equal(t, tc.expectedStatus, resp.StatusCode)
			require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			body, err := io.ReadAll(resp.Body)
			require.Nil(t, err)

			var got ReadyResponse
			require.Nil(t, json.Unmarshal(body, &got))
			require.Equal(t, tc.expectedBody, got)
		})
	}
}
