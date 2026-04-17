package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/internal/cluster/clusterfakes"
	"github.com/philborlin/committed/internal/cluster/http"
)

func TestBearerAuth(t *testing.T) {
	tests := []struct {
		name           string
		token          string // configured token; empty = no auth
		authHeader     string // Authorization header value
		path           string
		expectedStatus int
	}{
		{
			name:           "no token configured, no header",
			token:          "",
			authHeader:     "",
			path:           "/database",
			expectedStatus: 200,
		},
		{
			name:           "no token configured, header sent anyway",
			token:          "",
			authHeader:     "Bearer xyz",
			path:           "/database",
			expectedStatus: 200,
		},
		{
			name:           "token configured, no header",
			token:          "secret",
			authHeader:     "",
			path:           "/database",
			expectedStatus: 401,
		},
		{
			name:           "token configured, wrong token",
			token:          "secret",
			authHeader:     "Bearer wrong",
			path:           "/database",
			expectedStatus: 401,
		},
		{
			name:           "token configured, correct token",
			token:          "secret",
			authHeader:     "Bearer secret",
			path:           "/database",
			expectedStatus: 200,
		},
		{
			name:           "token configured, malformed scheme",
			token:          "secret",
			authHeader:     "Basic secret",
			path:           "/database",
			expectedStatus: 401,
		},
		{
			name:           "token configured, Bearer with no space",
			token:          "secret",
			authHeader:     "Bearersecret",
			path:           "/database",
			expectedStatus: 401,
		},
		{
			name:           "health exempt, no header",
			token:          "secret",
			authHeader:     "",
			path:           "/health",
			expectedStatus: 200,
		},
		{
			name:           "health exempt, wrong token",
			token:          "secret",
			authHeader:     "Bearer wrong",
			path:           "/health",
			expectedStatus: 200,
		},
		{
			name:           "ready exempt, no header",
			token:          "secret",
			authHeader:     "",
			path:           "/ready",
			expectedStatus: 503, // 503 not 401 — ready is exempt from auth
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &clusterfakes.FakeCluster{}
			var opts []http.Option
			if tc.token != "" {
				opts = append(opts, http.WithBearerToken(tc.token))
			}
			h := http.New(fake, opts...)

			req := httptest.NewRequest("GET", "http://localhost"+tc.path, nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			resp := w.Result()
			require.Equal(t, tc.expectedStatus, resp.StatusCode)
		})
	}
}

func TestBearerAuth_ErrorBody(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake, http.WithBearerToken("secret"))

	req := httptest.NewRequest("GET", "http://localhost/database", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, 401, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var errResp http.ErrorResponse
	require.NoError(t, json.Unmarshal(body, &errResp))
	require.Equal(t, "unauthorized", errResp.Code)
	require.NotEmpty(t, errResp.Message)
}
