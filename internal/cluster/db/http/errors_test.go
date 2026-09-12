package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

// A 500 is the server's fault and the cause is deliberately withheld from
// the client. The server log is then the only place the cause is captured
// — the one thing a fronting load balancer's access log can't see — so it
// must be logged, at Error, and must not leak into the response body.

// A 4xx is the client's fault; its cause is already in the response and a
// Warn line. It must not be elevated to the Error-level internal-error log.
func TestClientError_NotLoggedAsInternalError(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	e := newEngineHTTP(t, http.WithBearerToken("secret"))
	w := e.doEmpty(t, "GET", "/v1/type")

	require.Equal(t, 401, w.Code)
	require.Empty(t, logs.All(), "a client error must not reach the Error-level logs")
}
