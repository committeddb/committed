package http

import (
	"errors"
	"fmt"
	httpgo "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// A 500 is the server's fault and the cause is deliberately withheld from
// the client. The server log is then the only place the cause is captured
// — the one thing a fronting load balancer's access log can't see — so it
// must be logged, at Error, and must not leak into the response body.
func TestInternalError_LogsCauseAtErrorWithoutLeaking(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	// writeInternalError is the one door every 500 goes through; drive it
	// directly (there is no injectable engine failure any more).
	w := httptest.NewRecorder()
	writeInternalError(w, "failed to propose type", errors.New("boltdb: disk full"))

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	// The client gets a sanitized message, never the underlying cause.
	require.NotContains(t, w.Body.String(), "disk full")

	entries := logs.FilterMessage("http internal error").All()
	require.Len(t, entries, 1)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	require.Equal(t, "failed to propose type", fields["message"])
	// The cause is captured server-side.
	require.Contains(t, fmt.Sprint(fields["error"]), "disk full")
}
