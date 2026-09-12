package http

import (
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The mechanism behind the dry-run's "empty reply after exactly 120s":
// the server's global WriteTimeout starts at request read, so a handler
// whose compute budget EQUALS it builds its response for a connection
// that is already dead. ResponseController.SetWriteDeadline extends the
// window per-request — this pins that the extension actually works on
// our server shape (a handler outliving the global WriteTimeout still
// delivers its response after extending).
func TestWriteDeadlineExtensionOutlivesGlobalWriteTimeout(t *testing.T) {
	slow := func(extend bool) httpgo.HandlerFunc {
		return func(w httpgo.ResponseWriter, r *httpgo.Request) {
			if extend {
				require.NoError(t, httpgo.NewResponseController(w).SetWriteDeadline(time.Now().Add(5*time.Second)))
			}
			time.Sleep(600 * time.Millisecond) // outlive the 300ms global write window
			w.WriteHeader(httpgo.StatusOK)
			_, _ = w.Write([]byte("partial report"))
		}
	}

	run := func(extend bool) (string, error) {
		mux := httpgo.NewServeMux()
		mux.Handle("/dryrun", slow(extend))
		srv := httptest.NewUnstartedServer(mux)
		srv.Config.WriteTimeout = 300 * time.Millisecond
		srv.Start()
		defer srv.Close()

		resp, err := httpgo.Get(srv.URL + "/dryrun")
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return string(body), err
	}

	// Without the extension: the connection dies under the handler — the
	// client sees an error or an empty body, never the report.
	if body, err := run(false); err == nil {
		require.NotEqual(t, "partial report", body, "without extension the global write timeout must kill the response")
	}

	// With the extension: the report arrives.
	body, err := run(true)
	require.NoError(t, err)
	require.Equal(t, "partial report", body)
}
