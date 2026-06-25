package http_test

import (
	"encoding/json"
	"errors"
	"fmt"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// A 500 is the server's fault and the cause is deliberately withheld from
// the client. The server log is then the only place the cause is captured
// — the one thing a fronting load balancer's access log can't see — so it
// must be logged, at Error, and must not leak into the response body.
func TestInternalError_LogsCauseAtErrorWithoutLeaking(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	fake.ProposeDatabaseReturns(errors.New("boltdb: disk full"))
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodPost, "/v1/database/db-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	// The client gets a sanitized message, never the underlying cause.
	require.NotContains(t, w.Body.String(), "disk full")

	entries := logs.FilterMessage("http internal error").All()
	require.Len(t, entries, 1)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	fields := entries[0].ContextMap()
	require.Equal(t, "failed to propose database", fields["message"])
	// The cause is captured server-side.
	require.Contains(t, fmt.Sprint(fields["error"]), "disk full")
}

// A 4xx is the client's fault; its cause is already in the response and a
// Warn line. It must not be elevated to the Error-level internal-error log.
func TestClientError_NotLoggedAsInternalError(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	restore := zap.ReplaceGlobals(zap.New(core))
	defer restore()

	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake, http.WithBearerToken("secret"))

	// No Authorization header → 401, a client error.
	r := httptest.NewRequest(httpgo.MethodPost, "/v1/database/db-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusUnauthorized, w.Code)
	require.Empty(t, logs.FilterMessage("http internal error").All())
}

// rebuildRequired is a cluster.RebuildRequiredError test double — the generic
// shape the HTTP layer renders, with no dependency on the sql package's
// concrete SchemaChangeError.
type rebuildRequired struct {
	code    string
	message string
	details any
}

func (e *rebuildRequired) Error() string { return e.message }
func (e *rebuildRequired) Code() string  { return e.code }
func (e *rebuildRequired) Details() any  { return e.details }

// A RebuildRequiredError from propose is rendered as 409 with the
// machine-readable code + structured details, so a deploy pipeline can branch
// to the rebuild verb without scraping the message. The HTTP layer is agnostic
// to what the details contain.
func TestProposeSyncable_RebuildRequired_Returns409WithStructuredDetails(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ProposeSyncableReturns(&rebuildRequired{
		code:    "schema_change_requires_rebuild",
		message: "schema changed; rebuild the table",
		details: map[string]any{"table": "tenants", "addedColumns": []string{"tier"}},
	})
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodPost, "/v1/syncable/s-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusConflict, w.Code)

	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details struct {
			Table        string   `json:"table"`
			AddedColumns []string `json:"addedColumns"`
		} `json:"details"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "schema_change_requires_rebuild", body.Code)
	require.Contains(t, body.Message, "rebuild")
	require.Equal(t, "tenants", body.Details.Table)
	require.Equal(t, []string{"tier"}, body.Details.AddedColumns)
}

// A field-scoped ConfigError from propose is rendered as 400 with the config
// field path in structured details, so a deploy pipeline can point at the
// offending TOML key without scraping the message.
func TestProposeSyncable_ConfigError_FieldDetails(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.ProposeSyncableReturns(cluster.NewConfigError(&cluster.FieldError{
		Field: "sql.dialect",
		Issue: `unknown dialect "oracle"; valid: mysql, postgres`,
	}))
	h := http.New(fake)

	r := httptest.NewRequest(httpgo.MethodPost, "/v1/syncable/s-1", strings.NewReader("x = 1"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusBadRequest, w.Code)

	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details struct {
			Field string `json:"field"`
			Issue string `json:"issue"`
		} `json:"details"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "invalid_syncable_config", body.Code)
	require.Contains(t, body.Message, "unknown dialect")
	require.Equal(t, "sql.dialect", body.Details.Field)
	require.Contains(t, body.Details.Issue, "valid: mysql, postgres")
}
