package http_test

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

// POSTing a syncable under the removed "sql-projection" spelling is refused
// as an invalid config whose message names the rename; the canonical
// spelling admits with no warnings. Runs against the real engine (the fixture
// admits "projection" through the recorder sink).
func TestAddSyncable_RemovedProjectionSpellingIsRefused(t *testing.T) {
	e := newEngine(t)
	w := e.doTOML(t, "POST", "/v1/syncable/s1",
		"[syncable]\nname = \"s1\"\ntype = \"sql-projection\"\n[sql-projection]\ntopic = \"t\"\n")
	require.Equal(t, 400, w.Code, w.Body.String())
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "invalid_syncable_config", body.Code)
	require.Contains(t, body.Message, `"sql-projection" was removed in 0.8.0`)
	require.Contains(t, body.Message, `rename the type to "projection"`)
}

func TestAddSyncable_CanonicalProjectionSpellingNoWarnings(t *testing.T) {
	e := newEngine(t)
	body := decodeConfigWrite(t, postSyncable(t, e, "s1",
		"[syncable]\nname = \"s1\"\ntype = \"projection\"\n"))
	require.Empty(t, body.Warnings)
}

// decodeConfigWrite unmarshals a config-write response body.
func decodeConfigWrite(t *testing.T, w *httptest.ResponseRecorder) http.ConfigWriteResponse {
	t.Helper()
	var body http.ConfigWriteResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	return body
}

// postSyncable POSTs a syncable config and asserts it was admitted.
func postSyncable(t *testing.T, e *engine, id, toml string) *httptest.ResponseRecorder {
	t.Helper()
	w := e.doTOML(t, "POST", "/v1/syncable/"+id, toml)
	require.Equal(t, 200, w.Code, w.Body.String())
	return w
}
