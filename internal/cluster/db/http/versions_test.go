package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The version-history machinery against the real engine, on the type
// resource (every resource shares the generic handlers; the other
// resources' lifecycle tests cover their route bindings). The
// internal-error rendering leg is unit-covered by writeInternalError
// (errors_internal_test.go).

func TestGetVersions_Success(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	mustStatus(t, e.doTOML(t, "POST", "/v1/type/photos",
		"[type]\nname = \"photos\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n[migration]\nnone = true\n"), 200)

	var result []cluster.VersionInfo
	w := e.doEmpty(t, "GET", "/v1/type/photos/versions")
	mustStatus(t, w, 200)
	require.Equal(t, "application/json", w.Result().Header.Get("Content-Type"))
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Len(t, result, 2)
	require.Equal(t, uint64(1), result[0].Version)
	require.False(t, result[0].Current)
	require.Equal(t, uint64(2), result[1].Version)
	require.True(t, result[1].Current)
}

func TestGetVersions_ResourceNotFound(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/type/missing/versions"), 404, "type_not_found")
}

func TestGetVersion_Success(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")

	var result configResponse
	w := e.doEmpty(t, "GET", "/v1/type/photos/versions/1")
	mustStatus(t, w, 200)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	require.Equal(t, "photos", result.ID)
	require.Contains(t, result.Data, `name = "photos"`)
}

func TestGetVersion_VersionNotFound(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/type/photos/versions/99"), 404, "version_not_found")
}

func TestGetVersion_InvalidVersionParam(t *testing.T) {
	e := newEngine(t)
	for _, path := range []string{
		"/v1/type/photos/versions/abc",
		"/v1/type/photos/versions/0",
		"/v1/type/photos/versions/-1",
	} {
		t.Run(path, func(t *testing.T) {
			requireEnvelope(t, e.doEmpty(t, "GET", path), 400, "invalid_version")
		})
	}
}

// TestRollback_VersionNotFound: rolling a real ingestable back to a version
// that never existed is the version-level 404 through the real read.
func TestRollback_VersionNotFound(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderIngestable(t, "ing-1", "photos")
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/ingestable/ing-1/rollback?to=99"), 404, "version_not_found")
}

func TestRollback_MissingToParam(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "POST", "/v1/ingestable/ing-1/rollback"), 400, "missing_parameter")
}

func TestRollback_InvalidToParam(t *testing.T) {
	e := newEngine(t)
	for _, path := range []string{
		"/v1/database/db-1/rollback?to=abc",
		"/v1/database/db-1/rollback?to=0",
	} {
		t.Run(path, func(t *testing.T) {
			requireEnvelope(t, e.doEmpty(t, "POST", path), 400, "invalid_version")
		})
	}
}

// --- Type has no rollback endpoint ---

func TestTypeRollback_NoRoute(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "POST", "/v1/type/photos/rollback?to=1")
	// Chi returns 404 for unmatched routes (no POST /type/{id}/rollback registered).
	require.Equal(t, 404, w.Code)
}

// configResponse mirrors http.ConfigurationResponse for test unmarshaling.
type configResponse struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}
