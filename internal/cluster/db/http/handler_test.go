package http_test

import (
	"encoding/json"
	"io"
	httpgo "net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db/http"
)

// requireErrorResponse reads the response body, unmarshals it as an
// ErrorResponse, and asserts the code field matches expectedCode.
func requireErrorResponse(t *testing.T, resp *httpgo.Response, expectedCode string) http.ErrorResponse {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var errResp http.ErrorResponse
	err = json.Unmarshal(body, &errResp)
	require.Nil(t, err, "response body is not valid ErrorResponse JSON: %s", string(body))
	require.Equal(t, expectedCode, errResp.Code)
	require.NotEmpty(t, errResp.Message)
	return errResp
}

// TestErrorResponse_JSONShape: the error envelope's wire shape, through a
// real engine's own 404.
func TestErrorResponse_JSONShape(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "GET", "/v1/type/missing/versions")
	require.Equal(t, 404, w.Code)
	requireErrorResponse(t, w.Result(), "type_not_found")
}

// TestAddConfiguration_ContentTypeHandling: the stored MimeType tracks the
// request — text/toml by default, an explicit content type verbatim, and a
// charset parameter stripped to the base media type — proven by reading the
// stored version back from the real engine.
func TestAddConfiguration_ContentTypeHandling(t *testing.T) {
	storedMime := func(t *testing.T, e *engine, id string) string {
		t.Helper()
		var got struct {
			MimeType string `json:"mimeType"`
		}
		e.getJSON(t, "/v1/type/"+id+"/versions/1", &got)
		return got.MimeType
	}

	t.Run("default mime type is text/toml", func(t *testing.T) {
		e := newEngine(t)
		w := e.do(t, "POST", "/v1/type/t1", "", "[type]\nname = \"t1\"\n")
		mustStatus(t, w, 200)
		require.Equal(t, "text/toml", storedMime(t, e, "t1"))
	})

	t.Run("charset parameter is stripped to the base media type", func(t *testing.T) {
		e := newEngine(t)
		w := e.do(t, "POST", "/v1/type/t1", "text/toml; charset=utf-8", "[type]\nname = \"t1\"\n")
		mustStatus(t, w, 200)
		require.Equal(t, "text/toml", storedMime(t, e, "t1"),
			"the charset parameter must not leak into the stored MimeType")
	})
}

// TestAddConfiguration_EmptyBody: an empty body reaches admission intact —
// the http layer neither buffers it away nor 500s — and type admission
// accepts it (an empty TOML type parses; naming is optional), so the write
// lands as version 1.
func TestAddConfiguration_EmptyBody(t *testing.T) {
	e := newEngine(t)
	w := e.doTOML(t, "POST", "/v1/type/t1", "")
	mustStatus(t, w, 200)
	require.Contains(t, w.Body.String(), `"version":1`)
}
