package http_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// POSTing a syncable under the deprecated "sql-projection" spelling succeeds
// exactly as posted, and the response body carries a warnings[] entry telling
// the operator what to rename. The canonical spelling warns about nothing.
// Runs against the real engine (the fixture admits both spellings through
// the recorder sink); the warning itself is the HTTP layer's.
func TestAddSyncable_DeprecatedProjectionSpellingWarns(t *testing.T) {
	e := newEngine(t)
	body := decodeConfigWrite(t, postSyncable(t, e, "s1",
		"[syncable]\nname = \"s1\"\ntype = \"sql-projection\"\n"))
	require.Len(t, body.Warnings, 1)
	require.Contains(t, body.Warnings[0], "deprecated")
	require.Contains(t, body.Warnings[0], `"projection"`)
	require.Contains(t, body.Warnings[0], "[projection]")
}

func TestAddSyncable_CanonicalProjectionSpellingNoWarnings(t *testing.T) {
	e := newEngine(t)
	body := decodeConfigWrite(t, postSyncable(t, e, "s1",
		"[syncable]\nname = \"s1\"\ntype = \"projection\"\n"))
	require.Empty(t, body.Warnings)
}

func postSyncable(t *testing.T, e *engine, id, toml string) *httptest.ResponseRecorder {
	t.Helper()
	w := e.doTOML(t, "POST", "/v1/syncable/"+id, toml)
	require.Equal(t, 200, w.Code, w.Body.String())
	return w
}
