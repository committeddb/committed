package http_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/http"
)

// POSTing a syncable under the deprecated "sql-projection" spelling succeeds
// exactly as posted, and the response body carries a warnings[] entry telling
// the operator what to rename. The canonical spelling warns about nothing.
func TestAddSyncable_DeprecatedProjectionSpellingWarns(t *testing.T) {
	h, fake := setupTest()
	fake.ProposeSyncableReturns(nil)
	fake.SyncableVersionsReturns([]cluster.VersionInfo{{Version: 1, Current: true}}, nil)

	body := decodeConfigWrite(t, postSyncable(t, h, "s1", `[syncable]
name = "s1"
type = "sql-projection"
`))
	require.Len(t, body.Warnings, 1)
	require.Contains(t, body.Warnings[0], "deprecated")
	require.Contains(t, body.Warnings[0], `"projection"`)
	require.Contains(t, body.Warnings[0], "[projection]")
}

func TestAddSyncable_CanonicalProjectionSpellingNoWarnings(t *testing.T) {
	h, fake := setupTest()
	fake.ProposeSyncableReturns(nil)
	fake.SyncableVersionsReturns([]cluster.VersionInfo{{Version: 1, Current: true}}, nil)

	body := decodeConfigWrite(t, postSyncable(t, h, "s1", `[syncable]
name = "s1"
type = "projection"
`))
	require.Empty(t, body.Warnings)
}

func postSyncable(t *testing.T, h *http.HTTP, id, toml string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "http://localhost/v1/syncable/"+id, strings.NewReader(toml))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	return w
}
