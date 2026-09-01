package http_test

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

// A migration-only in-place type edit (schema + version unchanged, only the
// [migration] transform changed) must return a body advisory: the fix reaches
// only new Actuals, so already-synced history needs a projection rebuild. The
// handler detects it from before/after ResolveType snapshots bracketing the
// propose.
func TestAddType_MigrationEditAdvisoryInResponseBody(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturnsOnCall(0, &cluster.Type{ID: "t1", Version: 1, Schema: []byte("s"), Migration: []byte("old jq")}, nil)
	fake.ProposeTypeReturns(nil)
	fake.TypeVersionsReturns([]cluster.VersionInfo{{Version: 1, Current: true}}, nil)
	fake.ResolveTypeReturnsOnCall(1, &cluster.Type{ID: "t1", Version: 1, Schema: []byte("s"), Migration: []byte("new jq")}, nil)
	fake.MigrationEditDependentsReturns([]cluster.DependentSyncable{{ID: "mirror", Name: "mirror"}})

	body := decodeConfigWrite(t, postType(t, h, "t1"))
	require.Equal(t, "t1", body.ID)
	require.Contains(t, body.Advisory, "already-synced",
		"a migration-only in-place edit must carry the rebuild advisory")
	require.Contains(t, body.Advisory, "read-models.md",
		"the advisory must point the operator at the how-to")
	require.Contains(t, body.Advisory, "rematerialize",
		"the advisory names the one-verb fix")
	require.Len(t, body.MigrationEditDependents, 1,
		"the advisory carries the structured dependents to re-materialize")
	require.Equal(t, "mirror", body.MigrationEditDependents[0].ID)
}

// A schema change forces a new version with its own migration — not the
// silent-history case — so no advisory.
func TestAddType_NoAdvisoryOnSchemaBump(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturnsOnCall(0, &cluster.Type{ID: "t1", Version: 1, Schema: []byte("s1"), Migration: []byte("m")}, nil)
	fake.ProposeTypeReturns(nil)
	fake.TypeVersionsReturns([]cluster.VersionInfo{{Version: 2, Current: true}}, nil)
	fake.ResolveTypeReturnsOnCall(1, &cluster.Type{ID: "t1", Version: 2, Schema: []byte("s2"), Migration: []byte("m2")}, nil)

	require.Empty(t, decodeConfigWrite(t, postType(t, h, "t1")).Advisory)
}

// A brand-new type (no prior version) carries no advisory.
func TestAddType_NoAdvisoryOnNewType(t *testing.T) {
	h, fake := setupTest()

	fake.ResolveTypeReturnsOnCall(0, nil, nil) // before: no existing type
	fake.ProposeTypeReturns(nil)
	fake.TypeVersionsReturns([]cluster.VersionInfo{{Version: 1, Current: true}}, nil)
	fake.ResolveTypeReturnsOnCall(1, &cluster.Type{ID: "t1", Version: 1, Schema: []byte("s")}, nil)

	require.Empty(t, decodeConfigWrite(t, postType(t, h, "t1")).Advisory)
}

func postType(t *testing.T, h *http.HTTP, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "http://localhost/v1/type/"+id, strings.NewReader("[type]\nname = \"x\"\n"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code)
	return w
}

func decodeConfigWrite(t *testing.T, w *httptest.ResponseRecorder) http.ConfigWriteResponse {
	t.Helper()
	b, err := io.ReadAll(w.Body)
	require.NoError(t, err)
	var out http.ConfigWriteResponse
	require.NoError(t, json.Unmarshal(b, &out), "body: %s", string(b))
	return out
}
