package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// The migration-edit advisory, real end to end: an in-place [migration]
// transform edit (same version, same schema, different migration bytes)
// must warn the operator and name the always-current consumers whose
// already-synced rows keep the OLD migration's output; a schema bump (its
// own version + migration) and a brand-new type warrant no advisory.

type typeWriteResponse struct {
	ID                      string `json:"id"`
	Version                 int    `json:"version"`
	Advisory                string `json:"advisory,omitempty"`
	MigrationEditDependents []struct {
		ID string `json:"id"`
	} `json:"migrationEditDependents,omitempty"`
}

func postType(t *testing.T, e *engine, id, body string) typeWriteResponse {
	t.Helper()
	w := e.doTOML(t, "POST", "/v1/type/"+id, body)
	require.Equal(t, 200, w.Code, w.Body.String())
	var resp typeWriteResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

func TestAddType_MigrationEditAdvisoryInResponseBody(t *testing.T) {
	e := newEngine(t)

	// v1 plain, v2 with a schema (declaring the identity migration), then an
	// in-place edit that changes ONLY the [migration] transform — same
	// version, same schema.
	postType(t, e, "photos", "[type]\nname = \"photos\"\n")
	v2 := "[type]\nname = \"photos\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n[migration]\nnone = true\n"
	resp := postType(t, e, "photos", v2)
	require.Equal(t, 2, resp.Version)
	require.Empty(t, resp.Advisory, "a schema bump carries its own migration — no advisory")

	// The always-current consumer the advisory must name.
	e.addAlwaysCurrentRecorder(t, "rec-1", "photos")

	edited := "[type]\nname = \"photos\"\nschemaType = \"JSONSchema\"\nschema = '{\"type\":\"object\"}'\n[migration]\ntransform = '.x'\n"
	resp = postType(t, e, "photos", edited)
	require.Equal(t, 2, resp.Version, "a migration-only edit stays in place at the same version")
	require.NotEmpty(t, resp.Advisory, "an in-place transform edit must carry the advisory")
	require.Contains(t, resp.Advisory, "/rematerialize", "the advisory names the remedy")
	require.Len(t, resp.MigrationEditDependents, 1)
	require.Equal(t, "rec-1", resp.MigrationEditDependents[0].ID)
}

func TestAddType_NoAdvisoryOnNewType(t *testing.T) {
	e := newEngine(t)
	resp := postType(t, e, "photos", "[type]\nname = \"photos\"\n")
	require.Equal(t, 1, resp.Version)
	require.Empty(t, resp.Advisory)
	require.Empty(t, resp.MigrationEditDependents)
}
