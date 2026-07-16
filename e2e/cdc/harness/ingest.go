//go:build docker

package harness

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/e2e/cdc/dataset"
)

// postType creates a committed Type with the given ID (which doubles
// as the topic name on ingestable proposals). Body is the minimal TOML
// — committed accepts a bare [type] section with just a name.
func postType(t *testing.T, id string) {
	t.Helper()
	body := fmt.Sprintf("[type]\nname = %q\n", id)
	postConfig(t, "/v1/type/"+id, body)
}

// postIngestable registers one Postgres ingestable per TPC-H table.
// Each table gets its own ingestable so that proposals land in
// table-named topics — the oracle correlates by Entity.Type.ID = table
// name, which is trivial when one ingestable = one topic = one table.
func postIngestable(t *testing.T, table, pgConnStr, slotName, pubName string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "[ingestable]\nname = %q\ntype = \"sql\"\n\n", table)
	fmt.Fprintf(&b, "[sql]\ndialect = \"postgres\"\n")
	fmt.Fprintf(&b, "topic = %q\n", table)
	fmt.Fprintf(&b, "connectionString = %q\n", pgConnStr)
	fmt.Fprintf(&b, "primaryKey = %q\n", dataset.PrimaryKey(table))
	// Schema-qualified table name — the natural form, matching the
	// TOML examples in the repo. ensurePublication uses quoteTable
	// (postgres.go:389) so "public.region" correctly becomes the
	// schema.table reference "public"."region".
	fmt.Fprintf(&b, "tables = [\"public.%s\"]\n\n", table)
	fmt.Fprintf(&b, "[sql.postgres]\nslot_name = %q\npublication = %q\n\n", slotName, pubName)
	for _, col := range dataset.Columns(table) {
		fmt.Fprintf(&b, "[[sql.mappings]]\njsonName = %q\ncolumn = %q\n\n", col, col)
	}
	postConfig(t, "/v1/ingestable/"+table, b.String())
}

// postConfig POSTs a TOML configuration body to the given path. Fails
// the test if the response is not 2xx — committed returns the new ID
// in the body, which we discard (we already know it).
func postConfig(t *testing.T, path, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, committedURL(path), bytes.NewBufferString(body))
	require.NoError(t, err, "build request")
	req.Header.Set("Content-Type", "text/toml")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "POST %s", path)
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: %d %s\n----- request body -----\n%s", path, resp.StatusCode, string(b), body)
	}
	t.Logf("POST %s: %d %s", path, resp.StatusCode, string(b))
}

// deleteConfig issues DELETE against a versioned config resource and fails the
// test on a non-2xx response. Used to remove an ingestable so a later re-POST
// starts fresh from a full snapshot (the reconciling-refresh recreate path).
func deleteConfig(t *testing.T, path string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, committedURL(path), nil)
	require.NoError(t, err, "build request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "DELETE %s", path)
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("DELETE %s: %d %s", path, resp.StatusCode, string(b))
	}
	t.Logf("DELETE %s: %d %s", path, resp.StatusCode, string(b))
}

// Readiness gating moved to postgresEngine.WaitReady (postgres_engine.go).
