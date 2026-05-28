//go:build docker

package harness

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/philborlin/committed/e2e/cdc/dataset"
)

// postType creates a committed Type with the given ID (which doubles
// as the topic name on ingestable proposals). Body is the minimal TOML
// — committed accepts a bare [type] section with just a name.
func postType(t *testing.T, id string) {
	t.Helper()
	body := fmt.Sprintf("[type]\nname = %q\n", id)
	postConfig(t, "/type/"+id, body)
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
	postConfig(t, "/ingestable/"+table, b.String())
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

// waitForIngestableReady polls Postgres until the replication slot
// exists AND is active (a consumer is attached). Without this gate,
// mutations issued immediately after postIngestable race the ingestable
// startup and produce a flaky "lost initial events" failure mode.
//
// Mirrors the waitForSlot helper in
// internal/cluster/ingestable/sql/postgres/postgres_test.go.
func (h *Harness) waitForIngestableReady(t *testing.T, slot string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		// pg_stat_replication.state == 'streaming' is the right gate
		// for "snapshot done AND streaming live." The slot's `active`
		// flag flips true when the dialect connects (before snapshot),
		// not after, so racing on `active` lets test mutations slip
		// into the dialect's snapshot REPEATABLE READ batches and
		// double-count as both snapshot+streaming proposals.
		var state string
		err := h.pgConn.QueryRow(h.ctx, `
			SELECT s.state
			FROM pg_stat_replication s
			JOIN pg_replication_slots r ON r.active_pid = s.pid
			WHERE r.slot_name = $1`, slot,
		).Scan(&state)
		if err == nil && state == "streaming" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Diagnostic: dump full slot state on timeout.
	var exists bool
	_ = h.pgConn.QueryRow(h.ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot,
	).Scan(&exists)
	if !exists {
		t.Fatalf("ingestable slot %q was never created — supervisor likely never spawned the dialect", slot)
	}
	var state string
	_ = h.pgConn.QueryRow(h.ctx, `
		SELECT s.state FROM pg_stat_replication s
		JOIN pg_replication_slots r ON r.active_pid = s.pid
		WHERE r.slot_name = $1`, slot).Scan(&state)
	t.Fatalf("ingestable slot %q never reached state=streaming (last observed state=%q)", slot, state)
}
