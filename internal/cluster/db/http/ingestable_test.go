package http_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// The database + ingestable config groups run against the real engine
// (enginetest_test.go): "recorder"-kind parsers admit real configs and the
// controllable fake ingestable feeds the real status path. Rendering
// details a route test would restate (lag-unit omission, census shapes,
// the degraded-cause 404 message) are pinned in-package against the free
// renderer functions in ingestable_render_test.go; the census PIPELINE
// (worker → replicated census → this route) is covered end to end by
// db/census_test.go against this same server.

// TestDatabaseLifecycle: propose, list, versions, version read, rollback —
// the config CRUD surface for databases, real end to end.
func TestDatabaseLifecycle(t *testing.T) {
	e := newEngine(t)

	e.addRecorderDatabase(t, "db-1")
	mustStatus(t, e.doTOML(t, "POST", "/v1/database/db-1",
		"[database]\nname = \"db-1\"\ntype = \"recorder\"\n[recorder]\nnote = \"v2\"\n"), 200)

	var listing []struct {
		ID string `json:"id"`
	}
	e.getJSON(t, "/v1/database", &listing)
	require.Len(t, listing, 1)
	require.Equal(t, "db-1", listing[0].ID)

	var versions []struct {
		Version int `json:"version"`
	}
	e.getJSON(t, "/v1/database/db-1/versions", &versions)
	require.Len(t, versions, 2)

	var got struct {
		Data string `json:"data"`
	}
	e.getJSON(t, "/v1/database/db-1/versions/1", &got)
	require.Contains(t, got.Data, "type = \"recorder\"")

	mustStatus(t, e.doEmpty(t, "POST", "/v1/database/db-1/rollback?to=1"), 200)
	e.getJSON(t, "/v1/database/db-1/versions/3", &got)
	require.NotContains(t, got.Data, "v2", "rollback restores version 1's content")

	// The resource-specific 404 code on an unknown id's history.
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/database/missing/versions"), 404, "database_not_found")
}

// TestIngestableLifecycle: the same CRUD surface for ingestables, plus the
// resource-specific 404 code.
func TestIngestableLifecycle(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")

	e.addRecorderIngestable(t, "ing-1", "photos")

	var listing []struct {
		ID string `json:"id"`
	}
	e.getJSON(t, "/v1/ingestable", &listing)
	require.Len(t, listing, 1)
	require.Equal(t, "ing-1", listing[0].ID)

	var versions []struct {
		Version int `json:"version"`
	}
	e.getJSON(t, "/v1/ingestable/ing-1/versions", &versions)
	require.Len(t, versions, 1)

	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/ingestable/missing/versions"), 404, "ingestable_not_found")
}

// TestDeleteIngestable: a real delete — the config and its checkpoint go,
// and the listing no longer carries the id. The route is leader-pinned; the
// single node is the leader, so it serves locally (where the source-side
// teardown runs).
func TestDeleteIngestable(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderIngestable(t, "ing-1", "photos")

	w := e.doEmpty(t, "DELETE", "/v1/ingestable/ing-1")
	mustStatus(t, w, 200)
	var body struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "ing-1", body.ID)

	var listing []struct {
		ID string `json:"id"`
	}
	e.getJSON(t, "/v1/ingestable", &listing)
	require.Empty(t, listing)
}

// An id-less DELETE never matches the route — the defensive guard in the
// handler stays unreachable through the router, and the router answers with
// its own envelope.
func TestDeleteIngestable_EmptyID(t *testing.T) {
	e := newEngine(t)
	w := e.doEmpty(t, "DELETE", "/v1/ingestable/")
	require.NotEqual(t, 200, w.Code)
}

// TestGetIngestableStatus: the real status path — the worker (held alive by
// the fixture's blocking Ingest stub) answers through the engine with the
// dialect's own view, and the lag unit rides next to a non-null lag.
func TestGetIngestableStatus(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	lag := uint64(8192)
	e.ingest.StatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning,
		Phase:       "streaming",
		Position:    "binlog.000004:1547",
		Lag:         &lag,
		LagUnit:     cluster.LagUnitBytes,
		CaughtUp:    true,
	}, nil)
	e.addRecorderIngestable(t, "ing-1", "photos")

	w := e.doEmpty(t, "GET", "/v1/ingestable/ing-1/status")
	mustStatus(t, w, 200)
	require.Contains(t, w.Body.String(), `"lagUnit":"bytes"`)
	require.Contains(t, w.Body.String(), `"position":"binlog.000004:1547"`)
	require.Contains(t, w.Body.String(), `"caughtUp":true`)
}

// TestGetIngestableStatus_UnknownIs404: the existence gate — a typo'd id
// must not read as "exists, but no worker here".
func TestGetIngestableStatus_UnknownIs404(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/ingestable/nope/status"), 404, "not_found")
}
