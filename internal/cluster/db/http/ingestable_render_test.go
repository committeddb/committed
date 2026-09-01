package http

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// Rendering tables for the ingestable status surface — the shapes a route
// test would only restate, pinned directly against the free renderer
// functions. The route itself (real worker, real engine) is covered in
// ingestable_test.go, and the census PIPELINE end to end in db/census_test.

// TestToIngestableStatusResponse_LagUnit: lagUnit names the scale lag is on
// — load-bearing now that MySQL reports transactions under GTID positioning
// but bytes under the file:pos fallback: present next to a non-null lag,
// omitted with a null one (a unit without a number is noise).
func TestToIngestableStatusResponse_LagUnit(t *testing.T) {
	lag := uint64(8192)
	resp := toIngestableStatusResponse(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning, Phase: "streaming",
		Lag: &lag, LagUnit: cluster.LagUnitBytes,
	})
	bs, err := json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(bs), `"lagUnit":"bytes"`)

	resp = toIngestableStatusResponse(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning, Phase: "streaming",
	})
	bs, err = json.Marshal(resp)
	require.NoError(t, err)
	require.Contains(t, string(bs), `"lag":null`)
	require.False(t, strings.Contains(string(bs), "lagUnit"),
		"lagUnit must be omitted when lag is null")
}

// TestToCensusResponse pins the census section: shapes in first-seen order,
// the derived path view, and a rendered draft schema.
func TestToCensusResponse(t *testing.T) {
	tc := &cluster.TopicCensus{}
	require.NoError(t, tc.Fold([]byte(`{"caption":"a","size":1}`), cluster.CensusOptions{}))
	require.NoError(t, tc.Fold([]byte(`{"caption":"b","ai":{"m":"x"}}`), cluster.CensusOptions{}))

	out := toCensusResponse(&cluster.IngestableCensus{
		ID: "ing-1", RefreshEpoch: 1,
		Topics: map[string]*cluster.TopicCensus{"photos": tc},
	})
	photos := out["photos"]
	require.NotNil(t, photos)
	require.Equal(t, uint64(2), photos.Rows)
	require.Len(t, photos.Shapes, 2)
	require.LessOrEqual(t, photos.Shapes[0].FirstRow, photos.Shapes[1].FirstRow,
		"shapes render in first-seen order")
	require.NotEmpty(t, photos.Paths)
	require.Contains(t, photos.DraftSchema, `"additionalProperties": false`)
	require.Contains(t, photos.DraftSchema, `"ai"`)
}

// TestIngestableNotRunningMessage: when the answering node's degraded-config
// record explains the missing worker, the 404 names the (redacted) build
// error; other kinds' and ids' records never leak in.
func TestIngestableNotRunningMessage(t *testing.T) {
	errs := []cluster.ConfigBuildError{
		{Kind: "syncable", ID: "ing-1", Error: "wrong kind — must not match"},
		{Kind: "ingestable", ID: "other", Error: "wrong id — must not match"},
		{Kind: "ingestable", ID: "ing-1", Error: "interpolate: variable SOURCE_DSN not set"},
	}
	require.Equal(t,
		"the config failed to build on the node that answered (no worker started): interpolate: variable SOURCE_DSN not set",
		ingestableNotRunningMessage("ing-1", errs))
	require.Equal(t,
		"no ingestable worker is running for this id on the node that answered",
		ingestableNotRunningMessage("ing-2", errs))
}
