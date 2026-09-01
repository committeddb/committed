package http_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/http"
)

// The pipeline endpoint composed against the real engine: the linkage
// (producing ingestable → topic → consuming syncables) resolves from the
// stored configs' own bytes, and the numbers come from the same engine
// reads the single-resource endpoints use. Fail-loud legs a live fixture
// cannot produce (a not-running producer, a failed or PII-bearing progress
// read) are table-tested against the extracted composition functions in
// pipeline_compose_test.go.

func getPipeline(t *testing.T, e *engine, topic string) http.PipelineStatusResponse {
	t.Helper()
	w := e.doEmpty(t, "GET", "/v1/type/"+topic+"/pipeline")
	require.Equal(t, 200, w.Code, w.Body.String())
	var resp http.PipelineStatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

// TestGetPipelineStatus_EndToEndCaughtUp: producer + consumer at rest — the
// ingestable is named with its status, the consumer reports caught up, and
// the whole pipeline reads at rest.
func TestGetPipelineStatus_EndToEndCaughtUp(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	lag := uint64(0)
	e.ingest.StatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning, Phase: "streaming",
		Lag: &lag, LagUnit: cluster.LagUnitBytes, CaughtUp: true,
	}, nil)
	e.addRecorderIngestable(t, "ing-1", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")
	e.proposeRow(t, "photos", "k1")
	e.awaitStatus(t, "rec-1", func(m map[string]any) bool {
		caught, _ := m["caughtUp"].(bool)
		return caught
	}, "consumer caught up")

	resp := getPipeline(t, e, "photos")
	require.Equal(t, "photos", resp.Topic)
	require.Equal(t, "ing-1", resp.Ingestable)
	require.NotNil(t, resp.Ingest, "the producer's status rides along")
	require.True(t, resp.Ingest.CaughtUp)
	require.Len(t, resp.Syncables, 1)
	require.Equal(t, "rec-1", resp.Syncables[0].ID)
	require.True(t, resp.Syncables[0].CaughtUp)
	require.NotZero(t, resp.HeadIndex)
	require.True(t, resp.CaughtUp, "producer and consumer at rest — the pipeline is at rest")
}

// TestGetPipelineStatus_MultipleConsumersFanOut: two consumers of the same
// topic both appear; a lagging one (its sink wedged) forces the pipeline
// out of rest while the healthy one stays caught up.
func TestGetPipelineStatus_MultipleConsumersFanOut(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")
	e.addRecorderSyncable(t, "rec-2", "photos")
	e.proposeRow(t, "photos", "k1")

	require.Eventually(t, func() bool {
		resp := getPipeline(t, e, "photos")
		if len(resp.Syncables) != 2 {
			return false
		}
		return resp.Syncables[0].CaughtUp && resp.Syncables[1].CaughtUp && resp.CaughtUp
	}, 15*time.Second, 10*time.Millisecond, "both consumers never converged")

	// Wedge the shared sink and add another row: checkpoints stall behind
	// the new head and the pipeline leaves rest.
	e.sink.setErr(fmt.Errorf("hold"))
	e.proposeRow(t, "photos", "k2")
	require.Eventually(t, func() bool {
		resp := getPipeline(t, e, "photos")
		return !resp.CaughtUp
	}, 15*time.Second, 10*time.Millisecond, "a lagging consumer must pull the pipeline out of rest")
}

// TestGetPipelineStatus_ProposalFedTopicHasNoProducer: a topic fed by
// direct proposals lists its consumers with no ingestable named.
func TestGetPipelineStatus_ProposalFedTopicHasNoProducer(t *testing.T) {
	e := newEngine(t)
	e.addType(t, "photos", "photos")
	e.addRecorderSyncable(t, "rec-1", "photos")

	resp := getPipeline(t, e, "photos")
	require.Empty(t, resp.Ingestable)
	require.Nil(t, resp.Ingest)
	require.Empty(t, resp.IngestError)
	require.Len(t, resp.Syncables, 1)
}

// TestGetPipelineStatus_NotFound: the topic must be a registered type.
func TestGetPipelineStatus_NotFound(t *testing.T) {
	e := newEngine(t)
	requireEnvelope(t, e.doEmpty(t, "GET", "/v1/type/nope/pipeline"), 404, "type_not_found")
}
