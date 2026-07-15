package http_test

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

// sqlIngestableConfig is a minimal well-formed ingestable config naming a topic
// — the shape topicsOf reads a topic from ([ingestable].type = "sql" → sql.topic).
func sqlIngestableConfig(id, topic string) *cluster.Configuration {
	return &cluster.Configuration{
		ID:       id,
		MimeType: "text/toml",
		Data:     []byte("[ingestable]\ntype = \"sql\"\n\n[sql]\ntopic = \"" + topic + "\"\n"),
	}
}

// postIngestable POSTs a topic-bearing ingestable config to /v1/ingestable/{id}.
func postIngestable(h *http.HTTP, id, topic string) *httptest.ResponseRecorder {
	body := "[ingestable]\ntype = \"sql\"\n\n[sql]\ntopic = \"" + topic + "\"\n"
	req := httptest.NewRequest(httpgo.MethodPost, "http://localhost/v1/ingestable/"+id, strings.NewReader(body))
	req.Header["Content-Type"] = []string{"text/toml"}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// TestAddIngestable_RejectsSecondIngestableOnSameTopic is the single-authority
// guard's red→green: a topic already produced by one ingestable cannot get a
// second. Two ingestables on one topic each keep an independent reconciling-
// refresh epoch, so one's full-refresh sweep would delete the other's rows.
func TestAddIngestable_RejectsSecondIngestableOnSameTopic(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IngestablesReturns([]*cluster.Configuration{sqlIngestableConfig("orders-ing", "orders")}, nil)
	h := http.New(fake)

	w := postIngestable(h, "orders-ing-2", "orders")

	require.Equal(t, httpgo.StatusBadRequest, w.Code)
	require.Equal(t, 0, fake.ProposeIngestableCallCount(),
		"the conflicting ingestable must be rejected before it is proposed")

	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details struct {
			Field string `json:"field"`
			Issue string `json:"issue"`
		} `json:"details"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, "invalid_ingestable_config", body.Code)
	require.Equal(t, "sql.topic", body.Details.Field)
	require.Contains(t, body.Details.Issue, "orders")
	require.Contains(t, body.Details.Issue, "orders-ing",
		"the message should name the ingestable that already owns the topic")
}

// TestAddIngestable_AllowsDistinctTopic verifies the guard only fires on a
// shared topic — a new ingestable on its own topic is proposed normally.
func TestAddIngestable_AllowsDistinctTopic(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IngestablesReturns([]*cluster.Configuration{sqlIngestableConfig("orders-ing", "orders")}, nil)
	h := http.New(fake)

	w := postIngestable(h, "shipments-ing", "shipments")

	require.Equal(t, httpgo.StatusOK, w.Code)
	require.Equal(t, 1, fake.ProposeIngestableCallCount())
}

// TestAddIngestable_AllowsSameIdReconfigure verifies re-configuring the SAME
// ingestable to its own topic is allowed — it is the same authority, not a
// second one (a config update or a rollback must not trip the guard).
func TestAddIngestable_AllowsSameIdReconfigure(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IngestablesReturns([]*cluster.Configuration{sqlIngestableConfig("orders-ing", "orders")}, nil)
	h := http.New(fake)

	w := postIngestable(h, "orders-ing", "orders")

	require.Equal(t, httpgo.StatusOK, w.Code)
	require.Equal(t, 1, fake.ProposeIngestableCallCount())
}

// TestAddIngestable_FirstOnTopicSucceeds verifies the first ingestable on a
// topic (no existing owner) is proposed normally.
func TestAddIngestable_FirstOnTopicSucceeds(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	h := http.New(fake)

	w := postIngestable(h, "orders-ing", "orders")

	require.Equal(t, httpgo.StatusOK, w.Code)
	require.Equal(t, 1, fake.ProposeIngestableCallCount())
}

// TestAddIngestable_IngestablesReadError500 verifies the guard fails closed on a
// state-read error: it cannot confirm topic uniqueness, so it must not propose.
func TestAddIngestable_IngestablesReadError500(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.IngestablesReturns(nil, errors.New("read failed"))
	h := http.New(fake)

	w := postIngestable(h, "orders-ing", "orders")

	require.Equal(t, httpgo.StatusInternalServerError, w.Code)
	require.Equal(t, 0, fake.ProposeIngestableCallCount())
}
