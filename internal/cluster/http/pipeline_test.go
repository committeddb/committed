package http_test

import (
	"encoding/json"
	"errors"
	httpgo "net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/http"
)

func ingestableTOML(topic string) []byte {
	return []byte("[ingestable]\ntype = \"sql\"\n\n[sql]\ntopic = \"" + topic + "\"\n")
}

func syncableTOML(topic string) []byte {
	return []byte("[syncable]\ntype = \"sql\"\n\n[sql]\ntopic = \"" + topic + "\"\n")
}

// typeConfigs builds the minimal type configs the handler checks for existence —
// it only matches on id, it doesn't parse the type body.
func typeConfigs(ids ...string) []*cluster.Configuration {
	cfgs := make([]*cluster.Configuration, 0, len(ids))
	for _, id := range ids {
		cfgs = append(cfgs, &cluster.Configuration{ID: id, MimeType: "text/toml", Data: []byte("[type]\n")})
	}
	return cfgs
}

// pipeSync mirrors the http.PipelineSyncableStatus JSON shape for tests that
// assert across several consumers.
type pipeSync struct {
	ID              string `json:"id"`
	CheckpointIndex uint64 `json:"checkpointIndex"`
	Lag             uint64 `json:"lag"`
	CaughtUp        bool   `json:"caughtUp"`
	Error           string `json:"error"`
}

func findSync(syncs []pipeSync, id string) (pipeSync, bool) {
	for _, s := range syncs {
		if s.ID == id {
			return s, true
		}
	}
	return pipeSync{}, false
}

// GET /type/{id}/pipeline resolves the producing ingestable for the topic, finds
// only the syncables consuming it, and composes the ingest status with each
// consumer's lag — all from the topic (type id) alone.
func TestGetPipelineStatus_ComposesLinkage(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "movie-ingest", MimeType: "text/toml", Data: ingestableTOML("movie")},
		{ID: "other-ingest", MimeType: "text/toml", Data: ingestableTOML("other")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-card", MimeType: "text/toml", Data: syncableTOML("movie")},
		{ID: "unrelated", MimeType: "text/toml", Data: syncableTOML("other")},
	}, nil)
	lag := uint64(5)
	fake.IngestableStatusReturns(cluster.IngestableStatus{
		WorkerState: cluster.WorkerStateRunning,
		Phase:       "streaming", Position: "0/ABCD", Lag: &lag, CaughtUp: false,
	}, nil)
	fake.SyncableProgressReturns(90, 100, nil) // checkpoint 90, head 100 → lag 10

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)

	var body struct {
		Topic      string `json:"topic"`
		Ingestable string `json:"ingestable"`
		HeadIndex  uint64 `json:"headIndex"`
		Ingest     *struct {
			Phase    string `json:"phase"`
			CaughtUp bool   `json:"caughtUp"`
		} `json:"ingest"`
		Syncables []pipeSync `json:"syncables"`
		CaughtUp  bool       `json:"caughtUp"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	require.Equal(t, "movie", body.Topic)
	require.Equal(t, "movie-ingest", body.Ingestable, "the ingestable producing this topic is resolved server-side")
	require.Equal(t, uint64(100), body.HeadIndex)
	require.NotNil(t, body.Ingest)
	require.Equal(t, "streaming", body.Ingest.Phase)

	// Only the consumer of the "movie" topic is included — the linkage filters
	// out the unrelated syncable.
	require.Len(t, body.Syncables, 1)
	require.Equal(t, "movie-card", body.Syncables[0].ID)
	require.Equal(t, uint64(90), body.Syncables[0].CheckpointIndex)
	require.Equal(t, uint64(10), body.Syncables[0].Lag)
	require.False(t, body.Syncables[0].CaughtUp)

	require.False(t, body.CaughtUp, "end-to-end is not caught up: ingest behind and the syncable lags")
}

// CaughtUp is true only when ingest AND every consumer are caught up.
func TestGetPipelineStatus_EndToEndCaughtUp(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("t"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "i1", MimeType: "text/toml", Data: ingestableTOML("t")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "s1", MimeType: "text/toml", Data: syncableTOML("t")},
	}, nil)
	zero := uint64(0)
	fake.IngestableStatusReturns(cluster.IngestableStatus{WorkerState: cluster.WorkerStateRunning, Phase: "streaming", Lag: &zero, CaughtUp: true}, nil)
	fake.SyncableProgressReturns(100, 100, nil) // checkpoint == head → lag 0

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/t/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	var body struct {
		CaughtUp  bool `json:"caughtUp"`
		Syncables []struct {
			CaughtUp bool `json:"caughtUp"`
		} `json:"syncables"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.True(t, body.CaughtUp)
	require.Len(t, body.Syncables, 1)
	require.True(t, body.Syncables[0].CaughtUp)
}

// Fan-out: when several syncables consume the same topic, every one appears as
// its own entry with its own checkpoint/lag/caughtUp, and the pipeline's overall
// caughtUp is the AND across all of them — one lagging consumer makes the whole
// pipeline not caught up.
func TestGetPipelineStatus_MultipleConsumersFanOut(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "movie-ingest", MimeType: "text/toml", Data: ingestableTOML("movie")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-card", MimeType: "text/toml", Data: syncableTOML("movie")},
		{ID: "movie-webhook", MimeType: "text/toml", Data: syncableTOML("movie")},
		{ID: "movie-search", MimeType: "text/toml", Data: syncableTOML("movie")},
		{ID: "unrelated", MimeType: "text/toml", Data: syncableTOML("other")},
	}, nil)
	zero := uint64(0)
	fake.IngestableStatusReturns(cluster.IngestableStatus{WorkerState: cluster.WorkerStateRunning, Phase: "streaming", Lag: &zero, CaughtUp: true}, nil)
	// Head is 100 for everyone; only movie-webhook is behind (checkpoint 70).
	fake.SyncableProgressStub = func(id string) (uint64, uint64, error) {
		const head = uint64(100)
		switch id {
		case "movie-webhook":
			return 70, head, nil // lag 30
		case "movie-card", "movie-search":
			return 100, head, nil // caught up
		default:
			return 0, head, nil // head sentinel (the topic id)
		}
	}

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	var body struct {
		HeadIndex uint64     `json:"headIndex"`
		Syncables []pipeSync `json:"syncables"`
		CaughtUp  bool       `json:"caughtUp"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, uint64(100), body.HeadIndex)

	// All three consumers of "movie" appear; the unrelated syncable is filtered.
	require.Len(t, body.Syncables, 3)
	_, unrelated := findSync(body.Syncables, "unrelated")
	require.False(t, unrelated, "syncable on another topic must not appear")

	card, ok := findSync(body.Syncables, "movie-card")
	require.True(t, ok)
	require.Equal(t, uint64(0), card.Lag)
	require.True(t, card.CaughtUp)

	webhook, ok := findSync(body.Syncables, "movie-webhook")
	require.True(t, ok)
	require.Equal(t, uint64(30), webhook.Lag)
	require.False(t, webhook.CaughtUp)

	search, ok := findSync(body.Syncables, "movie-search")
	require.True(t, ok)
	require.Equal(t, uint64(0), search.Lag)
	require.True(t, search.CaughtUp)

	require.False(t, body.CaughtUp, "one lagging consumer makes the whole pipeline not caught up")
}

// Fail loud: a consumer whose progress can't be read is listed with its error
// rather than silently dropped, and forces the pipeline's caughtUp false — an
// unknown consumer can't be assumed at rest. The data head still resolves.
func TestGetPipelineStatus_ConsumerProgressErrorSurfaced(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "movie-ingest", MimeType: "text/toml", Data: ingestableTOML("movie")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-card", MimeType: "text/toml", Data: syncableTOML("movie")},
		{ID: "movie-broken", MimeType: "text/toml", Data: syncableTOML("movie")},
	}, nil)
	zero := uint64(0)
	fake.IngestableStatusReturns(cluster.IngestableStatus{WorkerState: cluster.WorkerStateRunning, Phase: "streaming", Lag: &zero, CaughtUp: true}, nil)
	fake.SyncableProgressStub = func(id string) (uint64, uint64, error) {
		const head = uint64(100)
		switch id {
		case "movie-card":
			return 100, head, nil // caught up
		case "movie-broken":
			return 0, 0, errors.New("storage read failed")
		default:
			return 0, head, nil // head sentinel (the topic id)
		}
	}

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	var body struct {
		HeadIndex uint64     `json:"headIndex"`
		Syncables []pipeSync `json:"syncables"`
		CaughtUp  bool       `json:"caughtUp"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	// The head still resolves even though a consumer errored.
	require.Equal(t, uint64(100), body.HeadIndex)
	require.Len(t, body.Syncables, 2)

	card, ok := findSync(body.Syncables, "movie-card")
	require.True(t, ok)
	require.Empty(t, card.Error)
	require.True(t, card.CaughtUp)

	broken, ok := findSync(body.Syncables, "movie-broken")
	require.True(t, ok)
	require.Equal(t, "storage read failed", broken.Error, "the consumer's read error is surfaced, not dropped")
	require.False(t, broken.CaughtUp)

	require.False(t, body.CaughtUp, "a consumer with unknown progress means the pipeline can't be at rest")
}

// redactedProgressErr is a cluster.RedactedError whose full text embeds
// connection identity (as a wrapped driver error would), while its
// RedactedMessage is PII-free — so the pipeline handler can be checked for
// leaking the former.
type redactedProgressErr struct{}

func (redactedProgressErr) Error() string {
	return "dial tcp: user=admin password=hunter2 host=10.0.0.5:5432 database=orders"
}

func (redactedProgressErr) RedactedMessage() string {
	return "progress read failed (detail in node logs)"
}

// TestGetPipelineStatus_ConsumerProgressErrorRedacted: a consumer's progress
// error that wraps a driver error (a cluster.RedactedError echoing connection
// identity) must surface as its PII-free RedactedMessage in the response, never
// the raw text — GET /type/{id}/pipeline is a plain read anyone can hit.
func TestGetPipelineStatus_ConsumerProgressErrorRedacted(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "movie-ingest", MimeType: "text/toml", Data: ingestableTOML("movie")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-broken", MimeType: "text/toml", Data: syncableTOML("movie")},
	}, nil)
	zero := uint64(0)
	fake.IngestableStatusReturns(cluster.IngestableStatus{WorkerState: cluster.WorkerStateRunning, Phase: "streaming", Lag: &zero, CaughtUp: true}, nil)
	fake.SyncableProgressStub = func(id string) (uint64, uint64, error) {
		if id == "movie-broken" {
			return 0, 0, redactedProgressErr{}
		}
		return 0, 100, nil // head sentinel
	}

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), "hunter2", "response leaked the password")
	require.NotContains(t, w.Body.String(), "10.0.0.5", "response leaked the host")
	require.NotContains(t, w.Body.String(), "admin", "response leaked the user")

	var body struct {
		Syncables []pipeSync `json:"syncables"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	broken, ok := findSync(body.Syncables, "movie-broken")
	require.True(t, ok)
	require.Equal(t, "progress read failed (detail in node logs)", broken.Error)
}

// A topic fed by direct proposals (no ingestable) still composes: there is no
// producer section, just the head and the consuming syncables.
func TestGetPipelineStatus_ProposalFedTopicHasNoProducer(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{}, nil) // nothing produces "movie"
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-card", MimeType: "text/toml", Data: syncableTOML("movie")},
	}, nil)
	fake.SyncableProgressReturns(100, 100, nil) // caught up

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	var body struct {
		Topic       string           `json:"topic"`
		Ingestable  string           `json:"ingestable"`
		Ingest      *json.RawMessage `json:"ingest"`
		IngestError string           `json:"ingestError"`
		Syncables   []pipeSync       `json:"syncables"`
		CaughtUp    bool             `json:"caughtUp"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	require.Equal(t, "movie", body.Topic)
	require.Empty(t, body.Ingestable, "no ingestable produces this topic")
	require.Nil(t, body.Ingest, "the ingest section is omitted when there is no producer")
	require.Empty(t, body.IngestError)
	require.Equal(t, 0, fake.IngestableStatusCallCount(), "no producer means no ingest-status call")
	require.Len(t, body.Syncables, 1)
	require.True(t, body.CaughtUp, "with no producer, caught-up depends only on the consumers")
}

// Fail loud: a producer whose status can't be read on the answering node is
// surfaced as ingestError rather than 404-ing the whole pipeline, and forces
// caughtUp false.
func TestGetPipelineStatus_ProducerNotRunningSurfaced(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns(typeConfigs("movie"), nil)
	fake.IngestablesReturns([]*cluster.Configuration{
		{ID: "movie-ingest", MimeType: "text/toml", Data: ingestableTOML("movie")},
	}, nil)
	fake.SyncablesReturns([]*cluster.Configuration{
		{ID: "movie-card", MimeType: "text/toml", Data: syncableTOML("movie")},
	}, nil)
	fake.IngestableStatusReturns(cluster.IngestableStatus{WorkerState: cluster.WorkerStateRunning}, cluster.ErrIngestableNotRunning)
	fake.SyncableProgressReturns(100, 100, nil) // the consumer itself is caught up

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/movie/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusOK, w.Code)
	var body struct {
		Ingestable  string           `json:"ingestable"`
		Ingest      *json.RawMessage `json:"ingest"`
		IngestError string           `json:"ingestError"`
		CaughtUp    bool             `json:"caughtUp"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	require.Equal(t, "movie-ingest", body.Ingestable, "the producer is still named")
	require.Nil(t, body.Ingest, "no ingest status when the worker isn't running here")
	require.NotEmpty(t, body.IngestError, "the not-running producer is surfaced loudly")
	require.False(t, body.CaughtUp, "an unknown producer means the pipeline can't be at rest")
}

// An unknown topic (no registered type) is a 404 — there is no pipeline to
// compose.
func TestGetPipelineStatus_NotFound(t *testing.T) {
	fake := &clusterfakes.FakeCluster{}
	fake.TypesReturns([]*cluster.Configuration{}, nil)

	h := http.New(fake)
	r := httptest.NewRequest(httpgo.MethodGet, "/v1/type/ghost/pipeline", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	require.Equal(t, httpgo.StatusNotFound, w.Code)
	var er struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &er))
	require.Equal(t, "type_not_found", er.Code, "the 404 is specifically the missing-type path")
}
