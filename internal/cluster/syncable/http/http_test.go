package http_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

var (
	testType  = &cluster.Type{ID: "test-topic", Name: "test", Version: 1}
	otherType = &cluster.Type{ID: "other-topic", Name: "other", Version: 1}
)

func newConfig(url string) *synchttp.Config {
	return &synchttp.Config{
		Topic:     "test-topic",
		URL:       url,
		Method:    "POST",
		TimeoutMs: 5000,
	}
}

func newActual(entities ...*cluster.Entity) *cluster.Actual {
	return &cluster.Actual{Entities: entities}
}

func newEntity(t *cluster.Type, key string, data any) *cluster.Entity {
	bs, _ := json.Marshal(data)
	return &cluster.Entity{
		Type: t,
		Key:  []byte(key),
		Data: bs,
	}
}

type webhookBody struct {
	Entities []webhookEntity `json:"entities"`
}

type webhookEntity struct {
	Op         string          `json:"op"`
	Key        string          `json:"key"`
	Type       webhookType     `json:"type"`
	Data       json.RawMessage `json:"data"`
	Generation uint64          `json:"generation"`
}

type webhookType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// TestSync_TransportError_RedactsURLSecret pins the webhook secret-leak fix: a
// transport failure returns a *url.Error whose text embeds the full request URL —
// path included, where Slack/Discord/?token= webhooks carry the secret. The value
// the dead-letter/stuck record and the status/errors/replay APIs persist and expose
// (RedactedMessage, via safeDeadLetterMessage) must not contain that secret.
func TestSync_TransportError_RedactsURLSecret(t *testing.T) {
	const secret = "SUPERSECRETTOKEN123"

	// A closed server → connection refused → client.Do returns a *url.Error whose
	// text includes the full secret-bearing URL.
	ts := httptest.NewServer(nethttp.HandlerFunc(func(nethttp.ResponseWriter, *nethttp.Request) {}))
	url := ts.URL + "/services/T000/B000/" + secret
	ts.Close()

	cfg := newConfig(url)
	cfg.TimeoutMs = 1000
	s := synchttp.New(cfg)

	_, err := s.Sync(context.Background(), newActual(newEntity(testType, "k", map[string]any{"x": 1})))
	require.Error(t, err)

	// Reproduce exactly what the dead-letter/status path persists + exposes:
	// RedactedMessage() when the error is a RedactedError, else the raw Error().
	persisted := err.Error()
	var red cluster.RedactedError
	if errors.As(err, &red) {
		persisted = red.RedactedMessage()
	}
	require.NotContains(t, persisted, secret,
		"the replicated dead-letter/status message must not carry the webhook URL secret")
	require.Contains(t, persisted, "127.0.0.1",
		"the message should still name the endpoint host to stay actionable")
}

func TestSync_SingleEntity_Success(t *testing.T) {
	var received webhookBody
	var headers nethttp.Header

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		headers = r.Header
		bs, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(bs, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	entity := newEntity(testType, "key1", map[string]string{"id": "1", "name": "test"})
	a := newActual(entity)
	a.Index = 42

	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Len(t, received.Entities, 1)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key1")), received.Entities[0].Key)
	require.Equal(t, "test-topic", received.Entities[0].Type.ID)
	require.Equal(t, "test", received.Entities[0].Type.Name)
	require.Equal(t, 1, received.Entities[0].Type.Version)

	require.Equal(t, "application/json", headers.Get("Content-Type"))
	// Idempotency key is the Actual's raft index (transaction-level), not a
	// per-entity key.
	require.Equal(t, "42", headers.Get("Idempotency-Key"))
}

// TestSync_MultipleEntities_OneAtomicRequest is the transaction-boundary
// guarantee: a multi-entity Actual is delivered as exactly ONE request whose
// body carries all the entities — not one request per entity. Shredding a
// committed transaction into independent calls (the old behaviour) broke
// atomicity downstream.
func TestSync_MultipleEntities_OneAtomicRequest(t *testing.T) {
	var count atomic.Int32
	var received webhookBody

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		count.Add(1)
		bs, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(bs, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	e1 := newEntity(testType, "key1", map[string]string{"id": "1"})
	e2 := newEntity(testType, "key2", map[string]string{"id": "2"})
	a := newActual(e1, e2)

	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Equal(t, int32(1), count.Load(), "the whole Actual must be one request")
	require.Len(t, received.Entities, 2, "both entities must ride in the single request")
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key1")), received.Entities[0].Key)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key2")), received.Entities[1].Key)
}

// TestSync_MixedTopic_PostsOnlyMatching is the mixed-topic regression: a
// proposal that mixes this syncable's topic with a foreign one must POST only
// the matching entity — never drop the whole Actual (silently losing this
// topic's data), nor forward the foreign entity.
func TestSync_MixedTopic_PostsOnlyMatching(t *testing.T) {
	var count atomic.Int32
	var received webhookBody

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		count.Add(1)
		bs, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(bs, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL)) // Topic "test-topic"
	defer s.Close()

	a := newActual(
		newEntity(testType, "mine", map[string]string{"id": "1"}),
		newEntity(otherType, "foreign", map[string]string{"id": "2"}),
	)
	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Equal(t, int32(1), count.Load(), "the matching entity is POSTed, not dropped")
	require.Len(t, received.Entities, 1, "only the matching entity rides the request")
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("mine")), received.Entities[0].Key)
}

// TestSync_AllForeignTopics_NoRequest verifies an Actual carrying only
// foreign-topic entities makes no POST and returns cleanly (checkpoint advances).
func TestSync_AllForeignTopics_NoRequest(t *testing.T) {
	var count atomic.Int32

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	a := newActual(newEntity(otherType, "foreign", map[string]string{"id": "1"}))
	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(false), snapshot)
	require.Equal(t, int32(0), count.Load(), "no request for an all-foreign Actual")
}

// TestSync_Delete_EmitsDeleteOp verifies a delete entity is delivered as an
// op:"delete" webhook carrying no data — the downstream half of
// right-to-be-forgotten. The receiver removes the record keyed by Key, and the
// sentinel payload must never be forwarded.
func TestSync_Delete_EmitsDeleteOp(t *testing.T) {
	var received webhookBody
	var rawBody []byte

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		_ = json.Unmarshal(rawBody, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	a := newActual(cluster.NewDeleteEntity(testType, []byte("key1")))
	a.Index = 7

	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Len(t, received.Entities, 1)
	require.Equal(t, "delete", received.Entities[0].Op)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key1")), received.Entities[0].Key)
	require.Equal(t, "test-topic", received.Entities[0].Type.ID)
	require.Empty(t, received.Entities[0].Data)
	// `data` must be omitted from the wire body entirely (omitempty).
	require.NotContains(t, string(rawBody), `"data"`)
}

// TestSync_Upsert_EmitsUpsertOp pins the op for the upsert path so receivers
// branch on it explicitly rather than inferring from a present/absent payload.
func TestSync_Upsert_EmitsUpsertOp(t *testing.T) {
	var received webhookBody

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		bs, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(bs, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	_, err := s.Sync(context.Background(), newActual(newEntity(testType, "key1", map[string]string{"id": "1"})))
	require.NoError(t, err)
	require.Len(t, received.Entities, 1)
	require.Equal(t, "upsert", received.Entities[0].Op)
	require.JSONEq(t, `{"id":"1"}`, string(received.Entities[0].Data))
}

// TestSync_Upsert_CarriesGeneration verifies an upsert forwards the source
// ingest's reconciling-refresh generation so a keyed receiver can stamp it on
// the row and later sweep by it. A zero generation (pre-feature / direct write)
// is omitted from the wire body.
func TestSync_Upsert_CarriesGeneration(t *testing.T) {
	var received webhookBody
	var rawBody []byte

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		_ = json.Unmarshal(rawBody, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	withGen := newEntity(testType, "key1", map[string]string{"id": "1"})
	withGen.Generation = 3
	zeroGen := newEntity(testType, "key2", map[string]string{"id": "2"})

	_, err := s.Sync(context.Background(), newActual(withGen, zeroGen))
	require.NoError(t, err)
	require.Len(t, received.Entities, 2)
	require.Equal(t, uint64(3), received.Entities[0].Generation, "the upsert must carry the source generation")
	require.Equal(t, uint64(0), received.Entities[1].Generation, "a zero generation decodes as 0")
	// The zero-generation entity omits the field entirely (omitempty), so a
	// receiver that predates the feature sees exactly today's wire body.
	require.NotContains(t, string(rawBody), `"generation":0`)
}

// TestSync_RefreshBoundary_EmitsRefreshOp is the Stage 4 red→green: a
// refresh-boundary marker (the close of a reconciling full refresh) is
// forwarded as op:"refresh" carrying the epoch the refresh reached and no
// key/data, so a keyed receiver can sweep every row still below that
// generation. Before Stage 4 the marker was silently dropped.
func TestSync_RefreshBoundary_EmitsRefreshOp(t *testing.T) {
	var received webhookBody
	var rawBody []byte

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		rawBody, _ = io.ReadAll(r.Body)
		_ = json.Unmarshal(rawBody, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	// A full-refresh Actual: the re-emitted row at epoch 5, then the marker.
	row := newEntity(testType, "key1", map[string]string{"id": "1"})
	row.Generation = 5
	marker := cluster.NewRefreshBoundaryEntity(testType, 5)

	snapshot, err := s.Sync(context.Background(), newActual(row, marker))
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Len(t, received.Entities, 2, "the marker rides the request alongside the row")
	require.Equal(t, "upsert", received.Entities[0].Op)
	require.Equal(t, uint64(5), received.Entities[0].Generation)

	refresh := received.Entities[1]
	require.Equal(t, "refresh", refresh.Op)
	require.Equal(t, uint64(5), refresh.Generation, "the refresh carries the epoch to sweep below")
	require.Equal(t, "test-topic", refresh.Type.ID)
	require.Empty(t, refresh.Key, "a refresh op carries no key")
	require.Empty(t, refresh.Data, "a refresh op carries no data")
}

// TestSync_RefreshBoundary_OnlyMarker verifies a marker-only Actual (a refresh
// whose window re-emitted no rows in this transaction) still POSTs the refresh
// op — the sweep must happen even when the boundary arrives alone.
func TestSync_RefreshBoundary_OnlyMarker(t *testing.T) {
	var count atomic.Int32
	var received webhookBody
	var rawBody []byte

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		count.Add(1)
		rawBody, _ = io.ReadAll(r.Body)
		_ = json.Unmarshal(rawBody, &received)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	_, err := s.Sync(context.Background(), newActual(cluster.NewRefreshBoundaryEntity(testType, 2)))
	require.NoError(t, err)
	require.Equal(t, int32(1), count.Load(), "a marker-only Actual must still POST the refresh")
	require.Len(t, received.Entities, 1)
	require.Equal(t, "refresh", received.Entities[0].Op)
	require.Equal(t, uint64(2), received.Entities[0].Generation)
	// A refresh carries no key, and the wire body omits the field entirely (not
	// `"key":""`), matching webhook-receiver.md's stated shape — a marker-only
	// body is the clean case to assert it, having no keyed entity.
	require.NotContains(t, string(rawBody), `"key"`, "the refresh body must omit the key field")
}

// TestSync_RefreshBoundary_ForeignTopicNotForwarded verifies a marker for a
// different topic is filtered out like any other foreign entity — the sweep is
// topic-scoped and must not reach a syncable watching another topic.
func TestSync_RefreshBoundary_ForeignTopicNotForwarded(t *testing.T) {
	var count atomic.Int32

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL)) // Topic "test-topic"
	defer s.Close()

	_, err := s.Sync(context.Background(), newActual(cluster.NewRefreshBoundaryEntity(otherType, 4)))
	require.NoError(t, err)
	require.Equal(t, int32(0), count.Load(), "a foreign-topic marker must not be POSTed")
}

func TestSync_TopicMismatch(t *testing.T) {
	var count atomic.Int32

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	entity := newEntity(otherType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	snapshot, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(false), snapshot)
	require.Equal(t, int32(0), count.Load())
}

func TestSync_PermanentError_4xx(t *testing.T) {
	codes := []int{400, 413, 422}

	for _, code := range codes {
		code := code
		t.Run("", func(t *testing.T) {
			ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				w.WriteHeader(code)
			}))
			defer ts.Close()

			s := synchttp.New(newConfig(ts.URL))
			defer s.Close()

			entity := newEntity(testType, "key1", map[string]string{"id": "1"})
			a := newActual(entity)

			_, err := s.Sync(context.Background(), a)
			require.Error(t, err)
			require.True(t, errors.Is(err, cluster.ErrPermanent), "status %d should be permanent", code)
		})
	}
}

func TestSync_TransientError_5xx(t *testing.T) {
	codes := []int{500, 502, 503}

	for _, code := range codes {
		code := code
		t.Run("", func(t *testing.T) {
			ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				w.WriteHeader(code)
			}))
			defer ts.Close()

			s := synchttp.New(newConfig(ts.URL))
			defer s.Close()

			entity := newEntity(testType, "key1", map[string]string{"id": "1"})
			a := newActual(entity)

			_, err := s.Sync(context.Background(), a)
			require.Error(t, err)
			require.False(t, errors.Is(err, cluster.ErrPermanent), "status %d should be transient", code)
		})
	}
}

// TestSync_UnsupportedMediaTypeIsTransient pins the round-8 fix: 415 must NOT
// dead-letter. committed sends a constant Content-Type, so a 415 rejects every
// entity identically — dead-lettering it would silently discard a whole topic's
// committed events. It must wedge (transient) so an operator fixes the receiver.
func TestSync_UnsupportedMediaTypeIsTransient(t *testing.T) {
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(415)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	_, err := s.Sync(context.Background(), newActual(newEntity(testType, "key1", map[string]string{"id": "1"})))
	require.Error(t, err)
	require.False(t, errors.Is(err, cluster.ErrPermanent),
		"415 must be transient (constant Content-Type → every-row error), never a silent dead-letter")
}

func TestSync_TransientError_429(t *testing.T) {
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(429)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	entity := newEntity(testType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	_, err := s.Sync(context.Background(), a)
	require.Error(t, err)
	require.False(t, errors.Is(err, cluster.ErrPermanent))
}

func TestSync_TransientError_408(t *testing.T) {
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(408)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	entity := newEntity(testType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	_, err := s.Sync(context.Background(), a)
	require.Error(t, err)
	require.False(t, errors.Is(err, cluster.ErrPermanent))
}

func TestSync_PUT_Method(t *testing.T) {
	var method string

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		method = r.Method
		w.WriteHeader(200)
	}))
	defer ts.Close()

	config := newConfig(ts.URL)
	config.Method = "PUT"
	s := synchttp.New(config)
	defer s.Close()

	entity := newEntity(testType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	_, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, "PUT", method)
}

func TestSync_CustomHeaders(t *testing.T) {
	var headers nethttp.Header

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		headers = r.Header
		w.WriteHeader(200)
	}))
	defer ts.Close()

	config := newConfig(ts.URL)
	config.Headers = []synchttp.Header{
		{Name: "Authorization", Value: "Bearer my-token"},
		{Name: "X-Custom", Value: "custom-value"},
	}
	s := synchttp.New(config)
	defer s.Close()

	entity := newEntity(testType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	_, err := s.Sync(context.Background(), a)
	require.NoError(t, err)
	require.Equal(t, "Bearer my-token", headers.Get("Authorization"))
	require.Equal(t, "custom-value", headers.Get("X-Custom"))
}

func TestSync_ContextCancellation(t *testing.T) {
	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	entity := newEntity(testType, "key1", map[string]string{"id": "1"})
	a := newActual(entity)

	_, err := s.Sync(ctx, a)
	require.Error(t, err)
}

func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		code      int
		wantErr   bool
		permanent bool
	}{
		{200, false, false},
		{201, false, false},
		{204, false, false},
		{299, false, false},
		// Payload-shaped: THIS entry can never be accepted — permanent.
		{400, true, true},
		{413, true, true},
		{422, true, true},
		{451, true, true},
		// 415 is NOT entry-specific: committed sends a constant Content-Type, so
		// a 415 fails every entity identically — transient (wedge), never a
		// silent whole-topic dead-letter.
		{415, true, false},
		// Access/config-shaped — would fail EVERY entry identically, so
		// transient: wedge visibly, zero dead-letters (a rotated token used
		// to dead-letter up to a breaker-run of real events).
		{401, true, false},
		{403, true, false},
		{404, true, false},
		{405, true, false},
		{407, true, false},
		{410, true, false},
		{301, true, false},
		{408, true, false},
		{429, true, false},
		{500, true, false},
		{502, true, false},
		{503, true, false},
		// Unknown codes default transient (conservative default).
		{418, true, false},
	}

	for _, tt := range tests {
		err := synchttp.ClassifyStatus(tt.code)
		if !tt.wantErr {
			require.NoError(t, err, "status %d", tt.code)
		} else {
			require.Error(t, err, "status %d", tt.code)
			if tt.permanent {
				require.True(t, errors.Is(err, cluster.ErrPermanent), "status %d should be permanent", tt.code)
			} else {
				require.False(t, errors.Is(err, cluster.ErrPermanent), "status %d should be transient", tt.code)
			}
		}
	}
}

// TestSync_AuthErrorsAreTransient pins the taxonomy fix: a rotated/expired
// receiver token (401/403) must WEDGE the worker — transient, stuck machinery
// engages, operator fixes the token, delivery resumes with zero data missing —
// never dead-letter committed events. An auth failure fails every entry
// identically, the exact criterion the Syncable contract's classification
// rule (and the SQL dialects' 42501 carve-out) names as access-shaped.
func TestSync_AuthErrorsAreTransient(t *testing.T) {
	for _, code := range []int{401, 403} {
		t.Run(fmt.Sprintf("%d", code), func(t *testing.T) {
			ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
				w.WriteHeader(code)
			}))
			defer ts.Close()

			s := synchttp.New(newConfig(ts.URL))
			defer s.Close()

			entity := newEntity(testType, "key1", map[string]string{"id": "1"})
			a := newActual(entity)

			_, err := s.Sync(context.Background(), a)
			require.Error(t, err)
			require.False(t, errors.Is(err, cluster.ErrPermanent),
				"status %d is access-shaped and must be transient (wedge, not dead-letter)", code)
		})
	}
}
