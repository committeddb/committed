package http_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	Op   string          `json:"op"`
	Key  string          `json:"key"`
	Type webhookType     `json:"type"`
	Data json.RawMessage `json:"data"`
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
	codes := []int{400, 401, 403, 404, 405, 422}

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
		{400, true, true},
		{401, true, true},
		{403, true, true},
		{404, true, true},
		{405, true, true},
		{408, true, false},
		{422, true, true},
		{429, true, false},
		{500, true, false},
		{502, true, false},
		{503, true, false},
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
