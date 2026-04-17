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

	"github.com/philborlin/committed/internal/cluster"
	synchttp "github.com/philborlin/committed/internal/cluster/syncable/http"
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

func newProposal(entities ...*cluster.Entity) *cluster.Proposal {
	return &cluster.Proposal{Entities: entities}
}

func newEntity(t *cluster.Type, key string, data any) *cluster.Entity {
	bs, _ := json.Marshal(data)
	return &cluster.Entity{
		Type:      t,
		Key:       []byte(key),
		Data:      bs,
		Timestamp: 1000,
	}
}

type webhookBody struct {
	Key       string          `json:"key"`
	Type      webhookType     `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

type webhookType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
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
	p := newProposal(entity)

	snapshot, err := s.Sync(context.Background(), p)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)

	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key1")), received.Key)
	require.Equal(t, "test-topic", received.Type.ID)
	require.Equal(t, "test", received.Type.Name)
	require.Equal(t, 1, received.Type.Version)
	require.Equal(t, int64(1000), received.Timestamp)

	require.Equal(t, "application/json", headers.Get("Content-Type"))
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("key1")), headers.Get("Idempotency-Key"))
}

func TestSync_MultipleEntities_Success(t *testing.T) {
	var count atomic.Int32

	ts := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer ts.Close()

	s := synchttp.New(newConfig(ts.URL))
	defer s.Close()

	e1 := newEntity(testType, "key1", map[string]string{"id": "1"})
	e2 := newEntity(testType, "key2", map[string]string{"id": "2"})
	p := newProposal(e1, e2)

	snapshot, err := s.Sync(context.Background(), p)
	require.NoError(t, err)
	require.Equal(t, cluster.ShouldSnapshot(true), snapshot)
	require.Equal(t, int32(2), count.Load())
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
	p := newProposal(entity)

	snapshot, err := s.Sync(context.Background(), p)
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
			p := newProposal(entity)

			_, err := s.Sync(context.Background(), p)
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
			p := newProposal(entity)

			_, err := s.Sync(context.Background(), p)
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
	p := newProposal(entity)

	_, err := s.Sync(context.Background(), p)
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
	p := newProposal(entity)

	_, err := s.Sync(context.Background(), p)
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
	p := newProposal(entity)

	_, err := s.Sync(context.Background(), p)
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
	p := newProposal(entity)

	_, err := s.Sync(context.Background(), p)
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
	p := newProposal(entity)

	_, err := s.Sync(ctx, p)
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
