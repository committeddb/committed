package http

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/clusterfakes"
)

// The shared read-consistency barrier, tested as the unit it is: every
// linearized GET (config listings, versions, statuses) routes through
// h.linearize before touching replicated state, so the contract is pinned
// once here — with a directly-observable LinearizableRead — instead of
// re-sampled per route. Route-level smoke (default and stale both serve on
// a real engine) lives in consistency_test.go; the 503 leg cannot be
// induced on a healthy single node, which is exactly why this unit exists.

func barrierHTTP(fake *clusterfakes.FakeCluster) *HTTP {
	return &HTTP{c: fake, readIndexTimeout: 100 * time.Millisecond}
}

func TestLinearizeBarrier(t *testing.T) {
	get := func(query string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		return w
	}
	_ = get

	t.Run("default runs read-index and proceeds", func(t *testing.T) {
		fake := &clusterfakes.FakeCluster{}
		h := barrierHTTP(fake)
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "http://localhost/v1/anything", nil)
		require.True(t, h.linearize(w, r))
		require.Equal(t, 1, fake.LinearizableReadCallCount(),
			"a default GET must confirm a linearizable read before serving")
	})

	t.Run("explicit linearizable behaves like the default", func(t *testing.T) {
		fake := &clusterfakes.FakeCluster{}
		h := barrierHTTP(fake)
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "http://localhost/v1/anything?consistency=linearizable", nil)
		require.True(t, h.linearize(w, r))
		require.Equal(t, 1, fake.LinearizableReadCallCount())
	})

	t.Run("stale skips read-index entirely", func(t *testing.T) {
		fake := &clusterfakes.FakeCluster{}
		h := barrierHTTP(fake)
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "http://localhost/v1/anything?consistency=stale", nil)
		require.True(t, h.linearize(w, r))
		require.Equal(t, 0, fake.LinearizableReadCallCount(),
			"?consistency=stale must skip the quorum round-trip")
	})

	t.Run("read-index failure is 503, never a local read", func(t *testing.T) {
		fake := &clusterfakes.FakeCluster{}
		fake.LinearizableReadReturns(fmt.Errorf("no quorum confirmed"))
		h := barrierHTTP(fake)
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "http://localhost/v1/anything", nil)
		require.False(t, h.linearize(w, r),
			"a failed linearizable read must not fall through to a local read")
		require.Equal(t, 503, w.Code)
		require.Contains(t, w.Body.String(), "not_linearizable")
	})

	t.Run("unknown consistency value is 400 before any read", func(t *testing.T) {
		fake := &clusterfakes.FakeCluster{}
		h := barrierHTTP(fake)
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "http://localhost/v1/anything?consistency=eventually", nil)
		require.False(t, h.linearize(w, r))
		require.Equal(t, 400, w.Code)
		require.Contains(t, w.Body.String(), "invalid_consistency")
		require.Equal(t, 0, fake.LinearizableReadCallCount())
	})
}
