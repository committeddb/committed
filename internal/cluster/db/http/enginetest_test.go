package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/clusterfakes"
	"github.com/committeddb/committed/internal/cluster/db"
	"github.com/committeddb/committed/internal/cluster/db/http"
	"github.com/committeddb/committed/internal/cluster/db/httptransport"
	"github.com/committeddb/committed/internal/cluster/db/parser"
	"github.com/committeddb/committed/internal/cluster/db/wal"
)

// The real-engine fixture for migrated handler groups. Handlers that hold
// *db.DB concretely have no interface seam to fake through, so their tests
// run against a real single-node engine: a wal.Storage on a temp dir
// (fsync off), raft electing itself in milliseconds, and real sync workers.
// The controllable piece is the PLUGIN seam — a "recorder" syncable parser
// whose sink records what it syncs and fails on command — which is how a
// test induces stuck workers, dead letters, and failed replays through the
// real pipeline instead of stubbing engine answers.

// recorderSink is the controllable test syncable: it records synced row keys
// and returns the configured error while one is set.
type recorderSink struct {
	mu   sync.Mutex
	keys []string
	err  error
}

func (r *recorderSink) Sync(_ context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	should := cluster.ShouldSnapshot(true)
	for _, e := range a.Entities {
		if e.Type == nil || cluster.IsInternal(e.Type.ID) {
			should = false
			continue
		}
		if e.Variant() == cluster.EntityVariantRow {
			if r.err != nil {
				return false, r.err
			}
			r.keys = append(r.keys, string(e.Key))
		}
	}
	return should, nil
}

func (r *recorderSink) Close() error { return nil }

func (r *recorderSink) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *recorderSink) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

// engine is one real single-node committed engine with its HTTP surface.
type engine struct {
	h    *http.HTTP
	d    *db.DB
	s    *wal.Storage
	sink *recorderSink
}

const engineTestTick = 1 * time.Millisecond

// newEngine boots the engine and serves it. The stuck threshold is tens of
// milliseconds so wedge-detection tests don't wait the production debounce.
func newEngine(t *testing.T) *engine {
	t.Helper()
	p := parser.New()
	sink := &recorderSink{}
	recParser := &clusterfakes.FakeSyncableParser{}
	recParser.ParseReturns(sink, nil)
	p.AddSyncableParser("recorder", recParser)
	// The projection spellings admit through the same recorder sink so the
	// http-layer deprecation warning is testable against a real admission.
	p.AddSyncableParser("projection", recParser)
	p.AddSyncableParser("sql-projection", recParser)

	syncCh := make(chan *db.SyncableWithID, 32)
	ingestCh := make(chan *db.IngestableWithID, 32)
	s, err := wal.Open(t.TempDir(), p, syncCh, ingestCh, wal.WithoutFsync())
	require.NoError(t, err)

	peers := db.Peers{1: ""} // no listener: single node, nothing to receive
	d := db.New(1, peers, s, p, syncCh, ingestCh,
		db.WithTickInterval(engineTestTick),
		db.WithTransportFactory(httptransport.Factory()),
		db.WithSyncStuckThreshold(50*time.Millisecond))
	t.Cleanup(func() { _ = d.Close(); _ = s.Close() })

	// Wait out the single-node election: leader-pinned routes (rebuild,
	// delete) consult Leader()/ID() and would 503 against a node that has
	// not elected itself yet. Proposals block on commit and hide this;
	// requests that read leadership first do not.
	require.Eventually(t, func() bool { return d.Leader() == 1 },
		10*time.Second, time.Millisecond, "the single node never elected itself")

	return &engine{h: http.New(d), d: d, s: s, sink: sink}
}

// do runs one request through the server and returns the recorder.
// contentType is empty for body-less requests.
func (e *engine) do(t *testing.T, method, path, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://localhost"+path, rd)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	return w
}

func (e *engine) doTOML(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return e.do(t, method, path, "text/toml", body)
}

func (e *engine) doJSON(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return e.do(t, method, path, "application/json", body)
}

func (e *engine) doEmpty(t *testing.T, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	return e.do(t, method, path, "", "")
}

// addType POSTs a minimal type and returns its id.
func (e *engine) addType(t *testing.T, id, name string) string {
	t.Helper()
	w := e.doTOML(t, "POST", "/v1/type/"+id, fmt.Sprintf("[type]\nname = %q", name))
	require.Equal(t, 200, w.Code, w.Body.String())
	return id
}

// addRecorderSyncable POSTs a "recorder" syncable consuming the given topic.
func (e *engine) addRecorderSyncable(t *testing.T, id, topic string) {
	t.Helper()
	body := fmt.Sprintf("[syncable]\nname = %q\ntype = \"recorder\"\n[recorder]\ntopic = %q\n", id, topic)
	w := e.doTOML(t, "POST", "/v1/syncable/"+id, body)
	require.Equal(t, 200, w.Code, w.Body.String())
}

// proposeRow POSTs one upsert for (typeID, key).
func (e *engine) proposeRow(t *testing.T, typeID, key string) {
	t.Helper()
	p := http.AddProposalRequest{Entities: []*http.AddEntityRequest{{
		TypeID: typeID, Key: key, Data: json.RawMessage(`{"one":"1"}`),
	}}}
	bs, err := json.Marshal(p)
	require.NoError(t, err)
	w := e.doJSON(t, "POST", "/v1/proposal", string(bs))
	require.Equal(t, 200, w.Code, w.Body.String())
}

// getJSON GETs path expecting 200 and decodes the body into out.
func (e *engine) getJSON(t *testing.T, path string, out any) {
	t.Helper()
	w := e.doEmpty(t, "GET", path)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), out))
}

// syncableStatus fetches GET /v1/syncable/{id}/status decoded loosely.
func (e *engine) syncableStatus(t *testing.T, id string) map[string]any {
	t.Helper()
	var got map[string]any
	e.getJSON(t, "/v1/syncable/"+id+"/status", &got)
	return got
}

// awaitStatus polls the status endpoint until cond accepts it.
func (e *engine) awaitStatus(t *testing.T, id string, cond func(map[string]any) bool, what string) map[string]any {
	t.Helper()
	var last map[string]any
	require.Eventually(t, func() bool {
		last = e.syncableStatus(t, id)
		return cond(last)
	}, 15*time.Second, 10*time.Millisecond, "status never showed: %s (last: %v)", what, last)
	return last
}

// requireEnvelope asserts a non-2xx response carries the error envelope with
// the given code.
func requireEnvelope(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	var body struct {
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, code, body.Code)
}

// mustStatus is requireEnvelope without the code check.
func mustStatus(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
}

// syncableIDs lists GET /v1/syncable and returns the ids.
func (e *engine) syncableIDs(t *testing.T) []string {
	t.Helper()
	var listing []struct {
		ID string `json:"id"`
	}
	e.getJSON(t, "/v1/syncable", &listing)
	ids := make([]string, 0, len(listing))
	for _, c := range listing {
		ids = append(ids, c.ID)
	}
	return ids
}
