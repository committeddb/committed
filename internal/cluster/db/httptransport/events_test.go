package httptransport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	httpgo "net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster/db"
)

// fakeEvents is an EventServer with a canned stream: it drives the sink the
// way wal.Storage.ServeEvents does, so the wire is exercised end to end.
type fakeEvents struct {
	gen, eventIndex uint64
	segments        map[string][]byte
	order           []string
	records         []byte
	lastIndex       uint64
	more            bool
	failMidStream   bool
}

func (f *fakeEvents) EventLogGeneration() uint64 { return f.gen }

func (f *fakeEvents) ServeEvents(_ context.Context, after, _ uint64, sink db.EventSink) (db.EventServeResult, error) {
	res := db.EventServeResult{Generation: f.gen, EventIndex: f.eventIndex}
	if err := sink.Begin(f.gen, f.eventIndex); err != nil {
		return res, err
	}
	if after >= f.eventIndex {
		return res, nil
	}
	for _, name := range f.order {
		data := f.segments[name]
		if err := sink.Segment(name, int64(len(data)), bytes.NewReader(data)); err != nil {
			return res, err
		}
	}
	if f.failMidStream {
		return res, errors.New("disk read failed")
	}
	if len(f.records) > 0 {
		if err := sink.Records(f.records); err != nil {
			return res, err
		}
	}
	res.LastIndex, res.More = f.lastIndex, f.more
	return res, sink.End(res)
}

// recordingSink is the receiving side: it keeps what arrived.
type recordingSink struct {
	begun    []uint64
	segments map[string][]byte
	order    []string
	records  [][]byte
	ended    []db.EventServeResult
	beginErr error
}

func (r *recordingSink) End(res db.EventServeResult) error {
	r.ended = append(r.ended, res)
	return nil
}

func (r *recordingSink) Begin(gen, _ uint64) error {
	r.begun = append(r.begun, gen)
	return r.beginErr
}

func (r *recordingSink) Segment(name string, size int64, rd io.Reader) error {
	// Exactly size bytes, as the real sink stages them.
	data, err := io.ReadAll(io.LimitReader(rd, size))
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("segment %s: got %d bytes, size says %d", name, len(data), size)
	}
	if r.segments == nil {
		r.segments = map[string][]byte{}
	}
	r.segments[name] = data
	r.order = append(r.order, name)
	return nil
}

func (r *recordingSink) Records(data []byte) error {
	r.records = append(r.records, append([]byte(nil), data...))
	return nil
}

// servePeer binds a transport's handler on a loopback listener and returns
// a client transport that knows it as peer 1.
func servePeer(t *testing.T, events db.EventServer, token string) (*HttpTransport, *fakeEvents) {
	t.Helper()
	fe, _ := events.(*fakeEvents)
	serving := New(1, nil, zap.NewNop(), newRecordingRaft(), events, nil, token)
	srv := httptest.NewServer(serving.handler())
	t.Cleanup(srv.Close)
	client := New(2, nil, zap.NewNop(), newRecordingRaft(), nil, nil, token)
	require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(srv.URL)}))
	t.Cleanup(client.Stop)
	return client, fe
}

// A fetch carries the serving node's stream across the wire whole: the
// generation and event index first, then each segment file byte for byte
// and each records run, then the end part's last index and more flag.
func TestFetchEvents_RoundTrip(t *testing.T) {
	seg1 := bytes.Repeat([]byte("segment-one-"), 1000)
	seg2 := []byte("a compressed segment's bytes")
	events := &fakeEvents{
		gen: 7, eventIndex: 500,
		segments:  map[string][]byte{"00000000000000000001": seg1, "00000000000000000090.zst": seg2},
		order:     []string{"00000000000000000001", "00000000000000000090.zst"},
		records:   []byte("records-run"),
		lastIndex: 480, more: true,
	}
	client, _ := servePeer(t, events, "s3cr3t")

	sink := &recordingSink{}
	res, err := client.FetchEvents(context.Background(), db.EventFetchRequest{After: 100, To: 500, Generation: 7}, sink)
	require.NoError(t, err)
	require.Equal(t, uint64(1), res.Peer)
	require.Equal(t, uint64(7), res.Generation)
	require.Equal(t, uint64(500), res.EventIndex)
	require.Equal(t, uint64(480), res.LastIndex)
	require.True(t, res.More)

	require.Equal(t, []uint64{7}, sink.begun)
	require.Equal(t, events.order, sink.order)
	require.Equal(t, seg1, sink.segments["00000000000000000001"])
	require.Equal(t, seg2, sink.segments["00000000000000000090.zst"])
	require.Equal(t, [][]byte{[]byte("records-run")}, sink.records)
	require.Equal(t, []db.EventServeResult{{Generation: 7, EventIndex: 500, LastIndex: 480, More: true}}, sink.ended,
		"the end part reaches the sink as the stream's result")
}

// A peer at another generation than the request pinned, or below the
// minimum it requires, answers a mismatch naming its generation and sends
// nothing; a peer with nothing past the requested index, one that does not
// serve fetches (an older binary), or one that refuses the token is
// skipped — and with no other peer that is ErrNoPeerToFetchFrom.
func TestFetchEvents_MismatchAndSkips(t *testing.T) {
	events := &fakeEvents{gen: 7, eventIndex: 500, records: []byte("x"), lastIndex: 500}
	client, _ := servePeer(t, events, "")

	sink := &recordingSink{}
	var mismatch *db.EventGenerationMismatch
	_, err := client.FetchEvents(context.Background(), db.EventFetchRequest{After: 100, To: 500, Generation: 5}, sink)
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, &db.EventGenerationMismatch{Peer: 1, Have: 7, Want: 5}, mismatch)
	require.Empty(t, sink.begun, "a refused stream never begins on the receiving side")

	_, err = client.FetchEvents(context.Background(), db.EventFetchRequest{After: 100, To: 500, MinGeneration: 9}, sink)
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, &db.EventGenerationMismatch{Peer: 1, Have: 7, Want: 9}, mismatch)

	_, err = client.FetchEvents(context.Background(), db.EventFetchRequest{After: 500, To: 600}, sink)
	require.ErrorIs(t, err, db.ErrNoPeerToFetchFrom, "nothing past the peer's event index")

	unserved, _ := servePeer(t, nil, "")
	_, err = unserved.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 10}, sink)
	require.ErrorIs(t, err, db.ErrNoPeerToFetchFrom, "a peer without an event log serves nothing")

	wrongToken := New(3, nil, zap.NewNop(), newRecordingRaft(), nil, nil, "other")
	srv := httptest.NewServer(New(4, nil, zap.NewNop(), newRecordingRaft(), events, nil, "right").handler())
	defer srv.Close()
	require.NoError(t, wrongToken.AddPeer(raft.Peer{ID: 4, Context: []byte(srv.URL)}))
	_, err = wrongToken.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 10}, sink)
	require.ErrorIs(t, err, db.ErrNoPeerToFetchFrom)

	alone := New(5, nil, zap.NewNop(), newRecordingRaft(), nil, nil, "")
	_, err = alone.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 10}, sink)
	require.ErrorIs(t, err, db.ErrNoPeerToFetchFrom, "no peers at all")
}

// A stream the serving side cannot finish is cut without its end part, and
// the receiving side reports it short — keeping what it already adopted, to
// resume from there — rather than mistaking it for complete. A sink that
// refuses the stream at Begin stops it before any content flows.
func TestFetchEvents_CutShortAndRefusedAtBegin(t *testing.T) {
	events := &fakeEvents{
		gen: 1, eventIndex: 50,
		segments: map[string][]byte{"00000000000000000001": []byte("first")},
		order:    []string{"00000000000000000001"},
		records:  []byte("never sent"), failMidStream: true,
	}
	client, _ := servePeer(t, events, "")

	sink := &recordingSink{}
	_, err := client.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 50}, sink)
	require.ErrorContains(t, err, "without its end part")
	require.Equal(t, []string{"00000000000000000001"}, sink.order, "what arrived before the cut is kept")
	require.Empty(t, sink.records)
	require.Empty(t, sink.ended, "a cut stream never ends")

	refusing := &recordingSink{beginErr: &db.EventGenerationMismatch{Have: 1, Want: 3}}
	var mismatch *db.EventGenerationMismatch
	_, err = client.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 50}, refusing)
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, uint64(1), mismatch.Peer, "the peer is named on a receiver-side refusal too")
	require.Empty(t, refusing.order)
}

// A serving peer that stops sending mid-stream — a connection that died
// without a reset — does not hang the receiver: the stream is cut once no
// bytes arrive for the stall bound, and the fetch returns so the loop can
// try again.
func TestFetchEvents_CutsAStalledPeer(t *testing.T) {
	old := streamReadStall
	streamReadStall = 200 * time.Millisecond
	defer func() { streamReadStall = old }()

	release := make(chan struct{})
	stalled := httpgo.HandlerFunc(func(w httpgo.ResponseWriter, _ *httpgo.Request) {
		w.Header().Set(generationHeader, "1")
		w.Header().Set(eventIndexHeader, "50")
		w.Header().Set("Content-Type", "multipart/mixed; boundary=b")
		w.WriteHeader(httpgo.StatusOK)
		_, _ = io.WriteString(w, "--b\r\n"+partHeader+": "+partSegment+"\r\n"+segmentHeader+": 00000000000000000001\r\n"+sizeHeader+": 1000\r\n\r\nhalf")
		httpgo.NewResponseController(w).Flush()
		<-release // never sends the rest
	})
	srv := httptest.NewServer(stalled)
	// The handler is released before the server closes (Close waits for it).
	defer srv.Close()
	defer close(release)
	client := New(2, nil, zap.NewNop(), newRecordingRaft(), nil, nil, "")
	require.NoError(t, client.AddPeer(raft.Peer{ID: 1, Context: []byte(srv.URL)}))
	defer client.Stop()

	start := time.Now()
	_, err := client.FetchEvents(context.Background(), db.EventFetchRequest{After: 0, To: 50}, &recordingSink{})
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second, "the stall bound, not a TCP timeout, cut the stream")
}

// The events route keeps the message route's guards: cluster id, protocol
// version, and method.
func TestEvents_Guards(t *testing.T) {
	tr := New(1, nil, zap.NewNop(), newRecordingRaft(), &fakeEvents{gen: 1, eventIndex: 5}, nil, "")
	srv := httptest.NewServer(tr.handler())
	defer srv.Close()

	get := func(headers map[string]string, query string) int {
		req, err := httpgo.NewRequest(httpgo.MethodGet, srv.URL+eventsPath+query, nil)
		require.NoError(t, err)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := httpgo.DefaultClient.Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	good := map[string]string{clusterIDHeader: clusterID, protocolHeader: protocolVersion}
	require.Equal(t, httpgo.StatusOK, get(good, "?after=0&to=5"))
	require.Equal(t, httpgo.StatusPreconditionFailed, get(map[string]string{clusterIDHeader: "someone-else", protocolHeader: protocolVersion}, "?after=0&to=5"))
	require.Equal(t, httpgo.StatusBadRequest, get(good, "?after=5&to=5"))
	require.Equal(t, httpgo.StatusBadRequest, get(good, "?after=x&to=5"))

	req, err := httpgo.NewRequest(httpgo.MethodPost, srv.URL+eventsPath+"?after=0&to=5", nil)
	require.NoError(t, err)
	for k, v := range good {
		req.Header.Set(k, v)
	}
	resp, err := httpgo.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, httpgo.StatusMethodNotAllowed, resp.StatusCode)
}
