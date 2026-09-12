package httptransport

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db"
)

// The catch-up exchange rides the peer transport: a node whose event log is
// behind the snapshot raft wants to install GETs the missing events from a
// peer here, with the same cluster-id, protocol, token, and mTLS posture as
// a raft message. One request is one bounded exchange — the serving node
// holds a layout freeze for its duration, streams whole sealed segment
// files and runs of records as multipart parts, and closes with an `end`
// part naming the last raft index served and whether more remain; the
// client asks again from its new event index until it has what it needs.
// A stream without its end part was cut short; the receiver keeps what it
// adopted and resumes from there.
const (
	eventsPath = "/raft/events"

	generationHeader = "X-Committed-Event-Log-Generation"
	eventIndexHeader = "X-Committed-Event-Index"

	partHeader      = "Committed-Part"
	partSegment     = "segment"
	partRecords     = "records"
	partEnd         = "end"
	segmentHeader   = "Committed-Segment"
	sizeHeader      = "Committed-Size"
	lastIndexHeader = "Committed-Last-Index"
	moreHeader      = "Committed-More"

	// fetchResponseTimeout bounds the wait for a serving peer's headers: the
	// peer takes a layout freeze first, which waits out any mover step in
	// flight (a segment's compression, a scrub swap's locked phase).
	fetchResponseTimeout = 30 * time.Second
	// maxRecordsPartBytes caps a records part on the receiving side — a
	// sanity bound well above what a serving node sends per part: its 8 MB
	// budget plus the one record that crosses it, and a record is one
	// proposal (db.DefaultMaxProposalBytes, 16 MB, unless raised).
	maxRecordsPartBytes = 256 << 20
)

// Stall bounds. Neither side may wait forever on the other: a receiver that
// stops reading would hold the serving node's layout freeze — and with it
// raft-log compaction — open indefinitely, and a serving peer that stops
// sending (a connection that died without a reset) would hang the
// receiver's catch-up. Both are bounds on PROGRESS, not on size: the
// serving side deadlines each write, the receiving side each read, so a
// slow link still completes a large segment while a dead one is cut. Vars,
// not consts, so tests can shorten them.
var (
	streamWriteStall = 60 * time.Second
	streamReadStall  = 60 * time.Second
)

// handleEvents serves a peer's catch-up fetch from this node's event log.
func (t *HttpTransport) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get(clusterIDHeader) != clusterID || r.Header.Get(protocolHeader) != protocolVersion {
		http.Error(w, "wrong cluster or protocol", http.StatusPreconditionFailed)
		return
	}
	if t.token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+t.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if t.events == nil {
		http.Error(w, "this node does not serve event-log fetches", http.StatusNotFound)
		return
	}
	after, err1 := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	to, err2 := strconv.ParseUint(r.URL.Query().Get("to"), 10, 64)
	var gen, minGen uint64
	var err3, err4 error
	if g := r.URL.Query().Get("generation"); g != "" {
		gen, err3 = strconv.ParseUint(g, 10, 64)
	}
	if g := r.URL.Query().Get("minGeneration"); g != "" {
		minGen, err4 = strconv.ParseUint(g, 10, 64)
	}
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || to <= after {
		http.Error(w, "bad range", http.StatusBadRequest)
		return
	}

	sink := &multipartSink{w: w, rc: http.NewResponseController(w), wantGen: gen, minGen: minGen}
	if _, err := t.events.ServeEvents(r.Context(), after, to, sink); err != nil {
		var mismatch *db.EventGenerationMismatch
		switch {
		case errors.As(err, &mismatch):
			// Written by the sink's Begin; nothing else was sent.
		case !sink.begun:
			msg, _ := cluster.RedactedMessage(err)
			http.Error(w, "serve events: "+msg, http.StatusInternalServerError)
		default:
			// Mid-stream: the status is out; the stream stops without its
			// end part, so the receiver knows it was short and keeps what
			// it has.
			t.logger.Warn("event-log fetch cut short", zap.Uint64("after", after), zap.Uint64("to", to), zap.Error(err))
		}
	}
}

// multipartSink is the serving side's db.EventSink over an HTTP response.
type multipartSink struct {
	w               http.ResponseWriter
	rc              *http.ResponseController
	wantGen, minGen uint64
	begun           bool
	mw              *multipart.Writer
}

func (m *multipartSink) Begin(gen, eventIndex uint64) error {
	m.w.Header().Set(generationHeader, strconv.FormatUint(gen, 10))
	m.w.Header().Set(eventIndexHeader, strconv.FormatUint(eventIndex, 10))
	if (m.wantGen != 0 && gen != m.wantGen) || gen < m.minGen {
		http.Error(m.w, "event-log generation mismatch", http.StatusConflict)
		return &db.EventGenerationMismatch{Have: gen, Want: max(m.wantGen, m.minGen)}
	}
	m.mw = multipart.NewWriter(m.w)
	m.w.Header().Set("Content-Type", "multipart/mixed; boundary="+m.mw.Boundary())
	m.w.WriteHeader(http.StatusOK)
	m.begun = true
	return nil
}

func (m *multipartSink) part(kind string, extra map[string]string) (io.Writer, error) {
	h := textproto.MIMEHeader{}
	h.Set(partHeader, kind)
	for k, v := range extra {
		h.Set(k, v)
	}
	if err := m.armWrite(); err != nil {
		return nil, err
	}
	pw, err := m.mw.CreatePart(h)
	if err != nil {
		return nil, err
	}
	return &stallBoundedWriter{w: pw, arm: m.armWrite}, nil
}

// armWrite gives the next write streamWriteStall to make progress.
func (m *multipartSink) armWrite() error {
	if err := m.rc.SetWriteDeadline(time.Now().Add(streamWriteStall)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// stallBoundedWriter re-arms the write deadline before every write, so the
// bound is on each write's progress rather than on a whole part.
type stallBoundedWriter struct {
	w   io.Writer
	arm func() error
}

func (s *stallBoundedWriter) Write(p []byte) (int, error) {
	if err := s.arm(); err != nil {
		return 0, err
	}
	return s.w.Write(p)
}

func (m *multipartSink) Segment(name string, size int64, r io.Reader) error {
	pw, err := m.part(partSegment, map[string]string{segmentHeader: name, sizeHeader: strconv.FormatInt(size, 10)})
	if err != nil {
		return err
	}
	n, err := io.Copy(pw, r)
	if err == nil && n != size {
		err = fmt.Errorf("segment %s: wrote %d of %d bytes", name, n, size)
	}
	return err
}

func (m *multipartSink) Records(data []byte) error {
	pw, err := m.part(partRecords, map[string]string{sizeHeader: strconv.Itoa(len(data))})
	if err != nil {
		return err
	}
	_, err = pw.Write(data)
	return err
}

func (m *multipartSink) End(res db.EventServeResult) error {
	if _, err := m.part(partEnd, map[string]string{
		lastIndexHeader: strconv.FormatUint(res.LastIndex, 10),
		moreHeader:      strconv.FormatBool(res.More),
	}); err != nil {
		return err
	}
	return m.mw.Close()
}

// FetchEvents asks one peer for the requested events and drives sink with
// the stream. Peers are tried in random order; one that cannot be reached,
// does not serve fetches (an older binary), or has nothing past After is
// skipped for the next. Implements db.EventFetcher.
func (t *HttpTransport) FetchEvents(ctx context.Context, req db.EventFetchRequest, sink db.EventSink) (db.EventFetchResult, error) {
	peers := t.peerList()
	if len(peers) == 0 {
		return db.EventFetchResult{}, db.ErrNoPeerToFetchFrom
	}
	rand.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	var last error
	for _, pr := range peers {
		res, err := t.fetchFrom(ctx, pr, req, sink)
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		var skip *peerSkipped
		if !errors.As(err, &skip) {
			return res, err
		}
		t.logger.Debug("event-log fetch: skipping peer", zap.Uint64("peer", pr.id), zap.Error(err))
		last = err
	}
	return db.EventFetchResult{}, fmt.Errorf("%w: %w", db.ErrNoPeerToFetchFrom, last)
}

// stallBoundedReader cancels the stream when no bytes arrive for the stall
// bound: each read that makes progress re-arms the timer.
type stallBoundedReader struct {
	r     io.Reader
	stall time.Duration
	timer *time.Timer
}

func newStallBoundedReader(r io.Reader, stall time.Duration, cancel func()) *stallBoundedReader {
	return &stallBoundedReader{r: r, stall: stall, timer: time.AfterFunc(stall, cancel)}
}

func (s *stallBoundedReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 {
		s.timer.Reset(s.stall)
	}
	return n, err
}

func (s *stallBoundedReader) stop() { s.timer.Stop() }

// cutDetector remembers whether a part's body ended before its boundary.
type cutDetector struct {
	r   io.Reader
	cut bool
}

func (c *cutDetector) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		c.cut = true
	}
	return n, err
}

// peerSkipped marks a peer that could not serve this request; the next is tried.
type peerSkipped struct{ err error }

func (e *peerSkipped) Error() string { return e.err.Error() }
func (e *peerSkipped) Unwrap() error { return e.err }

func (t *HttpTransport) peerList() []*peer {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*peer, 0, len(t.peers))
	for _, pr := range t.peers {
		out = append(out, pr)
	}
	return out
}

func (t *HttpTransport) fetchFrom(ctx context.Context, pr *peer, req db.EventFetchRequest, sink db.EventSink) (db.EventFetchResult, error) {
	res := db.EventFetchResult{Peer: pr.id}
	q := fmt.Sprintf("?after=%d&to=%d", req.After, req.To)
	if req.Generation != 0 {
		q += fmt.Sprintf("&generation=%d", req.Generation)
	}
	if req.MinGeneration != 0 {
		q += fmt.Sprintf("&minGeneration=%d", req.MinGeneration)
	}
	// The stream's own context: cut when the peer stops sending.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, pr.url+eventsPath+q, nil)
	if err != nil {
		return res, err
	}
	httpReq.Header.Set(clusterIDHeader, clusterID)
	httpReq.Header.Set(protocolHeader, protocolVersion)
	if t.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+t.token)
	}
	resp, err := t.fetchClient.Do(httpReq)
	if err != nil {
		return res, &peerSkipped{err}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
	}()

	gen, genErr := strconv.ParseUint(resp.Header.Get(generationHeader), 10, 64)
	eventIndex, idxErr := strconv.ParseUint(resp.Header.Get(eventIndexHeader), 10, 64)
	res.Generation, res.EventIndex = gen, eventIndex
	switch resp.StatusCode {
	case http.StatusOK:
		if genErr != nil || idxErr != nil {
			return res, &peerSkipped{fmt.Errorf("peer %d answered without its event-log generation and index", pr.id)}
		}
	case http.StatusConflict:
		return res, &db.EventGenerationMismatch{Peer: pr.id, Have: gen, Want: max(req.Generation, req.MinGeneration)}
	default:
		return res, &peerSkipped{fmt.Errorf("peer %d returned %s", pr.id, resp.Status)}
	}
	if eventIndex <= req.After {
		return res, &peerSkipped{fmt.Errorf("peer %d has nothing past index %d (its event index is %d)", pr.id, req.After, eventIndex)}
	}
	if err := sink.Begin(gen, eventIndex); err != nil {
		var mismatch *db.EventGenerationMismatch
		if errors.As(err, &mismatch) {
			mismatch.Peer = pr.id
		}
		return res, err
	}

	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/mixed" {
		return res, fmt.Errorf("peer %d: unexpected content type %q", pr.id, resp.Header.Get("Content-Type"))
	}
	body := newStallBoundedReader(resp.Body, streamReadStall, cancel)
	defer body.stop()
	mr := multipart.NewReader(body, params["boundary"])
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return res, fmt.Errorf("peer %d: stream ended without its end part", pr.id)
		}
		if err != nil {
			return res, fmt.Errorf("peer %d: %w", pr.id, err)
		}
		// A part whose body ends before its boundary was cut mid-part: not
		// content, however it reads to the sink.
		body := &cutDetector{r: part}
		switch kind := part.Header.Get(partHeader); kind {
		case partSegment:
			size, err := strconv.ParseInt(part.Header.Get(sizeHeader), 10, 64)
			if err != nil || size <= 0 {
				return res, fmt.Errorf("peer %d: segment part without a size", pr.id)
			}
			if err := sink.Segment(part.Header.Get(segmentHeader), size, body); err != nil {
				if body.cut {
					return res, fmt.Errorf("peer %d: stream ended inside a segment part", pr.id)
				}
				return res, err
			}
		case partRecords:
			size, err := strconv.Atoi(part.Header.Get(sizeHeader))
			if err != nil || size <= 0 || size > maxRecordsPartBytes {
				return res, fmt.Errorf("peer %d: records part with a bad size", pr.id)
			}
			data, err := io.ReadAll(io.LimitReader(body, int64(size)+1))
			if err != nil {
				return res, fmt.Errorf("peer %d: stream ended inside a records part: %w", pr.id, err)
			}
			if len(data) != size {
				return res, fmt.Errorf("peer %d: records part is %d bytes, header says %d", pr.id, len(data), size)
			}
			if err := sink.Records(data); err != nil {
				return res, err
			}
		case partEnd:
			res.LastIndex, _ = strconv.ParseUint(part.Header.Get(lastIndexHeader), 10, 64)
			res.More, _ = strconv.ParseBool(part.Header.Get(moreHeader))
			return res, sink.End(res.EventServeResult)
		default:
			return res, fmt.Errorf("peer %d: unknown part %q", pr.id, kind)
		}
	}
}
