package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

// payload is the body of one webhook delivery: a whole committed Actual,
// its entities sent together so the receiver sees the transaction boundary
// intact. An Actual is one atomic unit, so it is one request — never one
// request per entity.
type payload struct {
	Entities []entityPayload `json:"entities"`
}

type entityPayload struct {
	// Op is "upsert" or "delete". A delete carries no Data and the receiver
	// MUST remove the record keyed by Key (a delete for a record that does
	// not exist MUST be a harmless no-op) — this is the downstream half of
	// right-to-be-forgotten erasure. Op is always present so receivers
	// branch on it explicitly rather than inferring intent from missing Data.
	Op   string          `json:"op"`
	Key  string          `json:"key"`
	Type payloadType     `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

const (
	opUpsert = "upsert"
	opDelete = "delete"
)

type payloadType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// Syncable delivers committed Actuals to an HTTP webhook endpoint.
type Syncable struct {
	client *nethttp.Client
	config *Config
}

// New creates an HTTP webhook Syncable.
func New(config *Config) *Syncable {
	return &Syncable{
		client: &nethttp.Client{
			Timeout: time.Duration(config.TimeoutMs) * time.Millisecond,
		},
		config: config,
	}
}

// Sync delivers one Actual as a single POST whose body carries all of the
// Actual's entities. The Actual is the unit of atomicity, so one Actual is
// one request — the receiver gets the whole committed transaction together,
// never shredded into per-entity calls. The Idempotency-Key is the Actual's
// raft Index, so a redelivery (after a retry, leader change, or restart) is
// recognizable as the same transaction.
func (s *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	for _, e := range a.Entities {
		if s.config.Topic != e.ID {
			return false, nil
		}
	}

	entities := make([]entityPayload, 0, len(a.Entities))
	for _, e := range a.Entities {
		// A delete carries the sentinel in Data, not a payload — emit op
		// "delete" with no Data so the receiver removes the keyed record;
		// otherwise emit op "upsert" with the entity's data.
		op := opUpsert
		var data json.RawMessage
		if e.IsDelete() {
			op = opDelete
		} else {
			data = json.RawMessage(e.Data)
		}
		entities = append(entities, entityPayload{
			Op:  op,
			Key: base64.StdEncoding.EncodeToString(e.Key),
			Type: payloadType{
				ID:      e.ID,
				Name:    e.Name,
				Version: e.Version,
			},
			Data: data,
		})
	}

	bs, err := json.Marshal(payload{Entities: entities})
	if err != nil {
		return false, cluster.Permanent(fmt.Errorf("[http.Sync] marshal payload: %w", err))
	}

	req, err := nethttp.NewRequestWithContext(ctx, s.config.Method, s.config.URL, bytes.NewReader(bs))
	if err != nil {
		return false, cluster.Permanent(fmt.Errorf("[http.Sync] create request: %w", err))
	}

	idempotencyKey := strconv.FormatUint(a.Index, 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	for _, h := range s.config.Headers {
		req.Header.Set(h.Name, h.Value)
	}

	zap.L().Debug("http syncable sending",
		zap.String("url", s.config.URL),
		zap.Uint64("index", a.Index),
		zap.Int("entities", len(entities)))

	resp, err := s.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("[http.Sync] request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	if err := ClassifyStatus(resp.StatusCode); err != nil {
		return false, err
	}
	return true, nil
}

// ClassifyStatus maps HTTP status codes to error semantics:
//   - 2xx: success (nil)
//   - 408, 429: transient (retryable)
//   - other 4xx: permanent (skip proposal)
//   - 5xx: transient (retryable)
func ClassifyStatus(code int) error {
	if code >= 200 && code < 300 {
		return nil
	}
	if code == 408 || code == 429 || code >= 500 {
		return fmt.Errorf("[http.Sync] unexpected status %d", code)
	}
	return cluster.Permanent(fmt.Errorf("[http.Sync] unexpected status %d", code))
}

func (s *Syncable) Close() error {
	s.client.CloseIdleConnections()
	return nil
}
