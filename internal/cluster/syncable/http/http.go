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

	"github.com/committeddb/committed/internal/cluster"
)

// payload is the body of one webhook delivery: a whole committed Actual,
// its entities sent together so the receiver sees the transaction boundary
// intact. An Actual is one atomic unit, so it is one request — never one
// request per entity.
type payload struct {
	Entities []entityPayload `json:"entities"`
}

type entityPayload struct {
	// Op is "upsert", "delete", or "refresh". A delete carries no Data and the
	// receiver MUST remove the record keyed by Key (a delete for a record that
	// does not exist MUST be a harmless no-op) — the downstream half of
	// right-to-be-forgotten erasure. A refresh carries no Key/Data, only
	// Generation: it closes a reconciling full refresh and a keyed receiver
	// MUST sweep every row still below that generation (see Generation). Op is
	// always present so receivers branch on it explicitly rather than inferring
	// intent from missing Data.
	Op   string          `json:"op"`
	Key  string          `json:"key,omitempty"`
	Type payloadType     `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
	// Generation is committed's reconciling-refresh epoch: the source ingest
	// stamps it on every entity, monotonically increasing, and a full refresh
	// re-emits every live row at a new epoch. A keyed receiver SHOULD store it
	// on each upserted row, so that on the closing op:"refresh" (which carries
	// the epoch G the refresh reached) it can `DELETE WHERE generation < G` —
	// removing rows deleted at the source during a lost change-data window that
	// the upsert-only refresh could not re-emit. Omitted (0) for entities from
	// a source that predates the feature or for direct writes; a receiver that
	// ignores it keeps today's upsert/delete-only behavior.
	Generation uint64 `json:"generation,omitempty"`
}

const (
	opUpsert  = "upsert"
	opDelete  = "delete"
	opRefresh = "refresh"
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
	// endpoint is the URL's scheme://host only (no path/query/userinfo) — the
	// replicate-safe form used in delivery errors and logs, since the path/query
	// is where secret-in-URL webhooks (Slack/Discord/?token=) carry the secret.
	endpoint string
}

// New creates an HTTP webhook Syncable.
func New(config *Config) *Syncable {
	return &Syncable{
		client: &nethttp.Client{
			Timeout: time.Duration(config.TimeoutMs) * time.Millisecond,
		},
		config:   config,
		endpoint: redactedTarget(config.URL),
	}
}

// CheckpointPolicy implements cluster.CheckpointConfigurable so the sync
// worker honors the cadence parsed from the [syncable] TOML. A webhook is
// non-idempotent, so the default Every=1 is the right choice unless the
// operator explicitly accepts duplicate POSTs on crash.
func (s *Syncable) CheckpointPolicy() cluster.CheckpointPolicy {
	return s.config.Checkpoint
}

// Sync delivers one Actual as a single POST whose body carries all of the
// Actual's entities. The Actual is the unit of atomicity, so one Actual is
// one request — the receiver gets the whole committed transaction together,
// never shredded into per-entity calls. The Idempotency-Key is the Actual's
// raft Index, so a redelivery (after a retry, leader change, or restart) is
// recognizable as the same transaction.
func (s *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// A proposal can carry entities from several topics; POST only ours. Filtering
	// per-entity — rather than dropping the whole Actual on the first foreign
	// entity — is what keeps a mixed-topic Actual from silently losing this
	// topic's data.
	entities := make([]entityPayload, 0, len(a.Entities))
	for _, e := range a.Entities {
		if s.config.Topic != e.ID {
			continue // an entity from another topic in a mixed proposal — not ours
		}
		// A refresh-boundary marker closes a reconciling full refresh: forward it
		// as op "refresh" carrying only the generation the refresh reached, so a
		// keyed receiver can sweep every row still below it (rows deleted at the
		// source in a lost change-data window, never re-emitted). It has no
		// key/data. A receiver that ignores "refresh" keeps today's behavior.
		if e.IsRefreshBoundary() {
			entities = append(entities, entityPayload{
				Op: opRefresh,
				Type: payloadType{
					ID:      e.ID,
					Name:    e.Name,
					Version: e.Version,
				},
				Generation: e.Generation,
			})
			continue
		}
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
			Data:       data,
			Generation: e.Generation,
		})
	}

	// No entity was for our topic — nothing to POST; the checkpoint still advances.
	if len(entities) == 0 {
		return false, nil
	}

	bs, err := json.Marshal(payload{Entities: entities})
	if err != nil {
		return false, cluster.Permanent(fmt.Errorf("[http.Sync] marshal payload: %w", err))
	}

	req, err := nethttp.NewRequestWithContext(ctx, s.config.Method, s.config.URL, bytes.NewReader(bs))
	if err != nil {
		// The error can be a *url.Error echoing the full (secret-bearing) URL —
		// wrap it so only the redacted endpoint reaches the replicated dead-letter.
		return false, cluster.Permanent(&syncError{label: "[http.Sync] create request", target: s.endpoint, err: err})
	}

	idempotencyKey := strconv.FormatUint(a.Index, 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	for _, h := range s.config.Headers {
		req.Header.Set(h.Name, h.Value)
	}

	zap.L().Debug("http syncable sending",
		zap.String("endpoint", s.endpoint),
		zap.Uint64("index", a.Index),
		zap.Int("entities", len(entities)))

	resp, err := s.client.Do(req)
	if err != nil {
		// A transport failure returns a *url.Error whose text embeds the full
		// (secret-bearing) request URL; wrap it so the replicated dead-letter /
		// stuck record / status API sees only the redacted endpoint. Transient
		// (not Permanent) — a blip should retry.
		return false, &syncError{label: "[http.Sync] request failed", target: s.endpoint, err: err}
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
//
// The 4xx/5xx split is the deliberate boundary, and it upholds the
// asymmetric-risk principle (bias toward transient): a 4xx says the receiver
// rejected *this request* as malformed/unacceptable (400, 422, 404, 405) —
// the proposal will never be accepted as-is, so it's permanent and gets
// dead-lettered; 408 (timeout) and 429 (rate limited) are the two 4xx that are
// about timing, not the payload, so they stay transient. A 5xx is a
// server-side failure — including 501 Not Implemented and 503 Unavailable —
// and is always transient: retrying is safe, and if the endpoint is
// permanently wrong (e.g. never implements the method) the worker wedges
// visibly for an operator instead of silently dead-lettering every proposal.
// A transport error (no status at all) is handled by the caller and is also
// transient.
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
