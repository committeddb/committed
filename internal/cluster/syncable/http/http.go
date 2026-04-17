package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"time"

	"go.uber.org/zap"

	"github.com/philborlin/committed/internal/cluster"
)

type payload struct {
	Key       string          `json:"key"`
	Type      payloadType     `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp int64           `json:"timestamp"`
}

type payloadType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// Syncable delivers proposals to an HTTP webhook endpoint.
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

func (s *Syncable) Sync(ctx context.Context, p *cluster.Proposal) (cluster.ShouldSnapshot, error) {
	for _, e := range p.Entities {
		if s.config.Topic != e.ID {
			return false, nil
		}
	}

	for _, e := range p.Entities {
		if err := s.sendEntity(ctx, e); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *Syncable) sendEntity(ctx context.Context, e *cluster.Entity) error {
	encodedKey := base64.StdEncoding.EncodeToString(e.Key)

	body := payload{
		Key: encodedKey,
		Type: payloadType{
			ID:      e.ID,
			Name:    e.Name,
			Version: e.Version,
		},
		Data:      json.RawMessage(e.Data),
		Timestamp: e.Timestamp,
	}

	bs, err := json.Marshal(body)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[http.Sync] marshal payload: %w", err))
	}

	req, err := nethttp.NewRequestWithContext(ctx, s.config.Method, s.config.URL, bytes.NewReader(bs))
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[http.Sync] create request: %w", err))
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", encodedKey)
	for _, h := range s.config.Headers {
		req.Header.Set(h.Name, h.Value)
	}

	zap.L().Debug("http syncable sending", zap.String("url", s.config.URL), zap.String("key", encodedKey))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("[http.Sync] request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	return ClassifyStatus(resp.StatusCode)
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
