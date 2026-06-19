package http

import (
	"fmt"
	"os"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

// SyncableParser parses TOML configuration into an HTTP webhook Syncable.
type SyncableParser struct{}

func (p *SyncableParser) Parse(v *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}
	return New(config), nil
}

func (p *SyncableParser) ParseConfig(v *cluster.ParsedConfig) (*Config, error) {
	topic := v.GetString("http.topic")
	if topic == "" {
		return nil, fmt.Errorf("[http.syncable-parser] http.topic is required")
	}

	url := v.GetString("http.url")
	if url == "" {
		return nil, fmt.Errorf("[http.syncable-parser] http.url is required")
	}

	method := strings.ToUpper(v.GetString("http.method"))
	if method == "" {
		method = "POST"
	}
	if method != "POST" && method != "PUT" {
		return nil, fmt.Errorf("[http.syncable-parser] http.method must be POST or PUT, got %q", method)
	}

	timeoutMs := v.GetInt("http.timeout_ms")
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	var headers []Header
	if v.IsSet("http.headers") {
		// Decoded via mapstructure so the name/value field keys match
		// case-insensitively, while the values (real header names and
		// tokens — user data) stay byte-exact.
		var raw []struct {
			Name  string `mapstructure:"name"`
			Value string `mapstructure:"value"`
		}
		if err := v.UnmarshalKey("http.headers", &raw); err != nil {
			return nil, fmt.Errorf("[http.syncable-parser] http.headers must be an array of tables: %w", err)
		}
		for _, h := range raw {
			if h.Name == "" {
				return nil, fmt.Errorf("[http.syncable-parser] header name is required")
			}
			headers = append(headers, Header{
				Name:  h.Name,
				Value: os.ExpandEnv(h.Value),
			})
		}
	}

	policy, err := cluster.ParseCheckpointPolicy(v)
	if err != nil {
		return nil, fmt.Errorf("[http.syncable-parser] %w", err)
	}

	return &Config{
		Topic:      topic,
		URL:        url,
		Method:     method,
		TimeoutMs:  timeoutMs,
		Headers:    headers,
		Checkpoint: policy,
	}, nil
}
