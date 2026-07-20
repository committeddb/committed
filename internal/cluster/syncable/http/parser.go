package http

import (
	"fmt"
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

// TopicsFromConfig implements cluster.SyncableTopicExtractor: a webhook syncable
// consumes the single topic named at http.topic. Read straight from the config
// (no Init), so config-change enumeration runs no I/O.
func (p *SyncableParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	topic := v.GetString("http.topic")
	if topic == "" {
		return nil
	}
	return []string{topic}
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

	timeoutMs := v.GetInt("http.timeoutMs")
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
				Name: h.Name,
				// Do NOT expand here. Secret ${VAR} interpolation runs once at the
				// db/parser boundary (config.Interpolate, fail-closed on an unset
				// var), exactly as the SQL parsers rely on. A second stdlib
				// os.ExpandEnv pass would re-expand: it fail-OPENs a bare $VAR to
				// empty (silently dropping the credential) and mangles a resolved
				// secret that legitimately contains a literal '$'.
				Value: h.Value,
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
