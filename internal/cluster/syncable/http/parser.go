package http

import (
	"fmt"
	"os"
	"strings"

	"github.com/philborlin/committed/internal/cluster"
	"github.com/spf13/viper"
)

// SyncableParser parses TOML configuration into an HTTP webhook Syncable.
type SyncableParser struct{}

func (p *SyncableParser) Parse(v *viper.Viper, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}
	return New(config), nil
}

func (p *SyncableParser) ParseConfig(v *viper.Viper) (*Config, error) {
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
	if raw := v.Get("http.headers"); raw != nil {
		items, ok := raw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("[http.syncable-parser] http.headers must be an array of tables")
		}
		for _, item := range items {
			m, ok := item.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("[http.syncable-parser] invalid header entry")
			}
			name, _ := m["name"].(string)
			value, _ := m["value"].(string)
			if name == "" {
				return nil, fmt.Errorf("[http.syncable-parser] header name is required")
			}
			headers = append(headers, Header{
				Name:  name,
				Value: os.ExpandEnv(value),
			})
		}
	}

	return &Config{
		Topic:     topic,
		URL:       url,
		Method:    method,
		TimeoutMs: timeoutMs,
		Headers:   headers,
	}, nil
}
