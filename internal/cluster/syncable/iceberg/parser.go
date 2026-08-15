package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// SyncableParser parses iceberg syncable TOML.
type SyncableParser struct{}

func (p *SyncableParser) Parse(v *cluster.ParsedConfig, _ cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}
	// Builds run on the listener (off the raft apply path), so the catalog
	// connect + table ensure may do network I/O here; a failure degrades the
	// config loudly instead of starting a worker. Bounded so an unreachable
	// catalog cannot pin the listener.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return New(ctx, config)
}

// ParseConfig validates the [iceberg] section without touching the network.
func (p *SyncableParser) ParseConfig(v *cluster.ParsedConfig) (*Config, error) {
	topic := v.GetString("iceberg.topic")
	if topic == "" {
		return nil, &cluster.FieldError{Field: "iceberg.topic", Issue: "required"}
	}
	catalogURI := v.GetString("iceberg.catalog")
	if catalogURI == "" {
		return nil, &cluster.FieldError{Field: "iceberg.catalog", Issue: "required (the Iceberg REST catalog endpoint)"}
	}
	u, err := url.Parse(catalogURI)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, &cluster.FieldError{
			Field: "iceberg.catalog",
			Issue: "must be an http(s) REST catalog URL",
		}
	}
	// Credentials never ride in config: the same posture as the SQL
	// connection-string guard, enforced structurally here — auth comes from
	// the node's environment (AWS credential chain / catalog token), so
	// nothing secret can land in the replicated config log.
	if u.User != nil {
		return nil, &cluster.FieldError{
			Field: "iceberg.catalog",
			Issue: "must not carry credentials; the catalog authenticates via the node's environment (AWS credential chain / token)",
		}
	}
	namespace := v.GetString("iceberg.namespace")
	if namespace == "" {
		return nil, &cluster.FieldError{Field: "iceberg.namespace", Issue: "required"}
	}
	tableName := v.GetString("iceberg.table")
	if tableName == "" {
		return nil, &cluster.FieldError{Field: "iceberg.table", Issue: "required"}
	}

	flushRows := defaultFlushRows
	if v.IsSet("iceberg.flushRows") {
		if n := v.GetInt("iceberg.flushRows"); n > 0 {
			flushRows = n
		} else {
			return nil, &cluster.FieldError{Field: "iceberg.flushRows", Issue: "must be a positive integer"}
		}
	}
	flushInterval := defaultFlushInterval
	if raw := v.GetString("iceberg.flushInterval"); raw != "" {
		d, derr := time.ParseDuration(raw)
		if derr != nil || d <= 0 {
			return nil, &cluster.FieldError{
				Field: "iceberg.flushInterval",
				Issue: fmt.Sprintf("must be a positive Go duration (e.g. \"60s\"): %q", raw),
			}
		}
		flushInterval = d
	}

	return &Config{
		Topic:         topic,
		CatalogURI:    catalogURI,
		Namespace:     namespace,
		Table:         tableName,
		Warehouse:     v.GetString("iceberg.warehouse"),
		FlushRows:     flushRows,
		FlushInterval: flushInterval,
		Props:         v.GetStringMapString("iceberg.props"),
	}, nil
}

// TopicsFromConfig implements cluster.SyncableTopicExtractor: the iceberg
// sink consumes the single topic at iceberg.topic. Read straight from the
// config (no catalog I/O), so dependency enumeration and admission guards
// run free.
func (p *SyncableParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	topic := v.GetString("iceberg.topic")
	if topic == "" {
		return nil
	}
	return []string{topic}
}
