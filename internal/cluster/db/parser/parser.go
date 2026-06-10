package parser

import (
	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/config"
)

type Parser struct {
	databaseParsers   map[string]cluster.DatabaseParser
	ingestableParsers map[string]cluster.IngestableParser
	syncableParsers   map[string]cluster.SyncableParser
}

func New() *Parser {
	return &Parser{
		databaseParsers:   make(map[string]cluster.DatabaseParser),
		ingestableParsers: make(map[string]cluster.IngestableParser),
		syncableParsers:   make(map[string]cluster.SyncableParser),
	}
}

// Validate parses data as the given mime type and verifies every ${VAR}
// secret reference resolves against this node's environment, WITHOUT
// invoking the type-specific sub-parser. It opens no database
// connections and starts no workers, so it is the side-effect-free check
// the storage layer runs at startup to fail fast when a persisted config
// references an env var this node is missing. A missing reference
// surfaces as *config.MissingVarError (wrapped by parseBytes).
func (p *Parser) Validate(mimeType string, data []byte) error {
	_, err := parseBytes(mimeType, data)
	return err
}

func parseBytes(mimeType string, data []byte) (*cluster.ParsedConfig, error) {
	v, err := cluster.ParseConfigBytes(mimeType, data)
	if err != nil {
		return nil, err
	}

	// Expand ${VAR} secret references against this node's environment.
	// This happens at the parse boundary, after the TOML/JSON is parsed
	// but before the sub-parser sees any value, so the raw template
	// bytes are what get proposed into Raft and stored in bbolt — only
	// the live in-memory config carries the resolved secret. The
	// expansion mutates the decoded tree in place, so every downstream
	// Get* reads the expanded value.
	if err := config.Interpolate(v.Values()); err != nil {
		return nil, err
	}

	return v, nil
}
