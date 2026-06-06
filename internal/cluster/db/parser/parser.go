package parser

import (
	"bytes"
	"io"

	"github.com/spf13/viper"

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
	_, err := parseBytes(mimeType, bytes.NewReader(data))
	return err
}

func parseBytes(mimeType string, reader io.Reader) (*viper.Viper, error) {
	style := "toml"
	if mimeType == "application/json" {
		style = "json"
	}

	v := viper.New()

	v.SetConfigType(style)
	err := v.ReadConfig(reader)
	if err != nil {
		return nil, err
	}

	// Expand ${VAR} secret references against this node's environment.
	// This happens at the parse boundary, after the TOML/JSON is parsed
	// but before the sub-parser sees any value, so the raw template
	// bytes are what get proposed into Raft and stored in bbolt — only
	// the live in-memory config carries the resolved secret. Setting
	// each interpolated top-level subtree as a viper override leaves
	// every downstream Get* reading the expanded value.
	settings := v.AllSettings()
	if err := config.Interpolate(settings); err != nil {
		return nil, err
	}
	for k, val := range settings {
		v.Set(k, val)
	}

	return v, nil
}
