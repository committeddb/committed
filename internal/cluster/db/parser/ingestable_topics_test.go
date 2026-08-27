package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/parser"
)

// ingestTopicExtractorParser implements both cluster.IngestableParser and
// cluster.IngestableTopicExtractor — the shape the real sql ingest parser has.
type ingestTopicExtractorParser struct{}

func (ingestTopicExtractorParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) {
	return nil, nil
}

func (ingestTopicExtractorParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	t := v.GetString("demo.topic")
	if t == "" {
		return nil
	}
	return []string{t}
}

// plainIngestParser implements only cluster.IngestableParser — a kind that
// can't report its topics contributes no producer edges.
type plainIngestParser struct{}

func (plainIngestParser) Parse(*cluster.ParsedConfig) (cluster.Ingestable, error) {
	return nil, nil
}

func TestIngestableTopics_DelegatesToExtractor(t *testing.T) {
	p := parser.New()
	p.AddIngestableParser("demo", ingestTopicExtractorParser{})

	topics, err := p.IngestableTopics("text/toml", []byte(`[ingestable]
name = "d"
type = "demo"
[demo]
topic = "orders"`))
	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, topics)
}

func TestIngestableTopics_NilForNonExtractorKind(t *testing.T) {
	p := parser.New()
	p.AddIngestableParser("plain", plainIngestParser{})

	topics, err := p.IngestableTopics("text/toml", []byte(`[ingestable]
name = "d"
type = "plain"`))
	require.NoError(t, err)
	require.Nil(t, topics, "a kind without the extractor contributes no producer edges")
}
