package parser_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/db/parser"
)

// topicExtractorParser implements both cluster.SyncableParser and
// cluster.SyncableTopicExtractor — the shape a real syncable parser has.
type topicExtractorParser struct{}

func (topicExtractorParser) Parse(*cluster.ParsedConfig, cluster.DatabaseStorage) (cluster.Syncable, error) {
	return nil, nil
}

func (topicExtractorParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	t := v.GetString("demo.topic")
	if t == "" {
		return nil
	}
	return []string{t}
}

// plainParser implements only cluster.SyncableParser — a syncable kind that
// can't report its topics.
type plainParser struct{}

func (plainParser) Parse(*cluster.ParsedConfig, cluster.DatabaseStorage) (cluster.Syncable, error) {
	return nil, nil
}

func TestSyncableTopics_DelegatesToExtractor(t *testing.T) {
	p := parser.New()
	p.AddSyncableParser("demo", topicExtractorParser{})

	topics, err := p.SyncableTopics("text/toml", []byte(`[syncable]
name = "d"
type = "demo"
[demo]
topic = "orders"`))
	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, topics)
}

// A syncable kind whose parser doesn't extract topics contributes no dependents
// — nil, not an error, so enumeration keeps going.
func TestSyncableTopics_NonExtractorParserReturnsNil(t *testing.T) {
	p := parser.New()
	p.AddSyncableParser("plain", plainParser{})

	topics, err := p.SyncableTopics("text/toml", []byte(`[syncable]
name = "p"
type = "plain"`))
	require.NoError(t, err)
	require.Nil(t, topics)
}

func TestSyncableTopics_UnknownTypeErrors(t *testing.T) {
	p := parser.New()
	_, err := p.SyncableTopics("text/toml", []byte(`[syncable]
name = "x"
type = "nope"`))
	require.Error(t, err)
}

func TestSyncableTopics_UnparseableBytesError(t *testing.T) {
	p := parser.New()
	_, err := p.SyncableTopics("text/toml", []byte("this is : not { valid"))
	require.Error(t, err)
}
