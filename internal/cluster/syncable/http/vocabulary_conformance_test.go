package http_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/syncable/http"
)

func TestHTTPVocabulary_EqualsTheReads(t *testing.T) {
	read := cluster.ObserveConfigReads(func() {
		v := readConfig(t, "toml", bytes.NewReader([]byte(`
[http]
topic     = "simple"
url       = "https://example.test/hook"
method    = "put"
timeoutMs = 250
[[http.headers]]
name  = "X-Auth"
value = "t"
`)))
		p := &http.SyncableParser{}
		_, err := p.ParseConfig(v)
		require.NoError(t, err)
		_ = p.TopicsFromConfig(v)
	})
	undeclared, unread := cluster.VocabularyDiff(http.HTTPKeys, read["http"])
	require.Empty(t, undeclared, "[http]: keys read but not declared")
	require.Empty(t, unread, "[http]: keys declared but never read")
}
