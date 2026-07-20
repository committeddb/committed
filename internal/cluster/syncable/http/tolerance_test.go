package http_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/http"
)

// Pins decode tolerances deployed webhook configs may depend on (the
// golden corpus from .claude-scratch/tickets/viper-containment.md):
// case-variant section/field names — including inside header entries —
// keep decoding, while header values keep their case (they are user
// data). Written against the current pipeline; must stay green across
// any decoder change.
func TestHTTPParseConfigToleratesCaseVariantKeys(t *testing.T) {
	variant := `
[HTTP]
Topic      = "simple"
URL        = "https://example.test/hook"
Method     = "put"
TimeoutMS  = 250

[[HTTP.Headers]]
Name  = "X-Auth"
Value = "CaseSensitiveToken"
`
	v := readConfig(t, "toml", bytes.NewReader([]byte(variant)))

	config, err := (&http.SyncableParser{}).ParseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "simple", config.Topic)
	require.Equal(t, "https://example.test/hook", config.URL)
	require.Equal(t, "PUT", config.Method)
	require.Equal(t, 250, config.TimeoutMs)
	require.Equal(t, []http.Header{{Name: "X-Auth", Value: "CaseSensitiveToken"}}, config.Headers,
		"header names and values are user data — case preserved")
}
