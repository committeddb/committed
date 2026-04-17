package http_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	synchttp "github.com/philborlin/committed/internal/cluster/syncable/http"
)

func readConfig(t *testing.T, configType string, r io.Reader) *viper.Viper {
	t.Helper()
	v := viper.New()
	v.SetConfigType(configType)
	err := v.ReadConfig(r)
	require.NoError(t, err)
	return v
}

func TestParseConfig_Simple(t *testing.T) {
	bs, err := os.ReadFile("./simple_webhook.toml")
	require.NoError(t, err)

	v := readConfig(t, "toml", bytes.NewReader(bs))
	p := &synchttp.SyncableParser{}
	config, err := p.ParseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "test-topic", config.Topic)
	require.Equal(t, "http://localhost:8080/webhook", config.URL)
	require.Equal(t, "POST", config.Method)
	require.Equal(t, 3000, config.TimeoutMs)
	require.Len(t, config.Headers, 2)
	require.Equal(t, "Authorization", config.Headers[0].Name)
	require.Equal(t, "Bearer test-token", config.Headers[0].Value)
	require.Equal(t, "X-Custom", config.Headers[1].Name)
	require.Equal(t, "custom-value", config.Headers[1].Value)
}

func TestParseConfig_Defaults(t *testing.T) {
	toml := `
[syncable]
name = "minimal"
type = "http"

[http]
topic = "t1"
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	config, err := p.ParseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "POST", config.Method)
	require.Equal(t, 5000, config.TimeoutMs)
	require.Empty(t, config.Headers)
}

func TestParseConfig_EnvExpansion(t *testing.T) {
	t.Setenv("TEST_WEBHOOK_TOKEN", "secret123")

	toml := `
[syncable]
name = "env"
type = "http"

[http]
topic = "t1"
url = "http://example.com/hook"

[[http.headers]]
name = "Authorization"
value = "Bearer ${TEST_WEBHOOK_TOKEN}"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	config, err := p.ParseConfig(v)
	require.NoError(t, err)

	require.Equal(t, "Bearer secret123", config.Headers[0].Value)
}

func TestParseConfig_MissingURL(t *testing.T) {
	toml := `
[syncable]
name = "bad"
type = "http"

[http]
topic = "t1"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	_, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "http.url is required")
}

func TestParseConfig_MissingTopic(t *testing.T) {
	toml := `
[syncable]
name = "bad"
type = "http"

[http]
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	_, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "http.topic is required")
}

func TestParseConfig_InvalidMethod(t *testing.T) {
	toml := `
[syncable]
name = "bad"
type = "http"

[http]
topic = "t1"
url = "http://example.com/hook"
method = "PATCH"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	_, err := p.ParseConfig(v)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be POST or PUT")
}

func TestParse_IgnoresDatabaseStorage(t *testing.T) {
	toml := `
[syncable]
name = "ok"
type = "http"

[http]
topic = "t1"
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	s, err := p.Parse(v, nil)
	require.NoError(t, err)
	require.NotNil(t, s)
	_ = s.Close()
}
