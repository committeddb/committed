package http_test

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/config"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
)

func readConfig(t *testing.T, configType string, r io.Reader) *cluster.ParsedConfig {
	t.Helper()
	bs, err := io.ReadAll(r)
	require.NoError(t, err)
	mimeType := "text/toml"
	if configType == "json" {
		mimeType = "application/json"
	}
	v, err := cluster.ParseConfigBytes(mimeType, bs)
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
	// No cadence configured → zero policy (the worker resolves it to the
	// default Every=1 for this single-path syncable).
	require.Equal(t, cluster.CheckpointPolicy{}, config.Checkpoint)
}

func TestParseConfig_Checkpoint(t *testing.T) {
	toml := `
[syncable]
name = "cadenced"
type = "http"
checkpointEvery = 50
checkpointMaxAge = "250ms"

[http]
topic = "t1"
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	config, err := p.ParseConfig(v)
	require.NoError(t, err)
	require.Equal(t, cluster.CheckpointPolicy{Every: 50, MaxAge: 250 * time.Millisecond}, config.Checkpoint)

	// The constructed syncable exposes it via CheckpointConfigurable.
	s := synchttp.New(config)
	cc, ok := any(s).(cluster.CheckpointConfigurable)
	require.True(t, ok, "http syncable must implement CheckpointConfigurable")
	require.Equal(t, config.Checkpoint, cc.CheckpointPolicy())
}

func TestParseConfig_CheckpointRejectsBadEvery(t *testing.T) {
	toml := `
[syncable]
name = "bad"
type = "http"
checkpointEvery = 0

[http]
topic = "t1"
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	_, err := p.ParseConfig(v)
	require.Error(t, err, "checkpointEvery < 1 must be rejected at parse time")
}

func TestParseConfig_CheckpointRejectsBadMaxAge(t *testing.T) {
	toml := `
[syncable]
name = "bad"
type = "http"
checkpointMaxAge = "not-a-duration"

[http]
topic = "t1"
url = "http://example.com/hook"
`
	v := readConfig(t, "toml", bytes.NewBufferString(toml))
	p := &synchttp.SyncableParser{}
	_, err := p.ParseConfig(v)
	require.Error(t, err, "a malformed checkpointMaxAge must be rejected at parse time")
}

// webhookHeaderTOML is a webhook syncable config with one Authorization header
// carrying value, used by the secret-interpolation regressions.
func webhookHeaderTOML(value string) string {
	return `
[syncable]
name = "hook"
type = "http"

[http]
topic = "t1"
url = "http://example.com/hook"

[[http.headers]]
name = "Authorization"
value = "` + value + `"
`
}

// TestParseConfig_HeaderValueNotExpandedHere: the webhook parser must NOT
// interpolate ${VAR} in a header value. Secret interpolation runs exactly once,
// at the db/parser boundary (config.Interpolate); called directly here (bypassing
// the boundary) the value passes through verbatim. A second expansion at this
// layer is the bug this guards against.
func TestParseConfig_HeaderValueNotExpandedHere(t *testing.T) {
	v := readConfig(t, "toml", bytes.NewBufferString(webhookHeaderTOML("Bearer ${TEST_WEBHOOK_TOKEN}")))
	cfg, err := (&synchttp.SyncableParser{}).ParseConfig(v)
	require.NoError(t, err)
	require.Equal(t, "Bearer ${TEST_WEBHOOK_TOKEN}", cfg.Headers[0].Value,
		"the parser must not interpolate; the db/parser boundary already did")
}

// TestWebhookHeaderSecretWithDollarSurvivesBoundary drives the full production
// sequence — boundary interpolation, then the parser — and asserts a resolved
// secret containing a literal '$' survives verbatim. The removed second
// os.ExpandEnv pass re-expanded the '$…' and corrupted the token.
func TestWebhookHeaderSecretWithDollarSurvivesBoundary(t *testing.T) {
	t.Setenv("HOOK_TOKEN", "s3cr3t$with$dollars")
	v := readConfig(t, "toml", bytes.NewBufferString(webhookHeaderTOML("Bearer ${HOOK_TOKEN}")))
	require.NoError(t, config.Interpolate(v.Values())) // the db/parser boundary
	cfg, err := (&synchttp.SyncableParser{}).ParseConfig(v)
	require.NoError(t, err)
	require.Equal(t, "Bearer s3cr3t$with$dollars", cfg.Headers[0].Value)
}

// TestWebhookHeaderUnsetVarIsHardError: an unset ${VAR} must fail loudly at the
// boundary, never yield an empty credential. The removed os.ExpandEnv fail-OPENed
// an unset var to "" — silently sending `Authorization: Bearer `.
func TestWebhookHeaderUnsetVarIsHardError(t *testing.T) {
	// HOOK_TOKEN is deliberately unset.
	v := readConfig(t, "toml", bytes.NewBufferString(webhookHeaderTOML("Bearer ${HOOK_TOKEN}")))
	require.Error(t, config.Interpolate(v.Values()),
		"an unset ${VAR} must be a hard error, not a silently-empty credential")
}

// TestWebhookHeaderBareDollarIsLiteral: a bare $VAR (no braces) is not committed's
// interpolation grammar (${VAR}), so it stays literal — the removed os.ExpandEnv
// would have shell-expanded it (to empty if unset).
func TestWebhookHeaderBareDollarIsLiteral(t *testing.T) {
	t.Setenv("HOOK_TOKEN", "unused")
	v := readConfig(t, "toml", bytes.NewBufferString(webhookHeaderTOML("Bearer $HOOK_TOKEN")))
	require.NoError(t, config.Interpolate(v.Values()))
	cfg, err := (&synchttp.SyncableParser{}).ParseConfig(v)
	require.NoError(t, err)
	require.Equal(t, "Bearer $HOOK_TOKEN", cfg.Headers[0].Value,
		"a bare $VAR is a literal, not a secret reference")
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
