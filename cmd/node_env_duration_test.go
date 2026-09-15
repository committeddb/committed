package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// parseDisableableDurationEnv backs the settings where zero means DISABLE
// rather than "use the default". The shared parseDurationEnv treats a
// non-positive value as invalid and keeps the default, which is right for a
// timeout and wrong for a switch — COMMITTED_SCRUB_INTERVAL=0 promised to
// stop the erasure scrubber and did not.
func TestParseDisableableDurationEnv(t *testing.T) {
	const name = "COMMITTED_TEST_DISABLEABLE"

	t.Run("unset is no opinion", func(t *testing.T) {
		_, ok := parseDisableableDurationEnv(name)
		require.False(t, ok, "the caller keeps its default")
	})

	// Every spelling of zero the help text's duration syntax admits, not just
	// the literal character: an operator writing "0s" meant to disable.
	for _, raw := range []string{"0", "0s", "0ms", " 0s "} {
		t.Run("zero disables: "+raw, func(t *testing.T) {
			t.Setenv(name, raw)
			d, ok := parseDisableableDurationEnv(name)
			require.True(t, ok, "a zero duration is a decision, not an error")
			require.Zero(t, d)
		})
	}

	t.Run("positive passes through", func(t *testing.T) {
		t.Setenv(name, "30m")
		d, ok := parseDisableableDurationEnv(name)
		require.True(t, ok)
		require.Equal(t, 30*time.Minute, d)
	})

	for _, raw := range []string{"soon", "-5s"} {
		t.Run("refused, caller defaults: "+raw, func(t *testing.T) {
			t.Setenv(name, raw)
			_, ok := parseDisableableDurationEnv(name)
			require.False(t, ok)
		})
	}
}

// parseDurationEnv is the positive-only intent over the same body: zero is
// meaningless for a timeout, so it warns and the caller keeps its default.
// Both intents must agree on whitespace and on a bad value — that is why they
// share an implementation.
func TestParseDurationEnv_PositiveOnly(t *testing.T) {
	const name = "COMMITTED_TEST_POSITIVE"

	t.Run("zero is not a disable here", func(t *testing.T) {
		t.Setenv(name, "0s")
		_, ok := parseDurationEnv(name)
		require.False(t, ok, "a timeout has no zero meaning; the caller defaults")
	})
	t.Run("whitespace is tolerated like its sibling", func(t *testing.T) {
		t.Setenv(name, " 45s ")
		d, ok := parseDurationEnv(name)
		require.True(t, ok)
		require.Equal(t, 45*time.Second, d)
	})
}

// shutdownTimeout reads through the same parser, so it agrees with every
// other duration setting on whitespace and on a bad value. It had its own
// hand-rolled copy, which is how the three came to differ.
func TestShutdownTimeout_UsesTheSharedParser(t *testing.T) {
	require.Equal(t, defaultShutdownTimeout, shutdownTimeout(), "unset is the default")

	t.Run("whitespace tolerated", func(t *testing.T) {
		t.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", " 45s ")
		require.Equal(t, 45*time.Second, shutdownTimeout())
	})
	for _, raw := range []string{"soon", "0s", "-1s"} {
		t.Run("falls back: "+raw, func(t *testing.T) {
			t.Setenv("COMMITTED_SHUTDOWN_TIMEOUT", raw)
			require.Equal(t, defaultShutdownTimeout, shutdownTimeout())
		})
	}
}

// Every COMMITTED_* parser tolerates surrounding whitespace. Deployment
// tooling (templated env files, YAML block scalars) routinely emits a
// trailing newline or space, and a node must not refuse a setting over it —
// nor honor it for one type and reject it for another, which is what having
// per-type hand-rolled parsers produced.
func TestEnvParsers_TolerateWhitespaceUniformly(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		t.Setenv("COMMITTED_TEST_INT", " 4096 ")
		v, ok := parseInt64Env("COMMITTED_TEST_INT")
		require.True(t, ok)
		require.Equal(t, int64(4096), v)
	})
	t.Run("percent", func(t *testing.T) {
		t.Setenv("COMMITTED_TEST_PCT", " 12.5 ")
		require.InDelta(t, 12.5, parsePercentEnv("COMMITTED_TEST_PCT"), 0.0001)
	})
	t.Run("bool", func(t *testing.T) {
		t.Setenv("COMMITTED_TEST_BOOL", " true ")
		v, err := boolEnv("COMMITTED_TEST_BOOL")
		require.NoError(t, err)
		require.True(t, v)
	})
	t.Run("duration", func(t *testing.T) {
		t.Setenv("COMMITTED_TEST_DUR", " 90s ")
		d, ok := parseDurationEnv("COMMITTED_TEST_DUR")
		require.True(t, ok)
		require.Equal(t, 90*time.Second, d)
	})
}

// boolEnv REPORTS a bad value rather than exiting, so a helper that must
// hand its caller an error — loadProxyClient documents exactly that, so its
// failure cases are testable — does not inherit a process exit from a shared
// parser. The node command's own startup reads opt into exiting via
// boolEnvOrExit.
func TestBoolEnv_ReportsRatherThanExits(t *testing.T) {
	const name = "COMMITTED_TEST_BOOL_BAD"

	v, err := boolEnv(name)
	require.NoError(t, err, "unset is simply false")
	require.False(t, v)

	t.Setenv(name, "yes")
	_, err = boolEnv(name)
	require.Error(t, err, "a value ParseBool rejects must not read as false")
	require.Contains(t, err.Error(), "yes")
}

// The same value reaches loadProxyClient as an error, not as a dead test
// binary — the contract its doc comment states.
func TestLoadProxyClient_ReportsABadBoolean(t *testing.T) {
	t.Setenv("COMMITTED_HTTP_CLIENT_TLS_INSECURE_SKIP_VERIFY", "yes")
	_, err := loadProxyClient()
	require.Error(t, err)
	require.Contains(t, err.Error(), "COMMITTED_HTTP_CLIENT_TLS_INSECURE_SKIP_VERIFY")
}

// COMMITTED_NODE_ID reads through the same whitespace contract as every other
// setting — a templated " 3\n" is node 3, not a refusal — and the backup
// command's provenance stamp reuses parseNodeID rather than its own copy.
func TestParseNodeID_TrimsLikeEveryOtherSetting(t *testing.T) {
	id, err := parseNodeID(" 3 \n")
	require.NoError(t, err)
	require.Equal(t, uint64(3), id)

	id, err = parseNodeID("  ")
	require.NoError(t, err, "whitespace-only is unset")
	require.Equal(t, uint64(1), id)
}

// COMMITTED_API_TOKEN is trimmed at its one read: a secret materialised from
// a file carries a trailing newline, and untrimmed it fails from every
// direction (server compare, committed's own sender, external clients).
func TestAPITokenEnv_TrimsAFileSourcedSecret(t *testing.T) {
	t.Setenv("COMMITTED_API_TOKEN", "s3cr3t\n")
	require.Equal(t, "s3cr3t", apiTokenEnv())

	t.Setenv("COMMITTED_API_TOKEN", "")
	require.Empty(t, apiTokenEnv(), "unset stays unset — auth stays off, loudly, per the node's floor")
}
