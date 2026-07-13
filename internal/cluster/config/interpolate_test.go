package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// env builds a lookupFunc backed by a fixed map so the tests don't touch
// the real process environment.
func env(m map[string]string) lookupFunc {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestExpand(t *testing.T) {
	vars := env(map[string]string{
		"PW":    "s3cr3t",
		"USER":  "admin",
		"EMPTY": "",
	})

	t.Run("no reference is unchanged", func(t *testing.T) {
		got, err := expand("user:pass@tcp(localhost:3306)/db", vars)
		require.NoError(t, err)
		require.Equal(t, "user:pass@tcp(localhost:3306)/db", got)
	})

	t.Run("single reference expands", func(t *testing.T) {
		got, err := expand("password=${PW}", vars)
		require.NoError(t, err)
		require.Equal(t, "password=s3cr3t", got)
	})

	t.Run("multiple references expand", func(t *testing.T) {
		got, err := expand("${USER}:${PW}@tcp", vars)
		require.NoError(t, err)
		require.Equal(t, "admin:s3cr3t@tcp", got)
	})

	t.Run("empty value is allowed", func(t *testing.T) {
		// Set-but-empty is distinct from unset: an operator may
		// legitimately configure an empty value.
		got, err := expand("x=${EMPTY}y", vars)
		require.NoError(t, err)
		require.Equal(t, "x=y", got)
	})

	t.Run("missing variable is a typed error", func(t *testing.T) {
		_, err := expand("${NOPE}", vars)
		require.Error(t, err)
		var missing *MissingVarError
		require.True(t, errors.As(err, &missing))
		require.Equal(t, "NOPE", missing.Name)
	})

	t.Run("double dollar escapes to a literal dollar", func(t *testing.T) {
		got, err := expand("p$$w0rd", vars)
		require.NoError(t, err)
		require.Equal(t, "p$w0rd", got)
	})

	t.Run("escaped dollar is not treated as a reference", func(t *testing.T) {
		// $${PW} → literal "${PW}", NOT the value of PW.
		got, err := expand("$${PW}", vars)
		require.NoError(t, err)
		require.Equal(t, "${PW}", got)
	})

	t.Run("lone dollar is preserved", func(t *testing.T) {
		got, err := expand("cost is $5 for $foo", vars)
		require.NoError(t, err)
		require.Equal(t, "cost is $5 for $foo", got)
	})

	t.Run("unterminated reference errors", func(t *testing.T) {
		_, err := expand("${PW", vars)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unterminated")
	})

	t.Run("empty reference errors", func(t *testing.T) {
		_, err := expand("${}", vars)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty")
	})

	// Interpolation runs on every string value, including a connection string an
	// operator may (against guidance) have inlined a plaintext password into
	// rather than using ${VAR}. A malformed ${ in that value must not echo the
	// whole value — the error names the position, not the secret.
	t.Run("malformed reference does not echo the value", func(t *testing.T) {
		const secret = "sup3rSecretPassw0rd"
		for _, in := range []string{
			"postgres://u:" + secret + "@h/db?x=${PW", // unterminated
			"postgres://u:" + secret + "@h/db?x=${}",  // empty
		} {
			_, err := expand(in, vars)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret, "malformed ${} error leaked the surrounding value")
		}
	})
}

func TestInterpolate_Tree(t *testing.T) {
	vars := env(map[string]string{"PW": "s3cr3t", "TOK": "abc123"})

	settings := map[string]interface{}{
		"sql": map[string]interface{}{
			"connectionString": "user:${PW}@tcp",
			"dialect":          "mysql", // no reference, untouched
			"mysql": map[string]interface{}{
				"password": "${PW}",
			},
		},
		"http": map[string]interface{}{
			"headers": []interface{}{
				map[string]interface{}{
					"name":  "Authorization",
					"value": "Bearer ${TOK}",
				},
			},
		},
		"count": int64(3), // non-string leaf, untouched
	}

	require.NoError(t, interpolateMap(settings, vars))

	sql := settings["sql"].(map[string]interface{})
	require.Equal(t, "user:s3cr3t@tcp", sql["connectionString"])
	require.Equal(t, "mysql", sql["dialect"])
	require.Equal(t, "s3cr3t", sql["mysql"].(map[string]interface{})["password"])

	header := settings["http"].(map[string]interface{})["headers"].([]interface{})[0].(map[string]interface{})
	require.Equal(t, "Bearer abc123", header["value"])
	require.Equal(t, int64(3), settings["count"])
}

func TestInterpolate_Tree_MissingVarPropagates(t *testing.T) {
	vars := env(map[string]string{})
	settings := map[string]interface{}{
		"sql": map[string]interface{}{"connectionString": "${PW}"},
	}
	err := interpolateMap(settings, vars)
	require.Error(t, err)
	var missing *MissingVarError
	require.True(t, errors.As(err, &missing))
	require.Equal(t, "PW", missing.Name)
}

// TestMissingVarError_MessageHintsAtEscape locks in the operator-facing
// guidance: interpolation runs on EVERY string value, so a literal
// "${...}" in a non-secret field (a projection default, a webhook body)
// trips the same unset-variable error as a real secret reference. The
// message must therefore point at the escape ($${NAME}) so an operator
// who meant the text literally can fix it without guessing.
func TestMissingVarError_MessageHintsAtEscape(t *testing.T) {
	msg := (&MissingVarError{Name: "MYSQL_PASSWORD"}).Error()

	// The core statement the secrets runbook and the degraded-config log
	// quote — kept intact so those references still read correctly.
	require.Contains(t, msg,
		`environment variable "MYSQL_PASSWORD" referenced in config is not set`)
	// The escape hint, naming the specific variable.
	require.Contains(t, msg, "$${MYSQL_PASSWORD}")
}
