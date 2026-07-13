package sql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestParseConnString covers the contract both dialects depend on: a valid URL
// parses, and every parse failure yields an error that names the reason but
// never echoes the (already ${VAR}-resolved, password-bearing) connection string.
func TestParseConnString(t *testing.T) {
	t.Run("valid parses", func(t *testing.T) {
		u, err := sql.ParseConnString("postgres://user:pass@localhost:5432/db?sslmode=disable")
		require.NoError(t, err)
		require.Equal(t, "localhost:5432", u.Host)
		pw, _ := u.User.Password()
		require.Equal(t, "pass", pw)
	})

	const secret = "sup3rSecretPassw0rd"
	for _, tc := range []struct {
		name, in string
	}{
		{"bad escape in path", "postgres://user:" + secret + "@h:5432/db%zz"},
		{"control char", "postgres://user:" + secret + "@h:5432/d\x7fb"},
		{"bad escape mysql", "mysql://root:" + secret + "@127.0.0.1:3306/cdc%zz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sql.ParseConnString(tc.in)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret, "parse error leaked the connection string")
			require.Contains(t, err.Error(), "invalid connection string", "error must still name the problem")
		})
	}
}
