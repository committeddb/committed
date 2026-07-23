package cluster_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
)

// TestParseConnString covers the contract every dialect depends on: a valid URL
// parses, and every parse failure yields an error that names the reason but
// never echoes the (already ${VAR}-resolved, password-bearing) connection string.
func TestConnStringHasInlinePassword(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want bool
	}{
		{"literal password", "postgres://user:hunter2@host:5432/db", true},
		{"literal password with specials", "mysql://cdc:S3cr3t!@10.0.0.5:3306/shop", true},
		{"var password", "postgres://user:${DB_PASSWORD}@host:5432/db", false},
		{"var password concatenated", "postgres://user:pre${DB_PASS}@host/db", false},
		{"whole string is a var", "${ORDERS_DATABASE_URL}", false},
		{"no password", "postgres://user@host:5432/db", false},
		{"no credentials", "postgres://host:5432/db", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, cluster.ConnStringHasInlinePassword(tc.in))
		})
	}
}

func TestParseConnString(t *testing.T) {
	t.Run("valid parses", func(t *testing.T) {
		u, err := cluster.ParseConnString("postgres://user:pass@localhost:5432/db?sslmode=disable")
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
			_, err := cluster.ParseConnString(tc.in)
			require.Error(t, err)
			require.NotContains(t, err.Error(), secret, "parse error leaked the connection string")
			require.Contains(t, err.Error(), "invalid connection string", "error must still name the problem")
		})
	}
}
