package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildDSN_DoesNotEchoSecretOnConversionFailure guards the S6 landmine:
// buildDSN must never return the raw ${VAR}-resolved connection string (which
// carries the interpolated password) when the mysql:// -> DSN conversion fails.
// A postgres:// URL is a valid URL but MySQLDSN rejects the scheme, so it lands
// on the error branch with a password in the userinfo.
func TestBuildDSN_DoesNotEchoSecretOnConversionFailure(t *testing.T) {
	const secret = "sup3rSecretPw"

	got := buildDSN("postgres://user:" + secret + "@db.example.com:5432/app")
	require.NotContains(t, got, secret,
		"buildDSN leaked the resolved connection-string password on conversion failure")
	require.Empty(t, got,
		"buildDSN should return an empty DSN on conversion failure so sql.Open fails cleanly without the secret")

	// Happy path unchanged: a valid mysql:// URL still converts to a usable DSN
	// (which legitimately embeds the password — that's what sql.Open needs).
	ok := buildDSN("mysql://user:" + secret + "@db.example.com:3306/app")
	require.NotEmpty(t, ok, "buildDSN must still convert a valid mysql:// URL")
}
