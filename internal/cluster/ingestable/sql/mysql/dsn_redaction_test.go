package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenMySQL_DoesNotEchoSecretOnParseFailure guards the S6 landmine: opening
// the MySQL source must never surface the raw ${VAR}-resolved connection string
// (which carries the interpolated password) when the URL is rejected. A
// postgres:// URL is a valid URL but the mysql parser rejects the scheme, landing
// on the error branch with a password in the userinfo.
func TestOpenMySQL_DoesNotEchoSecretOnParseFailure(t *testing.T) {
	const secret = "sup3rSecretPw"

	_, err := openMySQL("postgres://user:" + secret + "@db.example.com:5432/app")
	require.Error(t, err)
	require.NotContains(t, err.Error(), secret,
		"openMySQL leaked the resolved connection-string password on parse failure")

	// Happy path: a valid mysql:// URL yields a usable (lazy) handle. OpenDB does
	// not dial, so this needs no live server.
	db, err := openMySQL("mysql://user:" + secret + "@db.example.com:3306/app")
	require.NoError(t, err)
	require.NotNil(t, db)
	require.NoError(t, db.Close())
}
