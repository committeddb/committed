package dialects_test

import (
	gosql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// secret is the (already ${VAR}-resolved) plaintext password embedded in every
// connection string below. No rejection path may echo it.
const secret = "sup3rSecretPassw0rd"

// opener is the one method both SQL syncable dialects share: turn a resolved
// connection string into a *sql.DB.
type opener interface {
	Open(string) (*gosql.DB, error)
}

// TestSyncableOpen_CanonicalURLContract pins the connection-string contract the
// A5 rework made uniform: a syncable/database connection string is a URL on BOTH
// the ingest and the syncable side — postgres:// for Postgres, mysql:// for MySQL
// — and each dialect enforces that identically while keeping the resolved secret
// out of every rejection it emits (these errors surface on GET /node/status and
// in dead-letter records). Before the rework the two sides diverged: MySQL took a
// bare go-sql-driver DSN and Postgres also accepted a libpq keyword string, so a
// literal '%' meant different things depending on where the string came from. The
// three axes below are asserted on both dialects so they cannot drift apart again.
func TestSyncableOpen_CanonicalURLContract(t *testing.T) {
	cases := []struct {
		name    string
		dialect opener
		// validURL: a well-formed canonical URL the dialect must accept (the
		// drivers dial lazily, so a good URL opens without a live server).
		validURL string
		// legacyForm: the same instance in the pre-canonical shape — a bare
		// go-sql-driver DSN (MySQL) or a libpq keyword string (Postgres) — which
		// must now be rejected as a non-URL, without leaking the password.
		legacyForm string
		// malformedURL: a URL-shaped string with a bad %-escape, which pgx/url.Parse
		// would otherwise echo verbatim (password included) in a *url.Error.
		malformedURL string
	}{
		{
			name:         "postgres",
			dialect:      &dialects.PostgreSQLDialect{},
			validURL:     "postgres://user:" + secret + "@host:5432/db",
			legacyForm:   "host=localhost port=5432 user=u password=" + secret + " dbname=db",
			malformedURL: "postgres://user:" + secret + "@host:5432/db%zz",
		},
		{
			name:         "mysql",
			dialect:      &dialects.MySQLDialect{},
			validURL:     "mysql://user:" + secret + "@host:3306/db",
			legacyForm:   "user:" + secret + "@tcp(127.0.0.1:3306)/db",
			malformedURL: "mysql://user:" + secret + "@host:3306/db%zz",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+"/valid_url_accepted", func(t *testing.T) {
			// pgx and go-sql-driver both defer the real dial to first use, so a
			// well-formed URL opens without error even with no server present. The
			// assertion is only that the canonical form is NOT false-rejected.
			db, err := tc.dialect.Open(tc.validURL)
			if db != nil {
				_ = db.Close()
			}
			require.NoError(t, err, "a well-formed canonical URL must be accepted")
		})

		t.Run(tc.name+"/legacy_form_rejected_no_leak", func(t *testing.T) {
			db, err := tc.dialect.Open(tc.legacyForm)
			if db != nil {
				_ = db.Close()
			}
			require.Error(t, err, "a non-URL (legacy DSN/keyword) connection string must be rejected")
			require.NotContains(t, err.Error(), secret, "rejection leaked the password")
		})

		t.Run(tc.name+"/malformed_url_redacted", func(t *testing.T) {
			db, err := tc.dialect.Open(tc.malformedURL)
			if db != nil {
				_ = db.Close()
			}
			require.Error(t, err, "a malformed URL must be rejected")
			require.NotContains(t, err.Error(), secret, "rejection leaked the password")
		})
	}
}
