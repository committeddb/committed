package dialects_test

import (
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster"
	synchttp "github.com/committeddb/committed/internal/cluster/syncable/http"
	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// TestClassificationRule_EntrySpecific is the SHARED cross-sink guard for the
// cluster.ErrPermanent classification rule: Permanent MUST mean ENTRY-SPECIFIC.
// Every sink must classify a value-shaped error (fails THIS row, would apply
// for others) as permanent, and an access/schema/routing-shaped error (fails
// EVERY row identically) as transient — so the worker wedges visibly and
// resumes on the operator's fix instead of dead-lettering real data. Both the
// SQL dialects (schema codes) and the webhook sink (auth codes) drifted from
// this rule; this one table keeps a new sink or code from drifting again.
func TestClassificationRule_EntrySpecific(t *testing.T) {
	pg := &dialects.PostgreSQLDialect{}
	my := &dialects.MySQLDialect{}
	pgPerm := func(code string) bool { return pg.IsPermanent(&pgconn.PgError{Code: code}) }
	myPerm := func(n uint16) bool { return my.IsPermanent(&mysql.MySQLError{Number: n}) }
	httpPerm := func(code int) bool { return errors.Is(synchttp.ClassifyStatus(code), cluster.ErrPermanent) }

	// VALUE-SHAPED (entry-specific) → PERMANENT on every sink.
	require.True(t, pgPerm("22003"), "pg numeric_value_out_of_range")
	require.True(t, pgPerm("23502"), "pg not_null_violation")
	require.True(t, myPerm(1264), "mysql out_of_range_value")
	require.True(t, myPerm(1062), "mysql duplicate_entry")
	require.True(t, httpPerm(422), "http unprocessable entity")
	require.True(t, httpPerm(400), "http bad request")

	// ACCESS / SCHEMA / ROUTING-SHAPED (fails every row) → TRANSIENT on every sink.
	require.False(t, pgPerm("42P01"), "pg undefined_table")
	require.False(t, pgPerm("42703"), "pg undefined_column")
	require.False(t, pgPerm("42501"), "pg insufficient_privilege")
	require.False(t, myPerm(1054), "mysql unknown_column")
	require.False(t, myPerm(1364), "mysql field_has_no_default")
	require.False(t, myPerm(1136), "mysql column_count_mismatch")
	require.False(t, httpPerm(401), "http unauthorized")
	require.False(t, httpPerm(403), "http forbidden")
	require.False(t, httpPerm(404), "http not found (routing)")
}
