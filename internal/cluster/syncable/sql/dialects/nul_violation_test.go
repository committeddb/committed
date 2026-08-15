package dialects_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// The NUL classifier keys on SQLSTATE 22021 (character_not_in_repertoire),
// including when the pg error arrives wrapped; every other shape — other
// data exceptions, non-pg errors — reads false so the hint never fires on
// the wrong class.
func TestPostgreSQLIsNulByteViolation(t *testing.T) {
	d := &dialects.PostgreSQLDialect{}

	nul := &pgconn.PgError{Code: "22021", Message: `invalid byte sequence for encoding "UTF8": 0x00`}
	require.True(t, d.IsNulByteViolation(nul))
	require.True(t, d.IsNulByteViolation(fmt.Errorf("exec: %w", nul)), "wrapped errors must classify")

	require.False(t, d.IsNulByteViolation(&pgconn.PgError{Code: "22003"}), "other data exceptions are not the NUL class")
	require.False(t, d.IsNulByteViolation(errors.New("not a pg error")))
	require.False(t, d.IsNulByteViolation(nil))
}
