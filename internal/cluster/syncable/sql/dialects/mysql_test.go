package dialects_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/syncable/sql/dialects"
)

// TestMySQLDialect_IsPermanent pins the asymmetric-risk classification for the
// MySQL allowlist: only the enumerated data and schema/constraint error numbers
// are permanent. The negative cases are the load-bearing half — a
// wrongly-permanent error silently drops a proposal past the dead letter, so a
// regression that promotes a lock/deadlock/connection error to permanent must
// fail here.
func TestMySQLDialect_IsPermanent(t *testing.T) {
	d := &dialects.MySQLDialect{}

	tests := []struct {
		name      string
		number    uint16
		permanent bool
	}{
		// Data: a specific row's value is bad and will never apply.
		{"column_cannot_be_null", 1048, true},
		{"out_of_range_value", 1264, true},
		{"data_truncated", 1265, true},
		{"truncated_incorrect_value", 1292, true},
		{"incorrect_value_for_column", 1366, true},
		{"data_too_long", 1406, true},
		{"numeric_out_of_range", 1690, true},
		// Integrity constraint: THIS row violates it (entry-specific).
		{"duplicate_entry", 1062, true},
		{"fk_constraint_fails", 1452, true},
		{"check_constraint_violated", 3819, true},
		{"check_constraint_column", 4025, true},
		// Schema / mapping shaped — fails EVERY row identically, so TRANSIENT
		// (the MySQL mirror of the Postgres class-42 carve-out).
		{"unknown_column_is_transient", 1054, false},
		{"column_count_mismatch_is_transient", 1136, false},
		{"field_no_default_is_transient", 1364, false},

		// Infrastructure stays transient — retry forever, wedge visibly.
		{"lock_wait_timeout", 1205, false},
		{"deadlock", 1213, false},
		{"too_many_connections", 1040, false},
		{"too_many_user_connections", 1203, false},
		{"server_gone_away", 2006, false},
		{"lost_connection", 2013, false},
		{"query_interrupted", 1317, false},
		// An unlisted error number is transient by default.
		{"unknown_error_number", 9999, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &mysql.MySQLError{Number: tt.number, Message: tt.name}
			require.Equal(t, tt.permanent, d.IsPermanent(err),
				"MySQL error %d", tt.number)
		})
	}

	// A non-MySQL error (e.g. a transport/driver error with no error number) is
	// never permanent — there is no data/schema signal to act on, so it retries.
	t.Run("non_mysql_error_is_transient", func(t *testing.T) {
		require.False(t, d.IsPermanent(errors.New("dial tcp: connection refused")))
	})

	// errors.As must reach through a wrapped MySQLError.
	t.Run("wrapped_mysql_error", func(t *testing.T) {
		wrapped := fmt.Errorf("exec failed: %w", &mysql.MySQLError{Number: 1062})
		require.True(t, d.IsPermanent(wrapped))
	})
}
