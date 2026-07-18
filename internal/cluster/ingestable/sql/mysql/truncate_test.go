package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestTruncateTarget(t *testing.T) {
	tests := map[string]struct {
		query      string
		wantSchema string
		wantTable  string
		wantOK     bool
	}{
		"table keyword":              {"TRUNCATE TABLE users", "", "users", true},
		"no table keyword":           {"TRUNCATE users", "", "users", true},
		"backticked":                 {"TRUNCATE TABLE `users`", "", "users", true},
		"schema qualified":           {"TRUNCATE TABLE appdb.users", "appdb", "users", true},
		"schema qualified backticks": {"TRUNCATE TABLE `appdb`.`users`", "appdb", "users", true},
		"lowercase keyword":          {"truncate table users", "", "users", true},
		"trailing semicolon":         {"TRUNCATE TABLE users;", "", "users", true},
		"leading whitespace":         {"  TRUNCATE   TABLE   users  ", "", "users", true},
		"not a truncate (delete)":    {"DELETE FROM users", "", "", false},
		"not a truncate (alter)":     {"ALTER TABLE users ADD COLUMN c INT", "", "", false},
		"empty":                      {"", "", "", false},
		"bare truncate no target":    {"TRUNCATE", "", "", true}, // recognized, unnamed
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			schema, table, ok := truncateTarget(tt.query)
			require.Equal(t, tt.wantOK, ok)
			require.Equal(t, tt.wantSchema, schema)
			require.Equal(t, tt.wantTable, table)
		})
	}
}

const truncateDivergenceMsg = "TRUNCATE on a watched table is not propagated to the sink; " +
	"the sink now diverges from the source and must be re-snapshotted to reconcile"

// TestHandleDDL_WatchedTruncateEmitsDivergenceWarn pins the MySQL half of the
// TRUNCATE-divergence promise (cdc-setup.md): a TRUNCATE on a WATCHED table logs
// the same specific divergence Warn Postgres emits, naming the table — while a
// TRUNCATE on an unwatched table (MySQL's binlog is server-wide) and any
// non-TRUNCATE DDL fall through to the generic DDL warn, so the signal doesn't cry
// wolf.
func TestHandleDDL_WatchedTruncateEmitsDivergenceWarn(t *testing.T) {
	h := &MySQLEventHandler{schema: "appdb", tables: lowerAll([]string{"Users"})}

	t.Run("watched truncate → divergence warn naming the table", func(t *testing.T) {
		core, observed := observer.New(zap.WarnLevel)
		defer zap.ReplaceGlobals(zap.New(core))()

		h.handleDDL(&replication.QueryEvent{Schema: []byte("appdb"), Query: []byte("TRUNCATE TABLE `users`")})

		entries := observed.FilterMessage(truncateDivergenceMsg).All()
		require.Len(t, entries, 1, "a watched-table TRUNCATE must emit the specific divergence Warn")
		require.Equal(t, []any{"appdb.users"}, entries[0].ContextMap()["tables"],
			"the divergence Warn names the affected schema.table")
	})

	t.Run("unwatched truncate → generic warn, no divergence", func(t *testing.T) {
		core, observed := observer.New(zap.WarnLevel)
		defer zap.ReplaceGlobals(zap.New(core))()

		h.handleDDL(&replication.QueryEvent{Schema: []byte("appdb"), Query: []byte("TRUNCATE TABLE payments")})

		require.Empty(t, observed.FilterMessage(truncateDivergenceMsg).All(),
			"an unwatched table diverges no sink of this ingest — no false divergence alarm")
		require.Len(t, observed.FilterMessage("handleDDL: DDL event received").All(), 1)
	})

	t.Run("non-truncate DDL → generic warn only", func(t *testing.T) {
		core, observed := observer.New(zap.WarnLevel)
		defer zap.ReplaceGlobals(zap.New(core))()

		h.handleDDL(&replication.QueryEvent{Schema: []byte("appdb"), Query: []byte("ALTER TABLE users ADD COLUMN c INT")})

		require.Empty(t, observed.FilterMessage(truncateDivergenceMsg).All())
		require.Len(t, observed.FilterMessage("handleDDL: DDL event received").All(), 1)
	})
}
