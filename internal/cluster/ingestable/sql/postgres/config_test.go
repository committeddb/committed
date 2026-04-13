package postgres

import (
	"testing"

	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/stretchr/testify/require"
)

func TestBuildPgConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      *sql.Config
		wantSlot    string
		wantPub     string
		wantTables  []string
		wantConnHas string // substring the replication conn string must contain
		wantSQLHas  string // substring the SQL conn string must contain
		wantSQLNot  string // substring the SQL conn string must NOT contain
	}{
		{
			name: "options_and_tables_from_config",
			config: &sql.Config{
				ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
				Tables:           []string{"public.orders", "public.items"},
				Options: map[string]string{
					"slot_name":   "my_slot",
					"publication": "my_pub",
				},
			},
			wantSlot:    "my_slot",
			wantPub:     "my_pub",
			wantTables:  []string{"public.orders", "public.items"},
			wantConnHas: "replication=database",
			wantSQLHas:  "sslmode=disable",
			wantSQLNot:  "replication",
		},
		{
			name: "defaults_when_options_empty",
			config: &sql.Config{
				ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
				Tables:           []string{"mytable"},
				Options:          map[string]string{},
			},
			wantSlot:   "committed_slot",
			wantPub:    "committed_pub",
			wantTables: []string{"mytable"},
		},
		{
			name: "defaults_when_options_nil",
			config: &sql.Config{
				ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
				Tables:           []string{"mytable"},
			},
			wantSlot:   "committed_slot",
			wantPub:    "committed_pub",
			wantTables: []string{"mytable"},
		},
		{
			name: "nil_tables",
			config: &sql.Config{
				ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable",
			},
			wantSlot:   "committed_slot",
			wantPub:    "committed_pub",
			wantTables: nil,
		},
		{
			name: "replication_already_in_url_stripped_for_sql",
			config: &sql.Config{
				ConnectionString: "postgres://user:pass@localhost:5432/db?sslmode=disable&replication=database",
				Tables:           []string{"t"},
				Options:          map[string]string{},
			},
			wantSlot:    "committed_slot",
			wantPub:     "committed_pub",
			wantTables:  []string{"t"},
			wantConnHas: "replication=database",
			wantSQLNot:  "replication",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := buildPgConfig(tt.config)
			require.NoError(t, err)

			require.Equal(t, tt.wantSlot, cfg.slotName)
			require.Equal(t, tt.wantPub, cfg.publication)
			require.Equal(t, tt.wantTables, cfg.tables)

			if tt.wantConnHas != "" {
				require.Contains(t, cfg.connString, tt.wantConnHas)
			}
			if tt.wantSQLHas != "" {
				require.Contains(t, cfg.sqlConnString, tt.wantSQLHas)
			}
			if tt.wantSQLNot != "" {
				require.NotContains(t, cfg.sqlConnString, tt.wantSQLNot)
			}

			// Replication conn string should always have replication=database
			require.Contains(t, cfg.connString, "replication=database")
			// SQL conn string should never have replication param
			require.NotContains(t, cfg.sqlConnString, "replication")
		})
	}
}

func TestBuildPgConfigInvalidURL(t *testing.T) {
	config := &sql.Config{
		ConnectionString: "://invalid",
	}
	_, err := buildPgConfig(config)
	require.Error(t, err)
}

func TestQuoteTable(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders", `"orders"`},
		{"public.orders", `"public"."orders"`},
		{`my"table`, `"my""table"`},
		{"schema.my-table", `"schema"."my-table"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			require.Equal(t, tt.want, quoteTable(tt.input))
		})
	}
}
