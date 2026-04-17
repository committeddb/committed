package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/dialectpb"
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

func TestParseBatchSize(t *testing.T) {
	tests := []struct {
		name string
		opts map[string]string
		want int
	}{
		{"nil_options", nil, defaultSnapshotBatchSize},
		{"missing_key", map[string]string{"slot_name": "s"}, defaultSnapshotBatchSize},
		{"valid_override", map[string]string{"batch_size": "500"}, 500},
		{"invalid_non_numeric", map[string]string{"batch_size": "abc"}, defaultSnapshotBatchSize},
		{"zero_falls_back", map[string]string{"batch_size": "0"}, defaultSnapshotBatchSize},
		{"negative_falls_back", map[string]string{"batch_size": "-1"}, defaultSnapshotBatchSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseBatchSize(tt.opts))
		})
	}
}

func TestDecodePositionLegacyLSN(t *testing.T) {
	// Legacy format: raw 8-byte big-endian LSN, no snapshot progress.
	legacy := make([]byte, 8)
	binary.BigEndian.PutUint64(legacy, 0x0123456789ABCDEF)

	lsn, progress, err := decodePosition(legacy)
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0x0123456789ABCDEF), lsn)
	require.Nil(t, progress)
}

func TestDecodePositionEmpty(t *testing.T) {
	lsn, progress, err := decodePosition(nil)
	require.NoError(t, err)
	require.Equal(t, pglogrepl.LSN(0), lsn)
	require.Nil(t, progress)
}

func TestEncodeDecodePositionRoundtrip(t *testing.T) {
	tests := []struct {
		name     string
		lsn      pglogrepl.LSN
		progress *dialectpb.SnapshotProgress
	}{
		{
			name:     "lsn_only",
			lsn:      pglogrepl.LSN(42),
			progress: nil,
		},
		{
			name: "lsn_with_progress",
			lsn:  pglogrepl.LSN(0xDEADBEEF),
			progress: &dialectpb.SnapshotProgress{
				LastPkByTable:   map[string]string{"orders": "999", "items": "abc"},
				CompletedTables: []string{"customers"},
			},
		},
		{
			name: "zero_lsn_with_progress",
			lsn:  0,
			progress: &dialectpb.SnapshotProgress{
				LastPkByTable: map[string]string{"t": "k"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs, err := encodePosition(tt.lsn, tt.progress)
			require.NoError(t, err)
			require.Equal(t, pgPositionProtoMagic, bs[0],
				"encoded position must start with the magic byte")

			gotLSN, gotProgress, err := decodePosition(bs)
			require.NoError(t, err)
			require.Equal(t, tt.lsn, gotLSN)
			if tt.progress == nil {
				require.Nil(t, gotProgress)
			} else {
				require.NotNil(t, gotProgress)
				require.Equal(t, tt.progress.LastPkByTable, gotProgress.LastPkByTable)
				require.Equal(t, tt.progress.CompletedTables, gotProgress.CompletedTables)
			}
		})
	}
}
