package mysql

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestBinlogSyncerConfig pins the syncer config built from a connection string,
// especially the decode-critical fields — UseDecimal/ParseTime wrong here would
// silently change DECIMAL/temporal columns and break the type matrix — and the
// host/port parse and non-zero ServerID (NewBinlogSyncer panics on 0).
func TestBinlogSyncerConfig(t *testing.T) {
	cfg, err := binlogSyncerConfig(&sql.Config{ConnectionString: "mysql://root:secret@127.0.0.1:3306/cdc"})
	require.NoError(t, err)

	require.Equal(t, "127.0.0.1", cfg.Host)
	require.Equal(t, uint16(3306), cfg.Port)
	require.Equal(t, "root", cfg.User)
	require.Equal(t, "secret", cfg.Password)
	require.Equal(t, mysql.DEFAULT_FLAVOR, cfg.Flavor)
	require.Equal(t, mysql.DEFAULT_CHARSET, cfg.Charset)
	require.NotZero(t, cfg.ServerID, "NewBinlogSyncer panics on a zero ServerID")

	require.False(t, cfg.UseDecimal, "DECIMAL must stay exact source text, not decimal.Decimal")
	require.False(t, cfg.ParseTime, "DATE/DATETIME/TIMESTAMP must stay strings, not time.Time")
	require.Nil(t, cfg.TimestampStringLocation)
}

// TestBinlogSyncerConfigErrors covers the parse failures that should surface as
// config errors rather than a bad connection later.
func TestBinlogSyncerConfigErrors(t *testing.T) {
	for _, cs := range []string{
		"mysql://root@hostwithoutport/cdc", // host has no :port
		"mysql://root@host:notaport/cdc",   // port not numeric
		"://bad",                           // unparseable
	} {
		_, err := binlogSyncerConfig(&sql.Config{ConnectionString: cs})
		require.Error(t, err, cs)
	}
}

func TestIsSkippableFakeRotate(t *testing.T) {
	require.True(t, isSkippableFakeRotate(0, "binlog.000007", "binlog.000007"), "fake rotate to same file is skipped")
	require.False(t, isSkippableFakeRotate(0, "binlog.000008", "binlog.000007"), "fake rotate to a new file is a real rotation")
	require.False(t, isSkippableFakeRotate(123, "binlog.000007", "binlog.000007"), "a non-zero timestamp is a real rotate")
}

func TestRowsAction(t *testing.T) {
	for _, tc := range []struct {
		typ  replication.EnumRowsEventType
		want string
		ok   bool
	}{
		{replication.EnumRowsEventTypeInsert, "insert", true},
		{replication.EnumRowsEventTypeUpdate, "update", true},
		{replication.EnumRowsEventTypeDelete, "delete", true},
		{replication.EnumRowsEventTypeUnknown, "", false},
	} {
		got, ok := rowsAction(tc.typ)
		require.Equal(t, tc.ok, ok)
		require.Equal(t, tc.want, got)
	}
}
