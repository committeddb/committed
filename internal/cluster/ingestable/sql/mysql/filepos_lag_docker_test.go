//go:build docker

package mysql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/mysql"
)

// The file:pos lag fallback against a live server: a streaming checkpoint
// WITHOUT a GTID set (gtid_mode=OFF / legacy shape) must report a real
// bytes-behind Lag with LagUnit "bytes" — file:pos deployments are no longer
// lag-blind — and a consumed file missing from the binlog inventory must
// surface the distinct re-snapshot state, the file:pos analog of the
// gtid_purged hole.
func TestMysqlStatusFileposBytesBehindLag(t *testing.T) {
	db := createDB(t)
	defer db.Close()

	// The oldest live binlog file is a coordinate the wade started behind:
	// consumed at its start, everything after it is lag.
	rows, err := db.Query("SHOW BINARY LOGS")
	require.NoError(t, err)
	cols, err := rows.Columns()
	require.NoError(t, err)
	var firstFile string
	var firstSize uint64
	require.True(t, rows.Next(), "server must have at least one binlog file")
	dest := make([]any, len(cols))
	dest[0], dest[1] = &firstFile, &firstSize
	for i := 2; i < len(cols); i++ {
		var sink []byte
		dest[i] = &sink
	}
	require.NoError(t, rows.Scan(dest...))
	require.NoError(t, rows.Close())

	d := &mysql.MySQLDialect{}
	cfg := &sql.Config{ConnectionString: ingestURL, Tables: []string{"any"}}

	pos, err := proto.Marshal(&dialectpb.MySQLBinLogPosition{Name: firstFile, Pos: 4})
	require.NoError(t, err)
	st, err := d.Status(context.Background(), cfg, pos)
	require.NoError(t, err)
	require.Equal(t, "streaming", st.Phase)
	require.NotNil(t, st.Lag, "file:pos streaming must report bytes-behind lag")
	require.Equal(t, cluster.LagUnitBytes, st.LagUnit)
	require.Positive(t, *st.Lag, "consumed at offset 4 of the oldest file: everything after is lag")
	require.False(t, st.ReSnapshotRequired)

	// A consumed file the server no longer has (numbered below the oldest):
	// the purge hole, not an understated lag.
	gone, err := proto.Marshal(&dialectpb.MySQLBinLogPosition{Name: "binlog.000000", Pos: 4})
	require.NoError(t, err)
	st, err = d.Status(context.Background(), cfg, gone)
	require.NoError(t, err)
	require.True(t, st.ReSnapshotRequired, "purged consumed file must surface the re-snapshot state")
	require.Nil(t, st.Lag)
}
