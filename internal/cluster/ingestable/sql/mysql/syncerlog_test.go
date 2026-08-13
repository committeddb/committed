package mysql

import (
	"bytes"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// TestSyncerLogger_NeverLogsReplicationPassword is a regression guard, not a bug
// fix: today the MySQL replication password can't reach the logs even at Debug,
// for two independent reasons — (1) go-mysql clears BinlogSyncerConfig.Password
// before logging its "create BinlogSyncer" line, and (2) even if it didn't, our
// slog->zap adapter serializes the config via zap.Any -> json.Marshal, which
// fails on the config's func fields (Option/Dialer/...) and emits a placeholder
// rather than the struct. Both are load-bearing but fragile across upgrades (a
// go-mysql version could drop the scrub or make the struct JSON-serializable),
// so this pins the real production path: build the config the way committed does
// (password populated), route it through the real NewBinlogSyncer with our
// adapter over a JSON core at Debug (matching zap.NewProduction in main.go), and
// assert the password never appears. If a future upgrade reintroduces the leak,
// this fails loudly.
func TestSyncerLogger_NeverLogsReplicationPassword(t *testing.T) {
	const secret = "sup3rSecretReplPassw0rd"

	cfg, err := binlogSyncerConfig(&sql.Config{
		ConnectionString: "mysql://repl:" + secret + "@127.0.0.1:3306/db",
	})
	require.NoError(t, err)
	require.Equal(t, secret, cfg.Password, "precondition: the config really carries the password")

	var buf bytes.Buffer
	cfg.Logger = newSyncerLogger(zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&buf),
		zapcore.DebugLevel, // capture the Info->Debug-demoted "create BinlogSyncer" line
	)))

	syncer := replication.NewBinlogSyncer(cfg) // logs "create BinlogSyncer" with the config
	t.Cleanup(syncer.Close)

	out := buf.String()
	require.Contains(t, out, "create BinlogSyncer", "the config log line must fire, else this guard is vacuous")
	require.NotContains(t, out, secret, "the replication password must never reach the logs, even at Debug")
}

// TestBinlogSyncerConfig_HalfOpenSocketGuard pins the two-part liveness guard
// against the reproducible field failure: a peer that dies without a FIN
// (host suspend, NAT idle timeout) froze the pure server→client binlog
// stream forever. Both halves must be present AND correctly ordered — a read
// deadline without heartbeats reconnect-churns on quiet sources, heartbeats
// without a deadline detect nothing.
func TestBinlogSyncerConfig_HalfOpenSocketGuard(t *testing.T) {
	cfg, err := binlogSyncerConfig(&sql.Config{
		ConnectionString: "mysql://u:p@127.0.0.1:3306/db",
	})
	require.NoError(t, err)

	require.Positive(t, cfg.HeartbeatPeriod,
		"heartbeats must flow through idle periods or the read deadline false-fires")
	require.Positive(t, cfg.ReadTimeout,
		"without a read deadline a half-open socket blocks the stream forever")
	require.Greater(t, cfg.ReadTimeout, 2*cfg.HeartbeatPeriod,
		"the deadline needs comfortable headroom over the heartbeat cadence")
}
