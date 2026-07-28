package mysql

import (
	"math/rand/v2"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
)

// binlogSyncerConfig builds the replication.BinlogSyncerConfig for the MySQL
// binlog stream, mirroring what canal's prepareSyncer produced from the same
// connection string so the migration off canal is byte-for-byte
// behavior-preserving. The decode-relevant fields are load-bearing:
//
//   - UseDecimal=false keeps DECIMAL columns as their exact source text (a
//     string), not a decimal.Decimal.
//   - ParseTime=false keeps DATE/DATETIME/TIMESTAMP/TIME as their text form, not a
//     time.Time.
//   - TimestampStringLocation=time.UTC renders a TIMESTAMP (stored as a UTC epoch,
//     tz-converted on read) in UTC. nil would use the committed node's LOCAL tz,
//     which is both non-deterministic across nodes and divergent from the snapshot
//     (the snapshot forces its session to UTC — see readBatch). DATETIME/DATE/TIME
//     are tz-agnostic literals, so this only affects TIMESTAMP.
//
// These are asserted by the e2e type matrix; getting them wrong silently corrupts
// the payload. ServerID is a non-zero random id (the replica id this connection
// registers under — NewBinlogSyncer panics on 0), as canal also randomized it.
func binlogSyncerConfig(config *sql.Config) (replication.BinlogSyncerConfig, error) {
	// cluster.ParseMySQLConn, not url.Parse or a bare split: it is the SINGLE
	// MySQL URL parse authority the DSN path also uses, so admission and this
	// runtime path resolve the same host/port (a portless URL defaults to 3306
	// on both, instead of the DSN path defaulting it while this path rejected
	// it). Errors are redaction-safe — never echo the ${VAR}-resolved string.
	conn, err := cluster.ParseMySQLConn(config.ConnectionString)
	if err != nil {
		return replication.BinlogSyncerConfig{}, err
	}
	// The same *tls.Config the go-sql-driver snapshot connection uses (both from
	// conn), so the CDC stream is secured identically to the snapshot — nil when
	// sslmode=disable. Mirrors Postgres, whose pgx URL carries sslmode to both.
	tlsCfg, err := conn.TLSClientConfig()
	if err != nil {
		return replication.BinlogSyncerConfig{}, err
	}

	return replication.BinlogSyncerConfig{
		//nolint:gosec // G404: a MySQL replica id, not security-sensitive; weak rand is fine (canal randomizes it the same way).
		ServerID:   1001 + rand.Uint32N(1<<31),
		Flavor:     mysql.DEFAULT_FLAVOR,
		Host:       conn.Host,
		Port:       conn.Port,
		User:       conn.User,
		Password:   conn.Password,
		TLSConfig:  tlsCfg,
		Charset:    mysql.DEFAULT_CHARSET,
		UseDecimal: false,
		// Emit JSON-embedded DECIMAL leaves as exact unquoted numbers so a CDC
		// payload is byte-identical to the initial-snapshot path (which renders
		// the same decimal exact and unquoted — see readBatch's type-aware JSON
		// rendering). Without this, go-mysql quotes a JSON decimal ("1.50") while
		// the snapshot emits a number (1.50), breaking replay/dedup byte-compare.
		// Backed by committed's forked go-mysql (third_party/forked/go-mysql).
		UseNumberForJSONDecimal: true,
		ParseTime:               false,
		TimestampStringLocation: time.UTC,
		Logger:                  newSyncerLogger(zap.L()),
		// Bound compressed-transaction decompression so a large or zstd-bomb
		// transaction can't OOM-crash-loop the node (see sql.MaxDecompressedTxnBytes).
		// Backed by committed's forked go-mysql (third_party/forked/go-mysql).
		PayloadDecoderMaxDecompressedSize: sql.MaxDecompressedTxnBytes,
	}, nil
}

// isSkippableFakeRotate reports whether a RotateEvent is the fake rotate MySQL
// emits at the start of a stream (Header.Timestamp == 0) that only restates the
// current binlog file and carries no real position. It mirrors canal: a fake
// rotate naming a *different* file is a real rotation and must be handled, so
// only a same-file fake rotate is skipped.
func isSkippableFakeRotate(timestamp uint32, nextLogName, currentFile string) bool {
	return timestamp == 0 && nextLogName == currentFile
}

// rowsAction maps a RowsEvent's operation to committed's action string (the
// values canal used: "insert" / "update" / "delete"). ok is false for an
// unknown/unsupported rows event, which the stream loop skips.
func rowsAction(t replication.EnumRowsEventType) (action string, ok bool) {
	switch t {
	case replication.EnumRowsEventTypeInsert:
		return "insert", true
	case replication.EnumRowsEventTypeUpdate:
		return "update", true
	case replication.EnumRowsEventTypeDelete:
		return "delete", true
	}
	return "", false
}
