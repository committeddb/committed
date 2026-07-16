package mysql

import (
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

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
	// sql.ParseConnString, not url.Parse: url.Parse's *url.Error embeds the raw
	// (already ${VAR}-resolved) connection string — password included — which
	// this path then logs at ingest runtime. The helper strips the value.
	u, err := sql.ParseConnString(config.ConnectionString)
	if err != nil {
		return replication.BinlogSyncerConfig{}, err
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		return replication.BinlogSyncerConfig{}, fmt.Errorf("connection host %q must be host:port: %w", u.Host, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return replication.BinlogSyncerConfig{}, fmt.Errorf("connection port %q: %w", portStr, err)
	}
	password, _ := u.User.Password()

	return replication.BinlogSyncerConfig{
		//nolint:gosec // G404: a MySQL replica id, not security-sensitive; weak rand is fine (canal randomizes it the same way).
		ServerID:   1001 + rand.Uint32N(1<<31),
		Flavor:     mysql.DEFAULT_FLAVOR,
		Host:       host,
		Port:       uint16(port),
		User:       u.User.Username(),
		Password:   password,
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
