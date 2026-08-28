package mysql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/committeddb/committed/internal/cluster/sqlident"
)

type MySQLDialect struct{}

// syncerBackoff{Min,Max} bound the retry interval for connecting the binlog
// syncer (StartSync) and reconnecting after a stream error. A transient MySQL
// outage (DNS blip, container restart, network partition) at Ingest startup
// used to immediately return an error, leaving the worker
// leader-but-not-ingesting with no recovery path. The retry loop below caps at
// Max and is bounded by ctx so a shutdown still propagates promptly.
// The per-transaction soft-flush is byte-bounded by sql.TxnSoftFlushBytes: a
// source transaction that cannot fit in one proposal is applied as ordered
// contiguous parts — the one documented exception to the
// proposal-is-a-transaction invariant (see cluster.Proposal and
// sql.TxnSoftFlushBytes), forced by the source data rather than chosen for
// convenience. flushSub keeps the parts' SourceSeqs strictly monotonic.

const (
	syncerBackoffMin = 1 * time.Second
	syncerBackoffMax = 30 * time.Second

	// defaultSnapshotBatchSize is the number of rows read per snapshot
	// batch when Config.Options has no "batch_size" override. The
	// snapshot uses keyset pagination (SELECT ... WHERE pk > :last_pk
	// LIMIT :batch_size) with a short transaction per batch, so this
	// also bounds how long each MVCC read-view is held.
	defaultSnapshotBatchSize = 10000
)

// Preflight implements sql.Dialect: it verifies the binlog row image carries the
// configured primaryKey on a DELETE, so the ingest can emit a keyed tombstone.
// FULL/NOBLOB carry the whole (non-blob) before-image; only MINIMAL trims to the
// row's identifying key, so only then must the table's PRIMARY KEY cover
// primaryKey.
func (m *MySQLDialect) Preflight(config *sql.Config) error {
	// Normalize to the per-topic model up front, single-threaded: a dialect entered
	// directly (a hand-built config, bypassing the sql.Ingestable wrapper) carries
	// only the flat singular fields, so the per-spec loops below (and Ingest/Status)
	// would otherwise see an empty Topics.
	config.EnsureTopics()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Operational warnings (logged, never fatal): GTID positioning governs
	// failover-safety and binlog retention governs the no-hold caveat, but
	// file:pos is a supported fallback and retention is the operator's call — so
	// these are surfaced, not gated. Delete-correctness (below) is the only hard
	// gate, matching the codebase's preflight philosophy.
	warnSourceConfig(ctx, db)

	var binlogFormat string
	if err := db.QueryRowContext(ctx, `SELECT @@global.binlog_format`).Scan(&binlogFormat); err != nil {
		return fmt.Errorf("read binlog_format: %w", err)
	}
	// ROW is required — the most fundamental gate: under STATEMENT (and MIXED's
	// statement-chosen paths) DML arrives as statement TEXT, not row events, so
	// committed's row-CDC silently captures nothing while the snapshot works —
	// silent, unbounded source/mirror divergence. The statement text also embeds
	// row VALUES (potential PII), which must never reach the (shipped) log path
	// that observes query events. Reject loudly instead of part-working.
	if !strings.EqualFold(binlogFormat, "ROW") {
		return fmt.Errorf(
			"binlog_format is %q, but committed requires ROW: STATEMENT and MIXED deliver DML as statement text rather than row events, so change capture would silently miss rows. Set `binlog_format=ROW`",
			binlogFormat)
	}

	var rowImage string
	if err := db.QueryRowContext(ctx, `SELECT @@global.binlog_row_image`).Scan(&rowImage); err != nil {
		return fmt.Errorf("read binlog_row_image: %w", err)
	}
	// FULL is required. NOBLOB omits an unchanged BLOB/TEXT column from the UPDATE
	// after-image, and MINIMAL omits every unchanged column, so a partial UPDATE
	// would null those columns downstream — silent mirror corruption, the same
	// class as Postgres unchanged-TOAST. FULL also guarantees the primary key is
	// in the before-image, so deletes stay keyed.
	if !strings.EqualFold(rowImage, "FULL") {
		return fmt.Errorf(
			"binlog_row_image is %q, but committed requires FULL: NOBLOB and MINIMAL omit unchanged columns from the UPDATE after-image, so a partial UPDATE silently nulls them downstream. Set `binlog_row_image=FULL`",
			rowImage)
	}

	var rowMetadata string
	if err := db.QueryRowContext(ctx, `SELECT @@global.binlog_row_metadata`).Scan(&rowMetadata); err != nil {
		return fmt.Errorf("read binlog_row_metadata: %w", err)
	}
	// FULL is required: committed decodes each row image against the schema carried
	// in its OWN binlog TableMapEvent (column names + ENUM/SET labels), which the
	// server emits only under FULL. Under MINIMAL the event has no column names, so
	// an online schema change on the source would leave committed decoding a
	// still-replaying old-image row against the post-ALTER columns — silent mirror
	// corruption (see columnsFromTableMap).
	if !strings.EqualFold(rowMetadata, "FULL") {
		return fmt.Errorf(
			"binlog_row_metadata is %q, but committed requires FULL: the binlog carries column names and ENUM/SET labels only under FULL, and committed decodes each row against its own event's schema to stay correct across source DDL. Set `binlog_row_metadata=FULL`",
			rowMetadata)
	}

	var rowValueOptions string
	if err := db.QueryRowContext(ctx, `SELECT @@global.binlog_row_value_options`).Scan(&rowValueOptions); err != nil {
		return fmt.Errorf("read binlog_row_value_options: %w", err)
	}
	// Must be empty. PARTIAL_JSON logs only the JSON *diff* for a partially-updated
	// JSON column, which go-mysql surfaces as a *replication.JsonDiff rather than
	// the document bytes; committed can't reconstruct the document from a diff and
	// would marshal the diff struct into structurally wrong JSON — silent mirror
	// corruption. Require the full JSON document in the binlog.
	if strings.TrimSpace(rowValueOptions) != "" {
		return fmt.Errorf(
			"binlog_row_value_options is %q, but committed requires it empty: PARTIAL_JSON logs only a JSON diff for a partially-updated JSON column, which committed cannot reconstruct and would corrupt downstream. Set `binlog_row_value_options=''`",
			rowValueOptions)
	}

	// Reject a mapped spatial/VECTOR column rather than silently corrupt it. Each
	// topic-spec is checked against its own mapped/PK columns (one spec for the
	// flat form, N for [[sql.topics]]).
	for i := range config.Topics {
		if err := checkUnsupportedColumnTypes(ctx, db, &config.Topics[i]); err != nil {
			return err
		}
	}
	return nil
}

// unsupportedColumnTypes are MySQL column types committed cannot represent
// without silent corruption. The binary CDC and snapshot paths both hand these
// back as raw bytes with no lossless JSON form (json.Marshal would mangle them to
// U+FFFD), so committed rejects a config that maps one instead of corrupting it:
// spatial (GEOMETRY and its subtypes) and VECTOR (MySQL 9.0+). These are
// information_schema `data_type` spellings. (Postgres renders the equivalent
// PostGIS/pgvector columns as lossless `::text` and is unaffected.)
var unsupportedColumnTypes = map[string]bool{
	"geometry":           true,
	"point":              true,
	"linestring":         true,
	"polygon":            true,
	"multipoint":         true,
	"multilinestring":    true,
	"multipolygon":       true,
	"geometrycollection": true,
	"vector":             true,
}

// checkUnsupportedColumnTypes fails loudly if a mapped or primary-key column is a
// spatial or VECTOR type (see unsupportedColumnTypes). Only mapped/PK columns are
// checked: an unmapped spatial column is read but never rendered into a payload
// or key, so committed leaves it alone rather than rejecting the whole table.
func checkUnsupportedColumnTypes(ctx context.Context, db *gosql.DB, spec *sql.TopicSpec) error {
	used := make(map[string]bool, len(spec.Mappings)+len(spec.PrimaryKey))
	for _, m := range spec.Mappings {
		used[strings.ToLower(m.SQLColumn)] = true
	}
	for _, pk := range spec.PrimaryKey {
		used[strings.ToLower(pk)] = true
	}

	for _, table := range spec.Tables {
		offenders, err := unsupportedMappedColumns(ctx, db, table, used)
		if err != nil {
			return err
		}
		if len(offenders) > 0 {
			return fmt.Errorf(
				"table %q maps unsupported column type(s) %s: MySQL spatial and VECTOR columns have no "+
					"lossless representation on committed's binary CDC/snapshot paths and would be silently "+
					"corrupted. Remove them from the mapping (or exclude them under map-all)",
				table, strings.Join(offenders, ", "))
		}
	}
	return nil
}

// unsupportedMappedColumns returns the "name (data_type)" of each column in table
// that is both used (mapped or PK) and an unsupported spatial/VECTOR type.
func unsupportedMappedColumns(ctx context.Context, db *gosql.DB, table string, used map[string]bool) ([]string, error) {
	schema, name := splitQualifiedTable(table)
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
		  AND table_name = ?`, schema, name)
	if err != nil {
		return nil, fmt.Errorf("read column types of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var offenders []string
	for rows.Next() {
		var col, dtype string
		if err := rows.Scan(&col, &dtype); err != nil {
			return nil, err
		}
		if used[strings.ToLower(col)] && unsupportedColumnTypes[strings.ToLower(dtype)] {
			offenders = append(offenders, fmt.Sprintf("%s (%s)", col, dtype))
		}
	}
	return offenders, rows.Err()
}

// minSafeBinlogRetention is the binlog-retention floor below which Preflight
// warns: committed holds no binlog on the source (unlike a Postgres slot), so a
// downtime longer than retention purges unconsumed transactions and forces a
// re-snapshot. It is a heuristic "this looks risky" line, not a guarantee — the
// real safe value is the operator's worst-case downtime.
const minSafeBinlogRetention = time.Hour

// warnSourceConfig logs (never fails) the source-config conditions that degrade
// MySQL CDC but don't break it: GTID positioning off (no failover-safety, no
// lag/caughtUp) and short binlog retention (the no-hold caveat). A failed read
// is itself only logged — these are advisory.
func warnSourceConfig(ctx context.Context, db *gosql.DB) {
	var gtidMode, enforce string
	if err := db.QueryRowContext(ctx,
		"SELECT @@GLOBAL.gtid_mode, @@GLOBAL.enforce_gtid_consistency").Scan(&gtidMode, &enforce); err != nil {
		zap.L().Debug("[mysql.preflight] could not read gtid settings", zap.Error(err))
	} else if w := gtidPreflightWarning(gtidMode, enforce); w != "" {
		zap.L().Warn("[mysql.preflight] GTID positioning unavailable", zap.String("detail", w))
	}

	var expireSeconds int64
	if err := db.QueryRowContext(ctx,
		"SELECT @@GLOBAL.binlog_expire_logs_seconds").Scan(&expireSeconds); err != nil {
		zap.L().Debug("[mysql.preflight] could not read binlog retention", zap.Error(err))
	} else if w := binlogRetentionWarning(expireSeconds); w != "" {
		zap.L().Warn("[mysql.preflight] short binlog retention", zap.String("detail", w))
	}
}

// gtidPreflightWarning returns an advisory message when the source's GTID
// settings disable failover-safe positioning, or "" when they are fully enabled.
// gtid_mode != ON means CDC falls back to binlog file:position; enforce off (it
// can lag ON during a mode transition) means GTIDs may be assigned inconsistently.
func gtidPreflightWarning(gtidMode, enforce string) string {
	if !strings.EqualFold(gtidMode, "ON") {
		return fmt.Sprintf("gtid_mode=%s (not ON): MySQL CDC falls back to binlog file:position, "+
			"which is not failover-safe and reports no lag/caughtUp. Set gtid_mode=ON and "+
			"enforce_gtid_consistency=ON for failover-safe positioning.", gtidMode)
	}
	if !strings.EqualFold(enforce, "ON") {
		return fmt.Sprintf("gtid_mode=ON but enforce_gtid_consistency=%s: GTIDs may be assigned "+
			"inconsistently. Set enforce_gtid_consistency=ON.", enforce)
	}
	return ""
}

// binlogRetentionWarning returns an advisory message when the source's binlog
// retention is short enough to risk the no-hold caveat, or "" when it is safe.
// expireSeconds <= 0 means binlogs are never auto-purged (no retention limit —
// the safest setting); a positive value below minSafeBinlogRetention warns.
func binlogRetentionWarning(expireSeconds int64) string {
	if expireSeconds <= 0 {
		return ""
	}
	if retention := time.Duration(expireSeconds) * time.Second; retention < minSafeBinlogRetention {
		return fmt.Sprintf("binlog_expire_logs_seconds=%d (%s): committed holds no binlog on the "+
			"source, so if it is down longer than this the source purges unconsumed transactions "+
			"and a re-snapshot is required. Raise retention to cover your worst-case downtime.",
			expireSeconds, retention)
	}
	return ""
}

// binlogFile is one row of the source's binlog inventory (SHOW BINARY LOGS).
type binlogFile struct {
	name string
	size uint64
	// encrypted is the Encrypted column (8.0.14+), when present. An encrypted
	// file's on-disk size includes the per-file encryption header, so it no
	// longer equals the logical stream offsets checkpoints are in — byte
	// arithmetic over it would be a known-wrong number (see binlogBytesBehind).
	encrypted bool
}

// readSourceBinlogInventory lists the source's binlog files with sizes under
// the status timeout. SHOW BINARY LOGS is used (not SHOW MASTER STATUS /
// SHOW BINARY LOG STATUS) because it is stable across the 8.4 rename AND
// carries per-file sizes, which the bytes-behind computation needs anyway —
// the last file's size IS the write head. The result column count varies by
// version (8.0.14 added Encrypted), so rows scan positionally on the first
// two columns. Read-only; one short-lived connection.
func readSourceBinlogInventory(ctx context.Context, config *sql.Config) ([]binlogFile, error) {
	cctx, cancel := context.WithTimeout(ctx, statusLagTimeout)
	defer cancel()

	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(cctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("show binary logs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("binlog inventory columns: %w", err)
	}
	encryptedCol := -1
	for i, c := range cols {
		if strings.EqualFold(c, "Encrypted") {
			encryptedCol = i
		}
	}
	var files []binlogFile
	for rows.Next() {
		var name string
		var size uint64
		var encrypted gosql.NullString
		dest := make([]any, len(cols))
		dest[0], dest[1] = &name, &size
		for i := 2; i < len(cols); i++ {
			if i == encryptedCol {
				dest[i] = &encrypted
			} else {
				dest[i] = new(gosql.RawBytes)
			}
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan binlog inventory: %w", err)
		}
		files = append(files, binlogFile{
			name:      name,
			size:      size,
			encrypted: strings.EqualFold(encrypted.String, "Yes"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("binlog inventory rows: %w", err)
	}
	return files, nil
}

// binlogBytesBehind computes how many binlog bytes lie between a consumed
// file:pos coordinate and the source's write head, from the binlog
// inventory: the consumed file's remaining bytes plus every later file in
// full (the final file's size is the current write offset — the head). It is
// pure so the arithmetic is unit-testable without a server.
//
// purged reports the consumed file missing from the inventory (with the
// inventory non-empty and parsable): the source discarded binlogs past our
// position, streaming can never close that gap, and the caller surfaces the
// re-snapshot state. ok is false when the computation cannot be trusted —
// an empty inventory or a file name without the numeric suffix ordering
// relies on. The subtraction clamps at zero: the checkpoint can be
// momentarily ahead of a stale inventory read (a commit between the two
// queries), and a negative lag is noise, not information.
func binlogBytesBehind(files []binlogFile, consumedFile string, consumedPos uint64) (bytesBehind uint64, purged, ok bool) {
	consumedNum, numOK := binlogFileNum(consumedFile)
	if !numOK || len(files) == 0 {
		return 0, false, false
	}
	var total uint64
	found := false
	for _, f := range files {
		n, fOK := binlogFileNum(f.name)
		if !fOK {
			return 0, false, false
		}
		if n < consumedNum {
			continue
		}
		// An encrypted file in the measured range poisons the arithmetic: its
		// on-disk size includes the encryption header, so it no longer equals
		// the logical offsets the checkpoint is in — the sum would be a small
		// constant lie that keeps caughtUp false forever. A nil lag is honest;
		// a known-wrong number is not. (Encrypted files BELOW the consumed
		// coordinate never enter the sum and don't matter.)
		if f.encrypted {
			return 0, false, false
		}
		if n == consumedNum {
			found = true
		}
		total += f.size
	}
	if !found {
		return 0, true, false
	}
	if total <= consumedPos {
		return 0, false, true
	}
	return total - consumedPos, false, true
}

// statusLagTimeout bounds the source query Status makes for @@gtid_executed /
// @@gtid_purged. Status is a read endpoint, so it must not hang on an
// unreachable source — a failed/late query simply leaves Lag nil.
const statusLagTimeout = 5 * time.Second

// Status implements sql.Dialect: it decodes the binlog checkpoint into a
// point-in-time IngestableStatus (phase, per-table snapshot progress, and the
// binlog coordinate as "file:pos"). Once streaming under GTID positioning, it
// diffs the consumed GTID set against the source's @@gtid_executed to report a
// transaction-count Lag and CaughtUp, and against @@gtid_purged to flag the
// re-snapshot hole. Without a consumed GTID set (gtid_mode=OFF / a legacy
// file:pos checkpoint) it falls back to a BYTES-behind Lag computed from the
// source's binlog inventory — LagUnit says which scale a caller got. Only when
// neither computation can run (source unreachable, unparsable inventory) does
// Lag stay nil with CaughtUp false — an unknown lag is not a caught-up lag.
func (m *MySQLDialect) Status(ctx context.Context, config *sql.Config, pos cluster.Position) (cluster.IngestableStatus, error) {
	config.EnsureTopics()
	// An EMPTY position means nothing has ever durably checkpointed — phase
	// "pending", every table incomplete. It must not fall through to the
	// progress==nil arm below, which renders the completed-streaming state
	// (see sql.PendingStatus for the false-green incident this prevents). No
	// source query: the binlog reader may not have run yet.
	if len(pos) == 0 {
		return sql.PendingStatus(config), nil
	}
	var progress *dialectpb.SnapshotProgress
	var position, consumedGTID string
	var consumedFile string
	var consumedPos uint64
	if len(pos) > 0 {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return cluster.IngestableStatus{}, fmt.Errorf("[mysql.status] decode position: %w", err)
		}
		progress = posProto.SnapshotProgress
		consumedGTID = posProto.GtidSet
		if posProto.Name != "" {
			position = fmt.Sprintf("%s:%d", posProto.Name, posProto.Pos)
			consumedFile, consumedPos = posProto.Name, uint64(posProto.Pos)
		}
	}

	status := cluster.IngestableStatus{
		Position:         position,
		SnapshotProgress: sql.SnapshotTableStatus(config, progress),
	}
	if progress != nil {
		status.Phase = "snapshot"
		return status, nil
	}
	status.Phase = "streaming"

	// No consumed GTID set → no transaction head to diff against. Fall back
	// to BYTES behind the binlog write head, computed from the source's
	// binlog inventory (SHOW BINARY LOGS — version-stable where SHOW MASTER
	// STATUS was removed in 8.4): every file at-or-after the consumed one,
	// minus the consumed offset. Same soft posture as the GTID query: any
	// failure leaves Lag nil (logged at Debug), and a consumed file missing
	// from the inventory means the source purged past our position — the
	// file:pos analog of the gtid_purged hole, surfaced as the same distinct
	// re-snapshot state rather than an understated lag.
	if consumedGTID == "" {
		if consumedFile == "" {
			return status, nil
		}
		files, err := readSourceBinlogInventory(ctx, config)
		if err != nil {
			zap.L().Debug("[mysql.status] binlog inventory query failed", zap.Error(err))
			return status, nil
		}
		bytesBehind, purged, ok := binlogBytesBehind(files, consumedFile, consumedPos)
		if purged {
			status.ReSnapshotRequired = true
			return status, nil
		}
		if ok {
			status.Lag = &bytesBehind
			status.LagUnit = cluster.LagUnitBytes
			status.CaughtUp = bytesBehind == 0
		}
		return status, nil
	}
	consumed, err := mysql.ParseMysqlGTIDSet(consumedGTID)
	if err != nil {
		zap.L().Debug("[mysql.status] parse consumed GTID failed",
			zap.String("gtid_set", consumedGTID), zap.Error(err))
		return status, nil
	}

	// Streaming with GTID positioning: read the source's executed/purged sets. A
	// failure (source unreachable, permissions) leaves Lag nil — the rest of the
	// status is still useful — so it is logged, not returned.
	executed, purged, err := readSourceGTIDState(ctx, config)
	if err != nil {
		zap.L().Debug("[mysql.status] gtid source query failed", zap.Error(err))
		return status, nil
	}

	if needsResnapshot(consumed, purged) {
		// The source purged change data we never consumed: an unrecoverable hole.
		// Surface the distinct re-snapshot state rather than a lag number that
		// would understate a gap streaming can never close.
		status.ReSnapshotRequired = true
		return status, nil
	}

	lag := gtidLag(executed, consumed)
	status.Lag = &lag
	status.LagUnit = cluster.LagUnitTransactions
	status.CaughtUp = consumed.Contain(executed)
	return status, nil
}

// readSourceGTIDState reads the source's @@gtid_executed (the global write head)
// and @@gtid_purged (transactions the source has already dropped) under a short
// timeout. Read-only; one short-lived connection.
func readSourceGTIDState(ctx context.Context, config *sql.Config) (executed, purged mysql.GTIDSet, err error) {
	cctx, cancel := context.WithTimeout(ctx, statusLagTimeout)
	defer cancel()

	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	var executedStr, purgedStr string
	if err := db.QueryRowContext(cctx,
		"SELECT @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged").Scan(&executedStr, &purgedStr); err != nil {
		return nil, nil, fmt.Errorf("query gtid state: %w", err)
	}

	executed, err = mysql.ParseMysqlGTIDSet(executedStr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse gtid_executed %q: %w", executedStr, err)
	}
	purged, err = mysql.ParseMysqlGTIDSet(purgedStr)
	if err != nil {
		return nil, nil, fmt.Errorf("parse gtid_purged %q: %w", purgedStr, err)
	}
	return executed, purged, nil
}

// fetchCharsetPlans reads the source's collation catalog into a collation-id ->
// UTF-8 transcoding plan (see resolveCharsetPlans) over one short-lived
// connection, so the stream can transcode a non-utf8mb4 character column to match
// the snapshot path.
func fetchCharsetPlans(ctx context.Context, config *sql.Config) (map[uint64]charsetPlan, error) {
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	return resolveCharsetPlans(cctx, db)
}

// SourceColumns implements sql.Dialect: it introspects each watched table's
// columns (in ordinal order) so the parser can expand a MapAllColumns config
// into explicit mappings. Read-only; one short-lived connection.
func (m *MySQLDialect) SourceColumns(config *sql.Config) (map[string][]string, map[string][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	out := make(map[string][]string, len(config.Tables))
	generated := make(map[string][]string, len(config.Tables))
	for _, table := range config.Tables {
		cols, gen, err := mysqlTableColumns(ctx, db, table)
		if err != nil {
			return nil, nil, err
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("source table %q has no columns (does it exist?)", table)
		}
		out[table] = cols
		if len(gen) > 0 {
			generated[table] = gen
		}
	}
	return out, generated, nil
}

// mysqlTableColumns returns a table's columns in ordinal order (from the
// connection's current database) plus the subset that are generated/computed
// (information_schema EXTRA carries "VIRTUAL GENERATED" / "STORED GENERATED") —
// committed cannot replicate a generated column (the binlog omits its value), so
// the caller excludes/rejects it.
func mysqlTableColumns(ctx context.Context, db *gosql.DB, table string) (cols, generated []string, err error) {
	schema, name := splitQualifiedTable(table)
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, extra
		FROM information_schema.columns
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
		  AND table_name = ?
		ORDER BY ordinal_position`, schema, name)
	if err != nil {
		return nil, nil, fmt.Errorf("read columns of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var c, extra string
		if err := rows.Scan(&c, &extra); err != nil {
			return nil, nil, err
		}
		cols = append(cols, c)
		if strings.Contains(strings.ToUpper(extra), "GENERATED") {
			generated = append(generated, c)
		}
	}
	return cols, generated, rows.Err()
}

// addedTables returns the configured tables absent from the durable
// snapshotted-tables registry — the tables a re-POST added since the last
// snapshot, whose history needs a backfill. Order follows config.Tables so the
// backfill scan order is deterministic.
func addedTables(configured, snapshotted []string) []string {
	have := make(map[string]bool, len(snapshotted))
	for _, t := range snapshotted {
		have[t] = true
	}
	var added []string
	for _, t := range configured {
		if !have[t] {
			added = append(added, t)
		}
	}
	return added
}

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, epochFloor uint64, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	// Populate Topics before any snapshot/stream goroutine spawns, so per-spec
	// routing (SpecForTable, specFor, refSpecs) never has to lazily mutate the shared
	// config concurrently — a dialect entered directly has only the flat fields.
	config.EnsureTopics()
	backoff := syncerBackoffMin

	// Parse the initial resume position, if any. snapshot_progress
	// being non-nil means a prior run was interrupted mid-snapshot
	// and we should resume from where it left off.
	var lastPos *mysql.Position
	var lastGTID string
	var resumeProgress *dialectpb.SnapshotProgress
	var currentEpoch uint64
	var resumeChunkTag uint64
	var snapshottedTables []string
	// Snapshot the soft-flush budget once, before the streaming goroutine sends
	// anything, so no per-row / per-checkpoint read of the mutable global races a
	// test override (and the budget is stable across this session's reconnects).
	flushBudget := sql.TxnSoftFlushBytes
	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return err
		}
		lastPos = &mysql.Position{Name: posProto.Name, Pos: posProto.Pos}
		lastGTID = posProto.GtidSet
		resumeProgress = posProto.SnapshotProgress
		currentEpoch = posProto.RefreshEpoch
		resumeChunkTag = posProto.ChunkTag
		snapshottedTables = posProto.SnapshottedTables
		if len(snapshottedTables) == 0 {
			// Pre-feature checkpoint: grandfather every currently-configured
			// table as snapshotted. Adds made BEFORE this upgrade are not
			// detected (matching the old behavior); adds made after are.
			snapshottedTables = append([]string(nil), config.Tables...)
		}
	}

	// charsetPlans is the source's collation catalog, read once and reused across
	// reconnects (collation ids are server-immutable). It lets the stream
	// transcode a non-utf8mb4 character column to UTF-8 so CDC matches the
	// snapshot. Fetched lazily inside the loop so a source that is briefly down at
	// start backs off and retries like any other connect step.
	var charsetPlans map[uint64]charsetPlan

	// Outer loop: each iteration either snapshots (first run, or
	// resuming mid-snapshot) or creates a canal, runs it until it
	// exits, then reconnects with backoff. Only ctx cancellation
	// breaks out.
	for {
		// --- snapshot on first run or mid-snapshot resume ---
		// When there is no saved position, perform a pure-SQL initial
		// snapshot to capture existing data and determine the binlog
		// position to stream from. This replaces canal's built-in
		// mysqldump phase, eliminating the external binary dependency.
		// Added-table backfill detection: streaming resume (no in-flight
		// snapshot) where the config names tables the durable registry has
		// never snapshotted — a re-POST added them. Their ongoing binlog
		// changes already stream (the filter is config-driven); their HISTORY
		// must be backfilled or the sink is silently partial. Stage a PARTIAL
		// snapshot: every already-snapshotted sibling is pre-seeded complete
		// (only the added tables scan) and PartialBackfill suppresses the
		// refresh-boundary marker — a topic sweep would delete the sibling
		// rows this backfill does not re-emit. The dialect parity twin of the
		// Postgres addedTables branch. Overlap with the stream is covered by
		// the sink's keyed upsert, exactly as on Postgres.
		if lastPos != nil && resumeProgress == nil {
			if added := addedTables(config.Tables, snapshottedTables); len(added) > 0 {
				zap.L().Info("config change added tables; backfilling their existing rows (a full-table scan per added table; sibling tables are not re-read)",
					zap.Strings("added_tables", added),
					zap.Int("tables_total", len(config.Tables)))
				completed := make([]string, 0, len(config.Tables)-len(added))
				addedSet := map[string]bool{}
				for _, t := range added {
					addedSet[t] = true
				}
				for _, t := range config.Tables {
					if !addedSet[t] {
						completed = append(completed, t)
					}
				}
				resumeProgress = &dialectpb.SnapshotProgress{
					CompletedTables: completed,
					PartialBackfill: true,
				}
				// Stamp backfilled rows at the current epoch, floored so they
				// never land below a generation already on the sink. No bump:
				// this is not a refresh.
				currentEpoch = max(currentEpoch, epochFloor)
			}
		}

		if lastPos == nil || resumeProgress != nil {
			// A mid-snapshot resume keeps the in-progress epoch the checkpoint
			// carries (its closing marker has not committed, so the topic highwater
			// does not yet reflect this refresh) — do NOT bump. A fresh full
			// snapshot uses RefreshSnapshotEpoch: strictly ABOVE the delete-surviving
			// topic highwater on a same-topic recreate (whose cleared position reset
			// the checkpoint epoch to 0 while the sink still holds rows up to the
			// highwater), else epoch 1. A purge re-snapshot arrives here with the
			// epoch already bumped (see the isGtidPurged branch).
			partialBackfill := resumeProgress != nil && resumeProgress.PartialBackfill
			if resumeProgress != nil {
				currentEpoch = max(currentEpoch, 1)
			} else {
				currentEpoch = sql.RefreshSnapshotEpoch(currentEpoch, epochFloor)
			}
			snapshotPos, snapshotGTID, err := snapshot(ctx, config, pr, po, lastPos, lastGTID, resumeProgress, currentEpoch)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				zap.L().Warn("snapshot failed, retrying",
					zap.Duration("backoff", backoff),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > syncerBackoffMax {
					backoff = syncerBackoffMax
				}
				continue
			}
			lastPos = snapshotPos
			lastGTID = snapshotGTID
			resumeProgress = nil
			backoff = syncerBackoffMin
			zap.L().Info("snapshot complete",
				zap.String("binlog_file", lastPos.Name),
				zap.Uint32("binlog_pos", lastPos.Pos),
			)

			// Close a FULL (re-)snapshot with a refresh-boundary marker PER TOPIC
			// at this shared epoch so each keyed sink sweeps its rows left at an
			// older one (a row deleted at the source in the purged window, never
			// re-emitted). A PARTIAL added-table backfill must NOT emit markers:
			// a topic-scoped sweep would delete every sibling row the backfill
			// deliberately did not re-emit. One marker per proposal keeps each
			// proposal homogeneous (one topic), matching the flush path; one spec
			// for the flat form emits exactly one marker as before.
			if !partialBackfill {
				config.EnsureTopics()
				for ti := range config.Topics {
					spec := &config.Topics[ti]
					if spec.Type == nil {
						continue
					}
					marker := cluster.NewRefreshBoundaryEntity(spec.Type, currentEpoch)
					select {
					case pr <- &cluster.Proposal{Entities: []*cluster.Entity{marker}}:
					case <-ctx.Done():
						return nil
					}
				}
			}

			// Any completed snapshot — full or backfill — leaves every
			// configured table snapshotted (table removal is rejected at
			// config-replace time, so config.Tables only ever grows).
			snapshottedTables = append([]string(nil), config.Tables...)

			// Checkpoint the final snapshot position (no
			// snapshot_progress) so a restart after snapshot
			// completion but before the first binlog commit
			// starts streaming instead of re-running snapshot.
			posProto := &dialectpb.MySQLBinLogPosition{Name: lastPos.Name, Pos: lastPos.Pos, GtidSet: snapshotGTID, RefreshEpoch: currentEpoch, ChunkTag: chunkTag(config, flushBudget), SnapshottedTables: snapshottedTables}
			bs, err := proto.Marshal(posProto)
			if err != nil {
				return err
			}
			select {
			case po <- bs:
			case <-ctx.Done():
				return nil
			}
		}

		// --- connect the binlog syncer with retry ---
		// A malformed connection string is fatal (not retryable); a failed
		// StartSync (source down, bad position) backs off and retries, the same
		// posture the canal path had.
		cfg, err := binlogSyncerConfig(config)
		if err != nil {
			return err
		}
		// GTID positioning: when the checkpoint carries a consumed GTID set, resume
		// by it (StartSyncGTID — failover-safe, file-independent); otherwise fall
		// back to file:pos (a legacy checkpoint or a gtid_mode=OFF source).
		var gtidSet mysql.GTIDSet
		if lastGTID != "" {
			gtidSet, err = mysql.ParseMysqlGTIDSet(lastGTID)
			if err != nil {
				return fmt.Errorf("parse resume GTID %q: %w", lastGTID, err)
			}
		}
		var syncer *replication.BinlogSyncer
		var streamer *replication.BinlogStreamer
		useGTID := gtidSet != nil && !gtidSet.IsEmpty()
		for {
			syncer = replication.NewBinlogSyncer(cfg)
			if useGTID {
				// Hand the syncer its OWN clone: BinlogSyncer mutates the GTID set it
				// is given, in place, on its stream goroutine (handleEventAndACK →
				// AddGTID). Passing our gtidSet directly would let that write race the
				// reads committed makes from it below (the handler seed) and on later
				// reconnect iterations.
				streamer, err = syncer.StartSyncGTID(gtidSet.Clone())
			} else {
				streamer, err = syncer.StartSync(*lastPos)
			}
			if err == nil {
				backoff = syncerBackoffMin
				break
			}
			syncer.Close()

			zap.L().Warn("binlog sync start failed, retrying",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > syncerBackoffMax {
				backoff = syncerBackoffMax
			}
		}
		logResumePositioning(useGTID, gtidSet, lastPos)

		// Read the source's collation catalog once (reused across reconnects) so
		// the stream can transcode a non-utf8mb4 character column to UTF-8. Backs
		// off and retries like a failed StartSync — the syncer just connected, so
		// close it before looping to avoid leaking the connection.
		if charsetPlans == nil {
			charsetPlans, err = fetchCharsetPlans(ctx, config)
			if err != nil {
				syncer.Close()
				zap.L().Warn("read source collation catalog failed, retrying",
					zap.Duration("backoff", backoff),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(backoff):
				}
				backoff *= 2
				if backoff > syncerBackoffMax {
					backoff = syncerBackoffMax
				}
				continue
			}
		}

		// --- set up the handler and run the stream ---
		// Every streamed entity carries the current refresh epoch, floored to
		// max(topic highwater, 1): never stamp a streamed row below a generation
		// already on the sink, and cover a pre-feature checkpoint that resumed
		// straight to streaming (snapshot already floors it). Keeping it on the
		// handler means handleXID's checkpoints and the stamped entities agree.
		currentEpoch = max(currentEpoch, epochFloor, 1)
		handler := &MySQLEventHandler{
			config:            config,
			proposalChan:      pr,
			positionChan:      po,
			gtidResume:        useGTID,
			epoch:             currentEpoch,
			charsetPlans:      charsetPlans,
			snapshottedTables: snapshottedTables,
			// Resolve each `tables` entry to a (schema, table) the binlog row filter
			// matches against: a bare entry is scoped to the DSN database (so a
			// same-named table in another database can't contaminate the topic — the
			// binlog is server-wide), a schema-qualified entry keeps its own schema.
			// Lowercased for the case-insensitive match; config.Tables itself stays as
			// written for the snapshot SELECTs, since MySQL table-name case-sensitivity
			// is a server setting we must not override.
			tableRefs: resolveTableRefs(config.Tables, strings.ToLower(dsnDatabase(config.ConnectionString))),
			// Per-topic routing map: same resolution as tableRefs, carrying each
			// table's owning spec so handleRows stamps a row with its own topic.
			refSpecs: resolveRefSpecs(config, strings.ToLower(dsnDatabase(config.ConnectionString))),
			// Seed the live coordinate from the resume position so a
			// mid-transaction flush before the first commit still stamps
			// a sane SourceSeq. lastPos is non-nil here (resume or
			// snapshot-derived).
			curFile: lastPos.Name,
			curPos:  lastPos.Pos,
			// The lineage baseline for failover-regression detection. lastPos is
			// non-nil here; a name without a numeric suffix yields 0 (no baseline
			// → never trips), matching encodeSourceSeq's dedup-disabled fallback.
			resumeFileNum: func() uint64 { n, _ := binlogFileNum(lastPos.Name); return n }(),
			resumePos:     lastPos.Pos,
			// Re-chunk guard: the checkpoint recorded the chunkTag under which its
			// parts were committed. A mismatch now means the budget/config/binary
			// changed and a replayed same-coordinate multi-part could mis-drop a
			// row — flag its soft-flushes. 0 (a pre-feature checkpoint) means no
			// baseline, so never trip.
			chunkingChanged: resumeChunkTag != 0 && resumeChunkTag != chunkTag(config, flushBudget),
			flushBudget:     flushBudget,
		}
		// Seed the consumed GTID set (the same set we resumed by) so streaming
		// checkpoints carry the full set (snapshot ∪ streamed) and it keeps advancing
		// across reconnects. A fresh clone: this is committed's own working copy that
		// handleXID mutates in place, kept separate from both the immutable resume
		// gtidSet and the clone the syncer owns. Empty (file:pos-only / gtid_mode=OFF)
		// leaves consumedGTID nil.
		if gtidSet != nil {
			handler.consumedGTID = gtidSet.Clone()
		}

		// runStream blocks until ctx is canceled (clean exit) or the stream
		// errors (reconnect). Close the syncer either way; on a stream error
		// capture the last committed position so the next iteration resumes.
		streamErr := handler.runStream(ctx, streamer)
		syncer.Close()
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(streamErr, sql.ErrPrimaryKeyColumnMissing) {
			// Permanent schema drift: a configured primaryKey column was renamed or
			// dropped at the source, so every row would collapse onto one key.
			// Reconnecting re-reads the same event and loops forever — return so the
			// worker parks (freeze → supervisor → committed.worker.parked), loud and
			// observable, rather than silently corrupting the sink.
			// streamErr (the ParkError) names the affected topic and the missing
			// columns, so it is the authoritative detail — no separate primary_key
			// field, which would show only the first topic's key on a multi-topic
			// ingestable.
			zap.L().Error("ingest stopping: a primaryKey column is missing from the source schema (renamed or dropped) — worker will park until the ingestable is re-POSTed or the column restored",
				zap.Error(streamErr),
			)
			return streamErr
		}
		if isGtidPurged(streamErr) {
			// The source purged binlogs past our consumed GTID set (error 1236) —
			// resume can't continue. Recover by re-snapshotting from the current
			// source state (the data re-applies idempotently downstream). Loud and
			// explicit, never a silent gap; retrying the same GTID would just loop.
			zap.L().Error("binlog purged past consumed position (error 1236) — re-snapshotting",
				zap.String("consumed_gtid", lastGTID),
				zap.Error(streamErr),
			)
			// Bump the refresh epoch so the recovery re-snapshot enumerates every
			// live row at a NEW generation and its closing refresh-boundary marker
			// sweeps rows a source-side delete removed during the purged window
			// (they keep their older generation and are never re-emitted). The gap
			// is reconciled in-band, not just re-loaded. Floor to the topic highwater
			// so the bump lands strictly above every generation already on the sink.
			currentEpoch = max(currentEpoch, epochFloor, 1) + 1
			lastPos = nil
			lastGTID = ""
			resumeProgress = nil
		} else {
			if handler.lastPos != nil {
				lastPos = handler.lastPos
			}
			if handler.consumedGTID != nil {
				lastGTID = handler.consumedGTID.String()
			}
			zap.L().Warn("binlog stream exited, will reconnect",
				zap.Duration("backoff", backoff),
				zap.Error(streamErr),
			)
		}

		// --- backoff before reconnect ---
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > syncerBackoffMax {
			backoff = syncerBackoffMax
		}
	}
}

func (m *MySQLDialect) Close() error {
	return nil
}

// MySQLEventHandler holds the per-Ingest streaming state and the logic that turns
// binlog events into proposals. Its methods are driven by runStream off a single
// goroutine, so the mutable state below needs no locking and cancellation is
// threaded through each method's ctx rather than stored on the struct.
type MySQLEventHandler struct {
	config       *sql.Config
	proposalChan chan<- *cluster.Proposal
	positionChan chan<- cluster.Position

	// tableRefs are the resolved source tables (schema + table, lowercased) the
	// binlog row filter matches against — see watches / resolveTableRefs. MySQL's
	// binlog is server-wide, so each ref's schema scopes the match to the right
	// database (the DSN's default for a bare `tables` entry, or a schema-qualified
	// entry's own), keeping a same-named table in another database out of the topic.
	tableRefs []tableRef

	// refSpecs pairs each resolved table filter with the topic-spec that routes it,
	// so a streamed row is stamped with its own table's Type / Mappings / PrimaryKey
	// (see specFor). The flat config has one spec, so every ref points to it;
	// [[sql.topics]] gives N. Matching mirrors watches exactly (the same schema
	// scoping); tableRefs stays the fast row-filter gate, refSpecs the post-gate
	// routing lookup.
	refSpecs []tableRefSpec

	// charsetPlans is the source's collation catalog (collation id -> UTF-8
	// transcoding plan), read once at stream start. handleRows hands it to
	// columnsFromTableMap so a non-utf8mb4 character column's CDC bytes are
	// transcoded to UTF-8 and agree with the snapshot path. Nil degrades to
	// passthrough (every column treated as already-UTF-8) — the pre-feature
	// behavior, kept for tests that construct a handler directly.
	charsetPlans map[uint64]charsetPlan

	// snapshottedTables is the durable registry of tables whose history has
	// been snapshotted, carried onto every streaming checkpoint so a later
	// resume can detect config-added tables and backfill them (see
	// addedTables / MySQLBinLogPosition.snapshotted_tables).
	snapshottedTables []string

	// epoch is the reconciling-refresh generation stamped on every streamed
	// entity and written into every streaming checkpoint, so it survives a
	// reconnect and a keyed sink can sweep by it. Seeded from the resume
	// position (or the just-completed snapshot's epoch) by the Ingest loop.
	epoch uint64

	// lastPos holds the most recently committed binlog position so the outer
	// reconnect loop can resume from where it left off.
	lastPos *mysql.Position

	// curFile / curPos track the live binlog coordinate as events stream, so every
	// flushed proposal can be stamped with a strictly-monotonic,
	// resume-deterministic SourceSeq for effectively-once dedup. curFile follows
	// binlog rotation; curPos follows each row's end-of-event offset and the
	// commit offset.
	curFile string
	curPos  uint32

	// flushSub disambiguates several partial flushes that share one binlog
	// coordinate. All rows of one RowsEvent carry that event's end offset, so an
	// event larger than maxPendingEntities soft-flushes twice at the SAME (file,
	// pos) — identical SourceSeqs, and the ingest dedup's <= drop would silently
	// lose every flush after the first. flushSub increments per flush at an
	// unchanged coordinate and resets when the coordinate advances, making the
	// SourceSeq strictly monotonic. It is derived purely from the (deterministic,
	// in-order) event stream, so a resume replays the same coordinates and
	// reproduces the same seqs — cross-reconnect dedup still holds.
	flushFile string
	flushPos  uint32
	flushSub  uint32

	// resumeFileNum / resumePos are the (file number, byte offset) of the position
	// this handler resumed from — the failover-lineage baseline (resumeFileNum is 0
	// when there is no numeric baseline). If the live coordinate ever lands at or
	// below this baseline — a lower file number, OR the same file number at an
	// equal/lower offset — the source's binlog lineage regressed (a failover to a
	// replica whose numbering restarts low or coincidentally shares the file
	// ordinal, or a RESET MASTER), so the coordinate it now emits encodes at or
	// below the durable SourceSeq highwater (SourceSeq = fileNum<<32 | pos). Those
	// proposals are stamped DedupUnsafe so the ingest worker freezes rather than
	// silently dedup-dropping real post-failover data. Comparing the file number
	// alone missed the equal-ordinal/lower-offset case, which is silent LOSS — so
	// the full coordinate is compared. Set once at construction; never advanced (a
	// normal same-lineage stream only ever advances above the baseline, so this
	// never trips falsely).
	resumeFileNum uint64
	resumePos     uint32

	// gtidResume records that this session resumed by the GTID-set watermark
	// (StartSyncGTID). It gates HOW a lineage regression is reported: under
	// GTID resume the regression is stamped LineageRegressed (the
	// transaction-scoped dedup rides through; only a legacy stored record
	// freezes), while a file:pos-only session keeps the DedupUnsafe freeze —
	// coordinates are its ONLY identity, so a regression is undecidable.
	gtidResume bool

	// chunkingChanged is set at construction when the current chunkTag differs
	// from the one recorded in the resume checkpoint — the flush budget, config
	// rendering, or binary rendering version changed since the parts were
	// committed. While true, every soft-flush (a same-coordinate multi-part
	// piece) is flagged DedupUnsafe: re-chunking under changed inputs can move an
	// as-yet-uncommitted row into a part the SourceSeq dedup would drop, so a
	// replay of such a part must freeze rather than risk silent loss. The commit
	// flush needs no flag — a re-chunked replay always trips on an earlier
	// soft-flush at/below the highwater first, and a transaction no longer
	// oversized under the new budget re-applies idempotently to the keyed sink.
	chunkingChanged bool

	// flushBudget is the soft-flush byte budget captured once at Ingest start
	// (a snapshot of the global sql.TxnSoftFlushBytes). The worker reads THIS,
	// not the mutable global, on every row and checkpoint — so a test overriding
	// the budget can't race the streaming goroutine, and the budget stays stable
	// for the whole session.
	flushBudget int

	// pending accumulates entities from row events until the transaction commits
	// (an XID event), so one MySQL transaction maps to one cluster.Proposal.
	// pendingBytes tracks their estimated marshaled size against
	// sql.TxnSoftFlushBytes.
	pending      []*cluster.Entity
	pendingBytes int

	// consumedGTID is the set of transactions fully processed and checkpointed —
	// the GTID resume cursor (Phase B). curTxnGTID holds the in-flight
	// transaction's GTID between its GTIDEvent and its commit, when it is merged
	// into consumedGTID. Both stay nil until the first GTIDEvent, so a file:pos-only
	// or gtid_mode=OFF source leaves the checkpoint GTID-free.
	consumedGTID mysql.GTIDSet
	curTxnGTID   mysql.GTIDSet

	// curTxnSourceTS and curTxnID are the in-flight transaction's capture
	// provenance, stamped onto every proposal flushed for it (see
	// cluster.Proposal.SourceCommitUnixNano/SourceTxnID) and cleared at
	// commit. curTxnSourceTS is the latest binlog event-header timestamp
	// (whole seconds — the header's resolution) observed for the
	// transaction: at the commit flush that is the XID event's header, i.e.
	// the source commit time; a mid-transaction soft flush carries its
	// latest RowsEvent header (statement time, at most seconds earlier).
	// curTxnID is the transaction's GTID when the source runs gtid_mode=ON,
	// else the binlog coordinate of the transaction's first buffered row —
	// captured ONCE so every part of a multi-part (soft-flushed)
	// transaction shares one identity (the co-transaction evidence).
	curTxnSourceTS uint32
	curTxnID       string

	// driftWarned dedups the DIVERGENCE-tier warning (a mapped non-key column
	// dropped/renamed at the source): the check runs per RowsEvent — i.e. per
	// transaction touching a watched table — so a bare warn would spam. Keyed by
	// "table\x00column", it fires once per drifted column per session (a fresh
	// handler is built on every reconnect, so a still-present drift re-warns once
	// after a reconnect, which is the right cadence). The CORRUPTION tier needs no
	// dedup — it parks on the first occurrence.
	driftWarned map[string]bool
}

// mysqlCategoryForTypeName maps a MySQL type name (information_schema data_type
// for the binlog decode path; database/sql DatabaseTypeName for the snapshot
// path) to a JSON category. MySQL has no native bool — a tinyint(1) is a number —
// so there is no bool category.
func mysqlCategoryForTypeName(name string) sql.JSONCategory {
	switch strings.ToUpper(name) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR",
		"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC",
		// BIT is an integer: the binlog path decodes it to an int64 (rendered as a
		// JSON number), so the snapshot must classify it numeric too — readBatch
		// converts the driver's raw BIT bytes to that same int64 (see bitToInt64)
		// so both paths emit an identical number instead of the snapshot's byte
		// string ("A") diverging from CDC's number (65).
		"BIT":
		return sql.CatNumber
	case "JSON":
		return sql.CatJSON
	// Binary columns render as base64 (see sql.JSONValue): the driver and the
	// binlog both hand these back as raw []byte, and a bytes→string coercion
	// would silently U+FFFD any non-UTF-8 content. TEXT/CHAR/VARCHAR stay text
	// (transcoded to UTF-8); only the no-charset BLOB/BINARY family is binary.
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY":
		return sql.CatBinary
	}
	return sql.CatText
}

// bitToInt64 converts a BIT column's bytes — as database/sql returns them, the
// value's bytes most-significant first — into the same int64 the binlog path
// produces (go-mysql accumulates BIT bytes big-endian into an int64). This keeps
// a BIT column's snapshot render byte-identical to its CDC render, both as a JSON
// number and in the composite key. BIT is at most 64 bits (8 bytes).
func bitToInt64(b []byte) int64 {
	var u uint64
	for _, x := range b {
		u = u<<8 | uint64(x)
	}
	return int64(u)
}

// stripZeroTimeFraction removes an all-zero fractional part from a MySQL TIME
// value ("13:45:30.000000" → "13:45:30") so the snapshot matches the CDC render:
// go-mysql's TIME2 decoder drops the fraction entirely when it is zero, while the
// driver's snapshot text pads it to the column's declared precision. A non-zero
// fraction is left intact (both paths keep it).
func stripZeroTimeFraction(s string) string {
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return s
	}
	for _, c := range s[dot+1:] {
		if c != '0' {
			return s // a real fraction — keep it
		}
	}
	return s[:dot]
}

// decodeEnumSet resolves the numeric binlog encoding of ENUM and SET columns to
// the label text the snapshot path (database/sql) also produces, so both ingest
// paths render the same JSON for the same value. The binlog hands back an ENUM as
// its 1-based ordinal index and a SET as a bitmask of member positions; without
// this they would leak into the payload as bare numbers (e.g. 'green' → 2), which
// is both unreadable and fragile (reordering members rewrites the meaning of
// historical numbers). A column that is neither passes through untouched.
func decodeEnumSet(ci columnInfo, val any) any {
	if val == nil {
		return nil
	}
	switch {
	case len(ci.enumValues) > 0:
		idx, ok := asInt64(val)
		if !ok {
			return val // already a label (string/[]byte) or an unexpected form
		}
		if idx <= 0 || int(idx) > len(ci.enumValues) {
			return "" // 0 is MySQL's invalid/empty-enum sentinel; out-of-range guarded
		}
		return ci.enumValues[idx-1]
	case len(ci.setValues) > 0:
		bits, ok := asInt64(val)
		if !ok {
			return val
		}
		parts := make([]string, 0, len(ci.setValues))
		for i, name := range ci.setValues {
			if bits&(int64(1)<<uint(i)) != 0 {
				parts = append(parts, name)
			}
		}
		return strings.Join(parts, ",")
	}
	return val
}

// asInt64 widens canal's integer encoding of an ENUM index / SET bitmask to
// int64 — canal decodes both as a signed integer (a SET's high member bit lands
// in the sign bit, which the bitmask AND in decodeEnumSet handles correctly).
// Returns false for non-integer values, so a caller falls back to passing the
// value through.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int8:
		return int64(n), true
	default:
		return 0, false
	}
}

// handleRows decodes one row event and buffers the resulting entity until the
// transaction commits. It is the BinlogSyncer successor to canal's OnRow: the row
// image, the operation (rowsAction), and the table name come straight from the
// raw replication.RowsEvent, and the column metadata from committed's own schema
// cache (joined to the positional row image by ordinal).
// tableRef is a resolved source table: its database schema and table name, both
// lowercased for case-insensitive binlog matching. A bare `tables` entry resolves
// its schema to the connection's database; a schema-qualified entry
// ("schema.table") carries its own. schema == "" means the connection named no
// database and the entry was bare — matched on table name alone (any schema).
type tableRef struct {
	schema string
	table  string
}

// splitQualifiedTable splits a config `tables` entry into an optional schema
// qualifier and the table name, preserving case for use in queries. A bare entry
// returns an empty schema. It splits on the first dot; a table name legitimately
// containing a dot is not supported (and never was — the same shape that made
// schema qualification ambiguous).
func splitQualifiedTable(entry string) (schema, table string) {
	if s, t, ok := strings.Cut(entry, "."); ok {
		return s, t
	}
	return "", entry
}

// resolveTableRefs resolves each config `tables` entry to a lowercased tableRef
// for the binlog row filter (watches). A bare entry takes dbSchema (the
// connection's database, already lowercased) as its schema; a schema-qualified
// entry keeps its own. When dbSchema is empty (the DSN named no database) a bare
// entry's schema stays empty, matching any schema by table name alone.
func resolveTableRefs(tables []string, dbSchema string) []tableRef {
	refs := make([]tableRef, 0, len(tables))
	for _, entry := range tables {
		schema, table := splitQualifiedTable(entry)
		if schema == "" {
			schema = dbSchema
		}
		refs = append(refs, tableRef{schema: strings.ToLower(schema), table: strings.ToLower(table)})
	}
	return refs
}

// tableRefSpec pairs a resolved binlog table filter with the topic-spec that owns
// that table, so a matched row is routed to its own topic (see specFor).
type tableRefSpec struct {
	ref  tableRef
	spec *sql.TopicSpec
}

// resolveRefSpecs resolves every spec's tables to (schema, table) filters paired
// with the owning spec, mirroring resolveTableRefs (bare entries scoped to the DSN
// database). Cross-spec table-routing conflicts are rejected at parse time, so a
// given table appears under exactly one spec.
func resolveRefSpecs(config *sql.Config, dbSchema string) []tableRefSpec {
	config.EnsureTopics()
	var out []tableRefSpec
	for i := range config.Topics {
		spec := &config.Topics[i]
		for _, ref := range resolveTableRefs(spec.Tables, dbSchema) {
			out = append(out, tableRefSpec{ref: ref, spec: spec})
		}
	}
	return out
}

// specFor returns the topic-spec that routes binlog rows for schema.table, or nil
// if none does. It applies the same match as watches (case-insensitive table name;
// a ref's own schema scopes the match, or any schema when the ref carries none), so
// a table watches() admits always resolves to its spec.
func (h *MySQLEventHandler) specFor(schema, table string) *sql.TopicSpec {
	s, t := strings.ToLower(schema), strings.ToLower(table)
	for i := range h.refSpecs {
		ref := h.refSpecs[i].ref
		if ref.table == t && (ref.schema == "" || ref.schema == s) {
			return h.refSpecs[i].spec
		}
	}
	return nil
}

// watches reports whether a binlog row event for schema.table should be ingested:
// the event must match one of the resolved tableRefs by table name (case-
// insensitively) AND by schema — either the ref's own schema (a schema-qualified
// `tables` entry, or a bare entry scoped to the connection's database) or, when the
// ref carries no schema (a bare entry with a DSN that named no database), any
// schema.
//
// The schema scope matters because MySQL's binlog is server-wide — it carries
// events for every database on the server — so without it a bare table-name match
// would ingest otherdb.users into a topic configured for appdb.users (silent
// cross-database/tenant contamination). A schema-qualified entry, conversely, lets
// one ingestable read a table outside the DSN's default database.
func (h *MySQLEventHandler) watches(schema, table string) bool {
	s, t := strings.ToLower(schema), strings.ToLower(table)
	for _, ref := range h.tableRefs {
		if ref.table == t && (ref.schema == "" || ref.schema == s) {
			return true
		}
	}
	return false
}

func (h *MySQLEventHandler) handleRows(ctx context.Context, header *replication.EventHeader, e *replication.RowsEvent) error {
	table := string(e.Table.Table)
	schema := string(e.Table.Schema)
	if !h.watches(schema, table) {
		return nil
	}
	action, ok := rowsAction(e.Type())
	if !ok {
		return nil // an unsupported rows-event variant — skip
	}

	// Route this table's rows to their own topic: specFor uses the same match as
	// watches, so a watched table always resolves. A nil is a gate/routing-map
	// disagreement (a bug) — skip rather than stamp a nil Type onto every row.
	spec := h.specFor(schema, table)
	if spec == nil {
		zap.L().Error("handleRows: no topic-spec routes a watched table; skipping event",
			zap.String("schema", schema), zap.String("table", table))
		return nil
	}

	// Decode against the schema carried by THIS row's binlog TableMapEvent — the
	// schema as of the write — not live information_schema, which reflects the
	// post-ALTER columns and would silently mis-join a still-replaying old image.
	ts, err := columnsFromTableMap(e.Table, h.charsetPlans)
	if err != nil {
		return fmt.Errorf("decode schema of %q from binlog: %w", table, err)
	}

	// Reconcile the config's column contract against the schema THIS row was
	// written under. A vanished primaryKey column is corruption — every row would
	// collapse onto "<nil>" — so park (the reconnect loop returns the sentinel
	// rather than busy-retry); a vanished mapped non-key column is divergence — the
	// field renders null but rows stay correctly keyed — so warn once and continue.
	observed := make(map[string]bool, len(ts.cols))
	for _, c := range ts.cols {
		observed[c.name] = true // ts.cols names are already lowercased
	}
	drift := sql.ReconcileSchema(spec, observed)
	if err := drift.ParkError(); err != nil {
		return err
	}
	h.warnSchemaDivergence(table, drift.MissingMapped)

	// Track the live offset (the row event's end position) so a mid-transaction
	// soft-limit flush stamps a monotonic SourceSeq. It applies to every row of
	// the event. The header timestamp feeds the transaction's provenance stamp
	// (a zero timestamp is a fake/heartbeat event — never one carrying rows,
	// but guarded anyway so it can't blank a real observation).
	if header != nil {
		h.curPos = header.LogPos
		if header.Timestamp != 0 {
			h.curTxnSourceTS = header.Timestamp
		}
	}

	h.captureTxnID()

	// A RowsEvent carries EVERY row of its statement, not just one: a multi-row
	// INSERT or a bulk DELETE has one image per row, and an UPDATE interleaves
	// [before, after, before, after, ...]. Emit an entity per affected row — the
	// after-image for an UPDATE (odd indices), every image otherwise — so batch
	// and bulk DML don't silently lose all rows but the last.
	start, step := 0, 1
	if action == "update" {
		start, step = 1, 2 // after-images only
	}
	for ri := start; ri < len(e.Rows); ri += step {
		entity, ok := h.rowEntity(ts, spec, table, action, e.Rows[ri])
		if !ok {
			continue // unmarshalable row, already logged — skip it
		}
		// Buffer until the transaction commits.
		h.pending = append(h.pending, entity)
		h.pendingBytes += sql.EntityFlushBytes(entity)

		// A PK-changing UPDATE must tombstone the old key too, or the old-key row
		// lingers downstream forever — two rows for one source row (divergence +
		// an un-deletable stale record, RTBF concern). The before-image sits at
		// ri-1 (the UPDATE stride is [before, after]); when its key differs from
		// the after-image key, emit a delete for it, mirroring the Postgres path.
		// binlog_row_image=FULL (Preflight-required) guarantees the before-image
		// carries the primary key.
		if action == "update" {
			if old, ok := h.rowEntity(ts, spec, table, "delete", e.Rows[ri-1]); ok && !bytes.Equal(old.Key, entity.Key) {
				h.pending = append(h.pending, old)
				h.pendingBytes += sql.EntityFlushBytes(old)
			}
		}

		// Byte-bounded soft flush: a transaction that cannot fit one proposal
		// is applied as ordered parts (the documented bounded exception — see
		// sql.TxnSoftFlushBytes).
		if h.pendingBytes >= h.flushBudget {
			if _, err := h.flushPending(ctx, true, nil); err != nil {
				return err
			}
		}
	}

	zap.L().Debug("handleRows", zap.String("table", table), zap.String("action", action), zap.Int("rows", len(e.Rows)))
	return nil
}

// warnSchemaDivergence emits the divergence-tier warning once per (table, column)
// per session for each mapped non-key column a source rename/drop removed: the
// sink now renders it null and diverges from the source until re-snapshotted,
// mirroring handleDDL's TRUNCATE-on-a-watched-table policy. Deduped because the
// caller runs per RowsEvent (see driftWarned).
func (h *MySQLEventHandler) warnSchemaDivergence(table string, missingMapped []string) {
	if len(missingMapped) == 0 {
		return
	}
	if h.driftWarned == nil {
		h.driftWarned = make(map[string]bool)
	}
	for _, col := range missingMapped {
		k := table + "\x00" + col
		if h.driftWarned[k] {
			continue
		}
		h.driftWarned[k] = true
		zap.L().Warn("mapped column dropped or renamed at the source; it now renders null on the sink, which diverges from the source and must be re-snapshotted to reconcile",
			zap.String("table", table),
			zap.String("column", col),
		)
	}
}

// rowEntity decodes one row image (positional values joined to the schema by
// ordinal) into an upsert or a delete tombstone keyed by the row's primary key.
// ok is false — the row is to be skipped, having already been logged — when its
// mapped data can't be marshaled to JSON.
func (h *MySQLEventHandler) rowEntity(ts *tableSchema, spec *sql.TopicSpec, table, action string, row []any) (*cluster.Entity, bool) {
	// m holds the key form: coerce []byte (TEXT/BLOB/JSON) to string so the key is
	// text and matches the snapshot path. raw/catByName keep the value and its
	// category so the payload renders as natural JSON.
	m := make(map[string]any)
	raw := make(map[string]any)
	catByName := make(map[string]sql.JSONCategory)
	for i, col := range ts.cols {
		if i >= len(row) {
			break // row image has fewer columns than the current schema (DDL skew)
		}
		// Transcode a non-utf8mb4 character column's raw binlog bytes to UTF-8
		// BEFORE they feed either the key or the payload, so both match the
		// snapshot path (which reads UTF-8 via the driver's utf8mb4 session). Only
		// character columns carry a transcoder; ENUM/SET labels were transcoded at
		// schema time and BLOB/binary stay passthrough. go-mysql hands a
		// VARCHAR/CHAR back as a string and a TEXT/BLOB as []byte, so both forms are
		// transcoded and the original form preserved; a NULL (nil) is left alone. A
		// byte sequence the decoder rejects skips the row (logged) rather than
		// emitting corrupt data.
		cell := row[i]
		if col.transcode != nil {
			transcoded, ok, err := transcodeCell(col.transcode, cell)
			if err != nil {
				zap.L().Warn("handleRows: skipping row with untranscodable text",
					zap.String("table", table),
					zap.String("column", col.name),
					zap.Error(err),
				)
				return nil, false
			}
			if ok {
				cell = transcoded
			}
		}
		val := decodeEnumSet(col, cell)
		raw[col.name] = val
		catByName[col.name] = col.cat
		if b, ok := val.([]byte); ok {
			m[col.name] = string(b)
		} else {
			m[col.name] = val
		}
	}

	key := sql.CompositeKey(m, spec.PrimaryKey)

	// A source DELETE becomes a delete (tombstone) entity keyed by the row's
	// primary key; INSERT/UPDATE upsert the (after-)image.
	if action == "delete" {
		return cluster.NewDeleteEntity(spec.Type, []byte(key)), true
	}

	toJSON := sql.BuildEntityJSON(spec.Mappings, raw, catByName)
	jsonString, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("handleRows: skipping row with unmarshalable data",
			zap.String("table", table),
			zap.Error(err),
		)
		return nil, false
	}
	return &cluster.Entity{
		Type: spec.Type,
		Key:  []byte(key),
		Data: []byte(jsonString),
	}, true
}

// flushPending emits the buffered entities and resets the buffer. No-op when the
// buffer is empty. A mixed-table transaction is split into one HOMOGENEOUS proposal
// per topic (one group for the flat single-topic form); the db/WAL layer routes by
// topic and dedups on a per-ingestable SourceSeq, so each per-topic proposal at this
// coordinate must get a strictly-greater SourceSeq or the ingest dedup would drop
// every group after the first. isSoftFlush is true for a mid-transaction byte-budget
// flush (a same-coordinate multi-part piece) and false for the commit flush — it
// drives the re-chunk DedupUnsafe guard.
// bundlePos, when non-nil, is the encoded post-transaction resume position
// (the GTID-set watermark): it is attached to the LAST emitted group so the
// durable watermark commits atomically with the transaction's final part —
// only the commit flush passes it (a position must never advance past an
// uncommitted transaction). Returns whether any proposal was emitted, so
// handleXID can fall back to the out-of-band position checkpoint for
// commits that flushed nothing (an unwatched-table transaction's XID).
func (h *MySQLEventHandler) flushPending(ctx context.Context, isSoftFlush bool, bundlePos []byte) (bool, error) {
	if len(h.pending) == 0 {
		return false, nil
	}
	// Stamp streamed rows with the current refresh epoch so a later
	// refresh-boundary marker can sweep rows a re-snapshot left behind (a
	// streamed-then-gap-deleted row still carries this epoch until the sweep).
	stampGeneration(h.pending, h.epoch)

	// One proposal per topic, in first-appearance order (deterministic for replay).
	// A single-topic flush yields one group == the original buffer, so its proposal
	// is byte-identical to the pre-routing single-proposal flush.
	groups := sql.PartitionByTopic(h.pending)
	h.pending = nil
	h.pendingBytes = 0

	for i, group := range groups {
		// Disambiguate repeated proposals at one coordinate: a RowsEvent bigger than
		// maxPendingEntities flushes several times at the same (file, pos), and a
		// mixed-table flush emits several per-topic proposals at one (file, pos).
		// Increment a sub-index per emitted proposal (reset when the coordinate
		// advances) so each proposal's SourceSeq is strictly greater than the last and
		// none is dropped by the ingest dedup.
		if h.curFile == h.flushFile && h.curPos == h.flushPos {
			h.flushSub++
		} else {
			h.flushFile, h.flushPos, h.flushSub = h.curFile, h.curPos, 0
		}
		p := &cluster.Proposal{
			Entities:  group,
			SourceSeq: encodeSourceSeq(h.curFile, h.curPos, h.flushSub),
			// Capture provenance: the source commit time (event-header seconds)
			// and transaction identity tracked for the in-flight transaction.
			// Every per-topic group of one transaction shares them.
			SourceCommitUnixNano: int64(h.curTxnSourceTS) * int64(time.Second),
			SourceTxnID:          h.curTxnID,
			// This dialect checkpoints per transaction (handleXID bundles the
			// post-transaction position), so its resume re-emits at most the
			// in-flight transaction — the contract that earns txn-scoped dedup.
			TxnScopedDedup: h.curTxnID != "",
		}
		// Failover-lineage guard: if the live coordinate is at or below the resume
		// baseline — a lower file number, OR the same file number at an equal/lower
		// byte offset — this stream is a different binlog lineage (a promoted replica
		// / RESET MASTER) whose coordinate encodes at or below coordinates the old
		// lineage already used. Under a GTID resume this is EXPECTED after a
		// promotion and harmless to the transaction-scoped dedup — stamp
		// LineageRegressed so the worker rides through (freezing only while the
		// stored dedup record is still the legacy scalar regime). A file:pos-only
		// session has no transaction watermark, so the regression stays a
		// DedupUnsafe freeze — coordinates are its only identity. A normal
		// same-lineage resume only ever streams ABOVE the baseline, so neither
		// trips falsely. Comparing the file number alone missed the
		// equal-ordinal/lower-offset case (silent loss).
		if h.resumeFileNum > 0 {
			if n, ok := binlogFileNum(h.curFile); ok &&
				(n < h.resumeFileNum || (n == h.resumeFileNum && h.curPos <= h.resumePos)) {
				if h.gtidResume {
					p.LineageRegressed = true
				} else {
					p.DedupUnsafe = true
				}
			}
		}
		// Re-chunk guard (F1): the chunking inputs changed since this coordinate's
		// parts were committed, so a replayed soft-flush may carry rows re-grouped
		// into a part the dedup would drop. Freeze rather than risk silent loss. Only
		// soft-flushes need it — see h.chunkingChanged.
		if h.chunkingChanged && isSoftFlush {
			p.DedupUnsafe = true
		}

		if bundlePos != nil && i == len(groups)-1 {
			p.Position = bundlePos
		}

		select {
		case h.proposalChan <- p:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return true, nil
}

// captureTxnID records the in-flight transaction's provenance identity at its
// first buffered row: the GTID when the source announces one (gtid_mode=ON —
// the GTIDEvent precedes the transaction's row events), else the first row
// event's binlog coordinate. Captured ONCE per transaction so every
// soft-flushed part shares one identity; cleared on commit (handleXID).
func (h *MySQLEventHandler) captureTxnID() {
	if h.curTxnID != "" {
		return
	}
	if h.curTxnGTID != nil {
		h.curTxnID = h.curTxnGTID.String()
		return
	}
	h.curTxnID = fmt.Sprintf("%s:%d", h.curFile, h.curPos)
}

// stampGeneration sets the reconciling-refresh epoch on every entity in a batch
// (see cluster.Entity.Generation). The whole batch shares one epoch: the worker
// holds a single "current epoch" that only changes when a binlog purge forces an
// in-place re-snapshot, so stamping at flush time is enough.
func stampGeneration(entities []*cluster.Entity, epoch uint64) {
	for _, e := range entities {
		e.Generation = epoch
	}
}

// mergeGTID extends the consumed GTID set with one committed transaction's GTID.
// A nil consumed starts from a clone of txn; a nil txn (file:pos-only /
// gtid_mode=OFF source, which emits no GTIDEvent) leaves consumed unchanged.
func mergeGTID(consumed, txn mysql.GTIDSet) (mysql.GTIDSet, error) {
	if txn == nil {
		return consumed, nil
	}
	if consumed == nil {
		return txn.Clone(), nil
	}
	if err := consumed.Update(txn.String()); err != nil {
		return nil, err
	}
	return consumed, nil
}

// isGtidPurged reports whether err is MySQL error 1236
// (ER_MASTER_FATAL_ERROR_READING_BINLOG) — the source has purged binlogs past our
// consumed GTID set, so resume can't continue and a re-snapshot is required.
// go-mysql surfaces it asynchronously on GetEvent as a *mysql.MyError, so the
// caller checks the stream error here rather than the StartSyncGTID return.
func isGtidPurged(err error) bool {
	var myErr *mysql.MyError
	return errors.As(err, &myErr) && myErr.Code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG
}

// needsResnapshot reports whether the source has purged change data this ingest
// never consumed — a GTID in purged that consumed does not contain is gone from
// the source and can never be streamed, so the only honest recovery is a fresh
// snapshot. Empty purged (nothing dropped) is never a hole; a nil consumed with
// a non-empty purged is a hole (we have consumed nothing the source already
// dropped). This is the steady-state, queryable complement to the runtime
// error-1236 detection (isGtidPurged).
func needsResnapshot(consumed, purged mysql.GTIDSet) bool {
	if purged == nil || purged.IsEmpty() {
		return false
	}
	return consumed == nil || !consumed.Contain(purged)
}

// gtidLag returns how many transactions the source has executed but this ingest
// has not yet consumed — the cardinality of (executed − consumed). It is exact
// interval arithmetic over the GTID sets, so an errant GTID in consumed that the
// source never executed cannot drive the count negative; only executed-but-
// unconsumed transactions count. A nil/empty executed yields 0.
func gtidLag(executed, consumed mysql.GTIDSet) uint64 {
	em, ok := executed.(*mysql.MysqlGTIDSet)
	if !ok || em == nil {
		return 0
	}
	cm, _ := consumed.(*mysql.MysqlGTIDSet)

	var lag uint64
	for sid := range *em {
		for tag, have := range (*em)[sid] {
			var covered mysql.IntervalSlice
			if cm != nil {
				if tags, ok := (*cm)[sid]; ok {
					covered = tags[tag]
				}
			}
			lag += countMissingGTIDs(have, covered)
		}
	}
	return lag
}

// countMissingGTIDs counts the integers in have's half-open [Start,Stop)
// intervals that no interval in covered includes — the size of (have − covered)
// for one (server-uuid, tag). Both slices are normalized (sorted, merged) so the
// single forward sweep over covered is correct.
func countMissingGTIDs(have, covered mysql.IntervalSlice) uint64 {
	have = have.Normalize()
	covered = covered.Normalize()

	var missing uint64
	for _, iv := range have {
		cur := iv.Start
		for _, cv := range covered {
			if cv.Stop <= cur {
				continue // covered region is entirely before the uncounted remainder
			}
			if cv.Start >= iv.Stop {
				break // covered is sorted; nothing further overlaps iv
			}
			if cv.Start > cur {
				missing += nonNeg(cv.Start - cur) // gap before this covered region
			}
			if cv.Stop > cur {
				cur = cv.Stop // skip past the covered region
			}
			if cur >= iv.Stop {
				break
			}
		}
		missing += nonNeg(iv.Stop - cur)
	}
	return missing
}

// nonNeg converts a guaranteed-non-negative int64 difference to uint64, clamping
// any (unexpected) negative to 0 so the conversion is always well-defined.
func nonNeg(n int64) uint64 {
	if n < 0 {
		return 0
	}
	return uint64(n) //nolint:gosec // G115: clamped non-negative immediately above
}

// handleXID handles a transaction commit (XID event): it stamps the post-commit
// coordinate, flushes the buffered entities as one proposal, and checkpoints the
// position so a restart resumes past a fully-committed transaction. The commit
// file is the live curFile (tracked from rotation); the offset is the XID event's
// end position.
func (h *MySQLEventHandler) handleXID(ctx context.Context, header *replication.EventHeader) error {
	if header != nil {
		h.curPos = header.LogPos
		// The XID event's header timestamp is the source commit time — the
		// authoritative provenance stamp for the commit flush below.
		if header.Timestamp != 0 {
			h.curTxnSourceTS = header.Timestamp
		}
	}
	pos := mysql.Position{Name: h.curFile, Pos: h.curPos}

	// Merge the just-committed transaction's GTID into the consumed set FIRST,
	// so the position built below — the post-transaction watermark — already
	// covers this transaction. Resume reads this set (StartSyncGTID), so a
	// restart streams strictly after it.
	merged, err := mergeGTID(h.consumedGTID, h.curTxnGTID)
	if err != nil {
		return fmt.Errorf("merge committed GTID: %w", err)
	}
	h.consumedGTID = merged
	h.curTxnGTID = nil

	posProto := &dialectpb.MySQLBinLogPosition{Name: pos.Name, Pos: pos.Pos, RefreshEpoch: h.epoch, ChunkTag: chunkTag(h.config, h.flushBudget), SnapshottedTables: h.snapshottedTables}
	if h.consumedGTID != nil {
		posProto.GtidSet = h.consumedGTID.String()
	}
	bs, err := proto.Marshal(posProto)
	if err != nil {
		return err
	}

	// Bundle the watermark onto the transaction's final proposal: position and
	// entities commit in ONE raft entry, so the durable watermark advances
	// atomically with the applied index — a crash can never leave a committed
	// transaction outside the resume set, which is what shrinks the resume
	// re-emission window to the single in-flight transaction and lets the
	// dedup be transaction-scoped.
	emitted, err := h.flushPending(ctx, false, bs)
	if err != nil {
		return err
	}

	// The transaction is flushed; its provenance must not bleed into the next
	// one (whose own first row / GTID re-captures it).
	h.curTxnSourceTS = 0
	h.curTxnID = ""

	// A commit that emitted nothing (an unwatched-table transaction's XID)
	// still advances the resume position — out-of-band, as before.
	if !emitted {
		select {
		case h.positionChan <- bs:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	h.lastPos = &mysql.Position{Name: pos.Name, Pos: pos.Pos}
	zap.L().Debug("handleXID", zap.String("pos", pos.Name), zap.Uint32("offset", pos.Pos))
	return nil
}

// handleDDL logs a DDL statement observed on the source. The decode holds no
// cached schema — each row image decodes against the schema in its own binlog
// TableMapEvent (see columnsFromTableMap) — so a DDL needs no cache invalidation.
//
// A watched-table TRUNCATE is the MySQL analogue of the Postgres logical-
// replication TruncateMessage: committed has no clear-all primitive, so the
// truncate is NOT propagated and the sink now diverges from the source until a
// re-snapshot. It gets the same specific divergence Warn Postgres emits (see
// postgres.go and the TRUNCATE caveat in docs/operations/cdc-setup.md) instead of
// being buried in the generic DDL warn. Filtered through watches() because MySQL's
// binlog is server-wide: a TRUNCATE on an unwatched table diverges no sink of
// this ingest, and warning on it would cry wolf.
//
// The generic log line carries a BOUNDED classifier — leading keyword, schema,
// statement length — never the statement text. Query text can embed customer
// row values (a DDL literal like SET DEFAULT '<value>'; and under a
// misconfigured binlog_format, whole INSERT ... VALUES statements): node logs
// ship to aggregation and outlive an RTBF scrub, so raw statement text must
// never reach them (Preflight also hard-rejects non-ROW formats). The
// per-transaction BEGIN query event every ROW-format transaction opens with is
// skipped entirely — it is not DDL, and logging it would warn once per
// transaction.
func (h *MySQLEventHandler) handleDDL(e *replication.QueryEvent) {
	if e == nil {
		return
	}
	query := string(e.Query)
	if schema, table, isTruncate := truncateTarget(query); isTruncate {
		if schema == "" {
			schema = string(e.Schema) // unqualified TRUNCATE — the session's current database
		}
		if h.watches(schema, table) {
			zap.L().Warn("TRUNCATE on a watched table is not propagated to the sink; "+
				"the sink now diverges from the source and must be re-snapshotted to reconcile",
				zap.Strings("tables", []string{schema + "." + table}),
			)
			return
		}
	}
	keyword := leadingKeyword(query)
	if keyword == "BEGIN" {
		return // ROW-format transaction wrapper, one per txn — not DDL, pure noise
	}
	zap.L().Warn("handleDDL: DDL event received",
		zap.String("schema", string(e.Schema)),
		zap.String("keyword", keyword),
		zap.Int("statement_len", len(query)),
	)
}

// leadingKeyword returns the statement's first token, upper-cased — the bounded
// classifier handleDDL logs in place of the (potentially value-bearing) text.
func leadingKeyword(query string) string {
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return ""
	}
	return strings.ToUpper(fields[0])
}

// truncateTarget parses "TRUNCATE [TABLE] [schema.]table" out of a binlog DDL
// QueryEvent. It returns the referenced table (and schema, when the statement
// qualified one) and whether the statement is a TRUNCATE at all. The binlog
// carries the exact executed statement, so a keyword scan recognizes TRUNCATE and
// names its single target without a full SQL grammar (MySQL TRUNCATE takes exactly
// one table). Identifiers are unquoted (backticks stripped); a TRUNCATE whose
// target can't be named still reports ok=true so the caller treats it as a
// TRUNCATE, just an unnamed one (it won't match a watched table and falls through
// to the generic DDL warn).
func truncateTarget(query string) (schema, table string, ok bool) {
	fields := strings.Fields(query)
	if len(fields) == 0 || !strings.EqualFold(fields[0], "TRUNCATE") {
		return "", "", false
	}
	rest := fields[1:]
	if len(rest) > 0 && strings.EqualFold(rest[0], "TABLE") {
		rest = rest[1:]
	}
	if len(rest) == 0 {
		return "", "", true // a TRUNCATE we can't name — still a TRUNCATE
	}
	ref := strings.TrimRight(rest[0], ";")
	if s, tbl, qualified := strings.Cut(ref, "."); qualified {
		return unquoteMySQLIdent(s), unquoteMySQLIdent(tbl), true
	}
	return "", unquoteMySQLIdent(ref), true
}

// unquoteMySQLIdent strips MySQL backtick quoting from a single identifier.
func unquoteMySQLIdent(s string) string {
	return strings.Trim(s, "`")
}

// runStream drives the binlog stream until the context is canceled (clean exit,
// returns nil) or the stream errors (returns the error; the caller reconnects
// with backoff). It is the BinlogSyncer replacement for canal's event loop: one
// GetEvent loop dispatching each event into the same buffer / flush / position
// logic, with committed owning the loop so cancellation flows through GetEvent's
// context rather than a stored field, and GTID/format-description events ignored
// (Phase A stays file:pos).
func (h *MySQLEventHandler) runStream(ctx context.Context, streamer *replication.BinlogStreamer) error {
	for {
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if err := h.dispatchEvent(ctx, ev.Header, ev.Event); err != nil {
			return err
		}
	}
}

// dispatchEvent routes one binlog event (its header + payload) into the
// buffer/flush/position handlers. It is called once per top-level GetEvent, and
// recursively for each sub-event of a compressed TransactionPayloadEvent — with
// the OUTER event's header in the recursive case (see that case for why).
func (h *MySQLEventHandler) dispatchEvent(ctx context.Context, header *replication.EventHeader, event replication.Event) error {
	switch e := event.(type) {
	case *replication.RotateEvent:
		// A real rotation moves curFile so subsequent commits checkpoint
		// against the right file; the start-of-stream fake rotate that only
		// restates the current file is ignored.
		if isSkippableFakeRotate(header.Timestamp, string(e.NextLogName), h.curFile) {
			return nil
		}
		// Loud on purpose: rotation is rare (file fill, FLUSH LOGS, failover),
		// and "from" is the datum that splits a routine roll-forward from a
		// server starting the dump at an OLD file — at connect, a fake rotate
		// naming a different file than the resume seed lands here, and
		// from=checkpoint-file to=older-file is the rewind signature a
		// re-delivery incident is diagnosed by.
		zap.L().Info("binlog stream rotated",
			zap.String("from", h.curFile),
			zap.String("to", string(e.NextLogName)))
		h.curFile = string(e.NextLogName)
	case *replication.GTIDEvent:
		// The GTID of the transaction about to stream; merged into the
		// consumed set when that transaction commits (handleXID).
		gtid, err := e.GTIDNext()
		if err != nil {
			return fmt.Errorf("decode GTID event: %w", err)
		}
		h.curTxnGTID = gtid
	case *replication.RowsEvent:
		return h.handleRows(ctx, header, e)
	case *replication.XIDEvent:
		return h.handleXID(ctx, header)
	case *replication.QueryEvent:
		h.handleDDL(e)
	case *replication.TransactionPayloadEvent:
		// A compressed transaction (binlog_transaction_compression=ON, GA since
		// MySQL 8.0.20 and common on managed MySQL): the server wraps this txn's
		// TableMap/Rows/XID events inside one payload event that go-mysql decodes
		// but does NOT auto-expand. Without dispatching the sub-events, every
		// compressed transaction is silently dropped — no rows, and the XID never
		// fires so the binlog position never advances (a stalled, silent-data-loss
		// ingest). Route each decompressed sub-event through the same handlers,
		// passing THIS (outer) header so the commit position is the payload's real
		// file offset: the inner sub-events carry end_log_pos == 0.
		for _, sub := range e.Events {
			if err := h.dispatchEvent(ctx, header, sub.Event); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeSourceSeq maps a binlog coordinate (file, offset) plus an intra-coordinate
// sub-index to a strictly-monotonic uint64 used as a proposal's SourceSeq for
// effectively-once dedup. The file's numeric suffix occupies the high 32 bits and
// the offset the low 32 bits, so ordering matches binlog order: a later file
// always outranks an earlier one regardless of offset, and within a file the
// offset orders. sub is added on top to separate several partial flushes that
// share one coordinate (an oversized RowsEvent) — sub==0 yields exactly the
// bare-coordinate value, so single-flush events (the common case) keep the same
// seq as before and a persisted highwater stays valid across upgrade. sub is
// bounded in practice by the gap to the next coordinate (>= the minimum binlog
// event size, ~19 bytes), which one RowsEvent's flush count never approaches.
// Returns 0 (which disables dedup for the proposal — never a false positive) when
// the file name has no parseable numeric suffix, e.g. an unexpected naming scheme.
func encodeSourceSeq(name string, pos uint32, sub uint32) uint64 {
	fileNum, ok := binlogFileNum(name)
	if !ok {
		return 0
	}
	return (fileNum<<32 | uint64(pos)) + uint64(sub)
}

// binlogFileNum parses the numeric suffix of a binlog file name (e.g.
// "binlog.000042" -> 42). ok is false for a name with no parseable numeric
// suffix. It is the shared basis of both the SourceSeq's high 32 bits
// (encodeSourceSeq) and the failover-regression check (a stream whose file
// number drops below the resume baseline is a different binlog lineage — a
// promoted replica or RESET MASTER — whose low coordinates would otherwise
// encode below the durable highwater and be silently deduped away).
func binlogFileNum(name string) (uint64, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot < 0 || dot == len(name)-1 {
		return 0, false
	}
	fileNum, err := strconv.ParseUint(name[dot+1:], 10, 32)
	if err != nil {
		return 0, false
	}
	return fileNum, true
}

// chunkRenderingVersion is folded into chunkTag so a binary change that alters
// how a row renders to JSON — and therefore where an oversized transaction's
// same-coordinate part boundaries fall (via sql.EntityFlushBytes) — is detected
// on resume and freezes a re-chunked replay rather than silently mis-dropping a
// shifted row. BUMP THIS whenever a change to row→JSON rendering could move
// those boundaries. (Config and budget changes are captured directly; this
// constant is the escape hatch for pure-code rendering changes, until the 0.8
// resume+dedup unification removes the reliance — see the
// unify-ingest-resume-dedup-gtid-watermark ticket.)
const chunkRenderingVersion = 1

// chunkTag fingerprints the inputs that determine how an oversized transaction
// is split into same-coordinate parts: the flush byte budget, every topic-spec's
// rendering-relevant fields (mappings, primary key, type), and the binary
// rendering version. It is stamped on every checkpoint; a mismatch on resume
// means re-chunking a replayed same-coordinate multi-part transaction could
// shift an as-yet-uncommitted row into a part whose SourceSeq the dedup would
// drop — silent loss — so those replayed parts are flagged DedupUnsafe and the
// worker freezes. Never returns 0 (reserved for "no baseline recorded").
//
// Every spec is hashed so a change to ANY topic's mappings/PK/type re-fingerprints
// the ingestable. The specs are separated only BETWEEN entries (not after the
// last), so a single-topic config hashes byte-identically to the pre-routing
// version — a persisted single-topic checkpoint stays valid across the upgrade.
//
// budget is passed in (not read from the global sql.TxnSoftFlushBytes) so it is
// captured once at Ingest start — the checkpoint path runs on the worker
// goroutine, and a test that overrides the budget must not race a per-checkpoint
// global read.
func chunkTag(config *sql.Config, budget int) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(budget)) //nolint:gosec // G115: budget is a positive byte count
	_, _ = h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], chunkRenderingVersion)
	_, _ = h.Write(buf[:])
	if config != nil {
		config.EnsureTopics()
		for si := range config.Topics {
			if si > 0 {
				_, _ = h.Write([]byte{0xfe}) // between-specs separator; absent for one spec
			}
			spec := &config.Topics[si]
			for _, m := range spec.Mappings {
				_, _ = h.Write([]byte(m.JsonName))
				_, _ = h.Write([]byte{0})
				_, _ = h.Write([]byte(m.SQLColumn))
				_, _ = h.Write([]byte{0})
			}
			_, _ = h.Write([]byte{0xff})
			for _, pk := range spec.PrimaryKey {
				_, _ = h.Write([]byte(pk))
				_, _ = h.Write([]byte{0})
			}
			if spec.Type != nil {
				_, _ = h.Write([]byte(spec.Type.ID))
			}
		}
	}
	t := h.Sum64()
	if t == 0 {
		t = 1 // 0 is reserved for "no baseline" (pre-feature checkpoint)
	}
	return t
}

// snapshot performs a pure-SQL initial dump of all watched tables using
// keyset (cursor) pagination with one short transaction per batch. It
// replaces the external mysqldump binary that canal uses internally.
//
// The flow:
//
//  1. If resumePos is nil (fresh start): FLUSH TABLES WITH READ LOCK,
//     SHOW BINARY LOG STATUS, UNLOCK TABLES. The global lock is held
//     only for microseconds to capture the starting binlog position.
//     If resumePos is non-nil (mid-snapshot resume), reuse the saved
//     position so the binlog tail still covers the time window from
//     before the original snapshot started.
//
//  2. For each table, read in batches:
//     SELECT * FROM t WHERE pk > :last_pk ORDER BY pk ASC LIMIT :N
//     inside a short REPEATABLE READ transaction. Each batch becomes
//     one cluster.Proposal, followed by a position checkpoint that
//     records the table and last pk flushed. On restart, the checkpoint
//     drives resume-from-last-pk so no already-flushed row is replayed.
//
//  3. The snapshot is not point-in-time consistent (tx per batch), but
//     the binlog stream that starts from the pre-snapshot position will
//     replay any concurrent changes, so the end state converges.
//
// Returns the binlog position where streaming should begin.
func snapshot(
	ctx context.Context,
	config *sql.Config,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
	resumePos *mysql.Position,
	resumeGTID string,
	resumeProgress *dialectpb.SnapshotProgress,
	epoch uint64,
) (*mysql.Position, string, error) {
	db, err := openMySQL(config.ConnectionString)
	if err != nil {
		return nil, "", fmt.Errorf("snapshot: open: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Determine the binlog position + GTID set to start streaming from. On a fresh
	// start we capture them now under a brief global lock; on a mid-snapshot resume
	// we keep the saved coordinate so the binlog tail covers the pre-snapshot
	// window (the GTID set, like the position, is the original snapshot point).
	var pos mysql.Position
	gtid := resumeGTID
	if resumePos != nil {
		pos = *resumePos
	} else {
		pos, gtid, err = captureBinlogPosition(ctx, db)
		if err != nil {
			return nil, "", err
		}
		logCapturedPositioning(ctx, db, gtid)
	}

	batchSize := parseBatchSize(config.Options)

	// Build the initial progress map from the resume checkpoint.
	progress := &dialectpb.SnapshotProgress{
		LastPkByTable:   map[string]string{},
		CompletedTables: nil,
		ChunksByTable:   map[string]*dialectpb.TableChunkProgress{},
	}
	completed := map[string]bool{}
	if resumeProgress != nil {
		maps.Copy(progress.LastPkByTable, resumeProgress.LastPkByTable)
		// A chunked table's FROZEN plan (with per-chunk cursors) resumes
		// verbatim — never re-planned; see the proto contract.
		maps.Copy(progress.ChunksByTable, resumeProgress.ChunksByTable)
		progress.CompletedTables = append(progress.CompletedTables, resumeProgress.CompletedTables...)
		// Carry the partial-backfill mark through to every persisted
		// mid-snapshot checkpoint: dropping it would make a crash-resume
		// treat the pre-seeded sibling completions as a nearly-done FULL
		// snapshot and emit the refresh marker — whose topic sweep would
		// delete every sibling row this backfill never re-emits.
		progress.PartialBackfill = resumeProgress.PartialBackfill
		for _, t := range resumeProgress.CompletedTables {
			completed[t] = true
		}
	}

	// Announce whether this is a resume or a cold start, at absolute (table-level)
	// granularity, so an operator can tell the two apart from the log alone — a
	// per-run batch counter that resets each supervisor restart cannot. The keyset
	// cursor itself is deliberately NOT logged: a natural PK is often source PII
	// (see the per-batch flush log), so progress is reported by table counts only.
	if resumeProgress != nil {
		zap.L().Info("snapshot: resuming from checkpoint",
			zap.Int("tables_complete", len(completed)),
			zap.Int("tables_resuming", len(progress.LastPkByTable)),
			zap.Int("tables_total", len(config.Tables)),
			zap.Uint64("refresh_epoch", epoch))
	} else {
		zap.L().Info("snapshot: starting fresh",
			zap.Int("tables_total", len(config.Tables)),
			zap.Uint64("refresh_epoch", epoch))
	}

	readers := parseSnapshotReaders(config.Options)

	for _, table := range config.Tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table",
				zap.String("table", table),
			)
			continue
		}
		// Chunked-parallel dispatch: a table with a persisted chunk plan
		// ALWAYS resumes chunked (the frozen-plan contract — even if
		// snapshot_readers has since changed, including to 1). A fresh table
		// goes parallel only when the operator opted in (snapshot_readers > 1)
		// AND the planner found a split strategy; everything else takes the
		// single-stream path unchanged. A table mid-flight on the SINGLE
		// stream (last_pk_by_table) similarly keeps its cursor — never
		// converted to chunks mid-table.
		plan := progress.ChunksByTable[table]
		if plan == nil && readers > 1 {
			if _, resuming := progress.LastPkByTable[table]; !resuming {
				spec := config.SpecForTable(table)
				if spec == nil {
					return nil, "", fmt.Errorf("snapshot: no topic-spec routes table %q", table)
				}
				p, perr := planTableChunks(ctx, db, table, spec, readers, batchSize)
				if perr != nil {
					return nil, "", fmt.Errorf("snapshot: table %s: %w", table, perr)
				}
				if p != nil {
					plan = p
					progress.ChunksByTable[table] = p
				}
			}
		}
		if plan != nil {
			if err := snapshotTableParallel(ctx, db, config, table, batchSize, readers, plan, progress, &pos, gtid, epoch, pr); err != nil {
				return nil, "", fmt.Errorf("snapshot: table %s: %w", table, err)
			}
		} else if err := snapshotTable(ctx, db, config, table, batchSize, progress, &pos, gtid, epoch, pr, po); err != nil {
			return nil, "", fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		// Mark complete and drop any partial cursor (single-stream or chunked).
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		delete(progress.ChunksByTable, table)
		completed[table] = true

		if err := emitProgress(ctx, po, pos, gtid, progress, epoch); err != nil {
			return nil, "", err
		}
		zap.L().Info("snapshot: table complete", zap.String("table", table))
	}

	return &pos, gtid, nil
}

// captureBinlogPosition briefly acquires a global read lock to read the
// current binlog file and offset. The lock is released immediately; it
// only needs to be held long enough to sample a consistent position.
func captureBinlogPosition(ctx context.Context, db *gosql.DB) (mysql.Position, string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return mysql.Position{}, "", fmt.Errorf("snapshot: conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return mysql.Position{}, "", fmt.Errorf("snapshot: FLUSH TABLES WITH READ LOCK: %w", err)
	}
	pos, gtidSet, err := binlogStatus(ctx, conn)
	// Always release the lock, regardless of binlogStatus success.
	if _, unlockErr := conn.ExecContext(ctx, "UNLOCK TABLES"); unlockErr != nil && err == nil {
		err = unlockErr
	}
	if err != nil {
		return mysql.Position{}, "", fmt.Errorf("snapshot: binlog status: %w", err)
	}
	return pos, gtidSet, nil
}

// parseBatchSize reads "batch_size" from Config.Options, falling back to
// defaultSnapshotBatchSize on missing/invalid values.
func parseBatchSize(options map[string]string) int {
	if v, ok := options["batch_size"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultSnapshotBatchSize
}

// emitProgress sends a position checkpoint with the given snapshot
// progress. The binlog position stays anchored at the pre-snapshot
// capture so a resume still replays binlog events that occurred while
// the snapshot was running.
func emitProgress(
	ctx context.Context,
	po chan<- cluster.Position,
	pos mysql.Position,
	gtid string,
	progress *dialectpb.SnapshotProgress,
	epoch uint64,
) error {
	bs, err := encodeProgress(pos, gtid, progress, epoch)
	if err != nil {
		return err
	}
	select {
	case po <- bs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// encodeProgress marshals a binlog position + snapshot progress into the
// dialect's checkpoint bytes. Shared by emitProgress (out-of-band checkpoint)
// and the snapshot loop, which carries the checkpoint INLINE on the batch
// proposal (Proposal.Position) so rows and checkpoint commit in one atomic raft
// entry. A shallow copy is enough — proto.Marshal serializes the maps/slices at
// call time.
func encodeProgress(pos mysql.Position, gtid string, progress *dialectpb.SnapshotProgress, epoch uint64) ([]byte, error) {
	return proto.Marshal(&dialectpb.MySQLBinLogPosition{
		Name:             pos.Name,
		Pos:              pos.Pos,
		GtidSet:          gtid,
		SnapshotProgress: progress,
		RefreshEpoch:     epoch,
	})
}

// handOffSnapshotWindow hands one read window's rows to pr — one single-row
// proposal per row (a proposal is a TRANSACTION; the read window is read tuning,
// never proposal composition) — bundling the inline resume checkpoint
// (Proposal.Position) on every sql.SnapshotCheckpointStride-th row and on the
// window's final row. Each checkpoint's cursor is THAT row's key, not the
// window's last, so the durable resume cursor trails committed progress by at
// most one stride: a worker frozen mid-window restarts from the last committed
// checkpoint instead of re-reading and re-proposing the whole window (whose
// SourceSeq-0 duplicates nothing would dedup out of the permanent log), and the
// freeze/restart supervisor sees an advancing position instead of counting a
// merely-slow worker toward give-up. A checkpoint-bearing row is an ordered
// proposal (the ingest worker drains its pipeline before proposing it), so it
// trails every row it covers; rows after it simply re-emit on resume.
// progress is mutated in place per checkpoint, ending at the window's last
// EMITTED key. The caller's read cursor (lastPK) can end further: it advances
// per SCANNED row, so marshal-skipped tail rows sit past the durable cursor.
// That gap is convergent — a crash re-scans only rows that skip again, and an
// all-skipped window (no proposals, so no checkpoint) simply leaves the
// durable cursor at the previous emission. Mirrors the Postgres dialect's
// handOffSnapshotWindow; only the checkpoint encoding differs.
func handOffSnapshotWindow(
	ctx context.Context,
	rows []*cluster.Entity,
	table string,
	progress *dialectpb.SnapshotProgress,
	pos mysql.Position,
	gtid string,
	epoch uint64,
	pr chan<- *cluster.Proposal,
) error {
	stride := sql.SnapshotCheckpointStride
	for ri, row := range rows {
		p := &cluster.Proposal{Entities: []*cluster.Entity{row}}
		if ri == len(rows)-1 || (ri+1)%stride == 0 {
			progress.LastPkByTable[table] = string(row.Key)
			posBytes, err := encodeProgress(pos, gtid, progress, epoch)
			if err != nil {
				return err
			}
			p.Position = posBytes
		}
		select {
		case pr <- p:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// logCapturedPositioning makes the positioning decision VISIBLE at snapshot
// capture — the one moment it is decided. The preflight advisory keys on
// @@gtid_mode, but the capture keys on @@gtid_executed being non-empty, and
// the two disagree on a server whose GTID mode was enabled online (or that
// is brand new) with no transactions committed since: mode=ON, executed set
// empty. That case silently fell back to file:pos with no log line anywhere
// — found in the field as "lag: null and no GTID lines despite gtid_mode=ON".
// The fallback itself is CORRECT (StartSyncGTID with an empty set would
// replay every retained binlog from the beginning, re-delivering history the
// snapshot already covered) and it self-heals: the stream handler merges
// GTIDEvents regardless of how the sync started, so the first streamed
// transaction upgrades the checkpoint — and reconnects/restarts — to GTID
// positioning. This log line is the missing observability, not a behavior
// change. The mode query is best-effort: on error the generic message logs.
// logResumePositioning is logCapturedPositioning's resume-time twin: capture
// logs the positioning decision when it is MADE; this logs it when it is
// USED — one line per binlog (re)connect stating how the stream positioned
// and from exactly where. It is the first datum a re-delivery incident needs
// ("what did the worker resume FROM?") and previously existed nowhere: the
// durable checkpoint is a protobuf in bbolt, invisible without decoding it
// out of a backup. Paired with the rotation log in dispatchEvent, a server
// that starts the dump at an old binlog is visible on the next line instead
// of surfacing as an unexplained replay. The coordinates are server
// replication metadata (file names, offsets, transaction-id sets) — no row
// data, keys, or connection identity.
func logResumePositioning(useGTID bool, gtidSet mysql.GTIDSet, lastPos *mysql.Position) {
	if useGTID {
		zap.L().Info("binlog stream started by GTID positioning (failover-safe)",
			zap.String("resumeGtidSet", gtidSet.String()))
		return
	}
	zap.L().Info("binlog stream started by binlog file:pos positioning",
		zap.String("resumeFile", lastPos.Name),
		zap.Uint32("resumePos", lastPos.Pos))
}

func logCapturedPositioning(ctx context.Context, db *gosql.DB, gtidSet string) {
	if gtidSet != "" {
		zap.L().Info("snapshot: captured GTID positioning (failover-safe resume, real lag/caughtUp)")
		return
	}
	var mode string
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil {
		mode = "unknown"
	}
	msg, level := gtidFallbackLog(mode)
	if level == zap.WarnLevel {
		zap.L().Warn(msg, zap.String("gtid_mode", mode))
	} else {
		zap.L().Info(msg, zap.String("gtid_mode", mode))
	}
}

// gtidFallbackLog picks the message and level for an empty captured GTID set.
// mode=ON is the benign self-healing case (Info); anything else is the
// documented degraded mode (Warn, matching the preflight advisory's promise
// that file:pos positioning is visible rather than silent).
func gtidFallbackLog(gtidMode string) (string, zapcore.Level) {
	if strings.EqualFold(gtidMode, "ON") {
		return "snapshot: gtid_mode=ON but @@gtid_executed is empty (no transactions committed " +
			"since GTID was enabled — a freshly-enabled or brand-new server); positioning by " +
			"binlog file:pos for now, upgrading to GTID automatically at the first streamed " +
			"transaction", zap.InfoLevel
	}
	return "snapshot: positioning by binlog file:pos (no GTID set at capture); resume is not " +
		"failover-safe and lag/caughtUp stay null — set gtid_mode=ON and " +
		"enforce_gtid_consistency=ON for failover-safe positioning", zap.WarnLevel
}

// binlogStatus returns the current binlog filename and byte offset. It
// tries SHOW BINARY LOG STATUS (MySQL 8.4+) first, falling back to
// SHOW MASTER STATUS for older versions.
func binlogStatus(ctx context.Context, conn *gosql.Conn) (mysql.Position, string, error) {
	for _, query := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			continue
		}
		if len(cols) < 2 {
			// A conformant SHOW ...STATUS returns at least File + Position; a
			// non-standard fork/proxy that returns fewer is skipped rather than
			// indexed blindly — dest[0]/dest[1] below would panic.
			_ = rows.Close()
			continue
		}

		if !rows.Next() {
			_ = rows.Close()
			continue
		}

		// Scan File (col 0) and Position (col 1) into typed destinations; pull the
		// Executed_Gtid_Set column by name (the GTID set as of this position —
		// empty under gtid_mode=OFF); discard the rest. database/sql handles the
		// int64→uint32 conversion for Position.
		var file, gtidSet string
		var pos uint32
		dest := make([]any, len(cols))
		for i := range dest {
			dest[i] = new(any)
		}
		dest[0] = &file
		dest[1] = &pos
		for i, c := range cols {
			if strings.EqualFold(c, "Executed_Gtid_Set") {
				dest[i] = &gtidSet
			}
		}

		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			continue
		}
		_ = rows.Close()

		return mysql.Position{Name: file, Pos: pos}, gtidSet, nil
	}

	return mysql.Position{}, "", fmt.Errorf("binlogStatus: could not determine binlog position")
}

// snapshotTable reads all rows from a table using keyset pagination.
// Each batch opens its own short REPEATABLE READ transaction, emits
// its rows as a single proposal, then checkpoints the progress
// (last pk flushed) before moving on. This bounds MVCC read-view
// lifetime to the batch duration instead of the full-table scan.
//
// Column mapping and primary-key extraction mirror the binlog streaming
// path (OnRow) so consumers see identical entity shapes.
func snapshotTable(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	table string,
	batchSize int,
	progress *dialectpb.SnapshotProgress,
	pos *mysql.Position,
	gtid string,
	epoch uint64,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	// Resolve this table's topic-spec (the snapshot iterates config.Tables, so table
	// is a config entry SpecForTable keys on directly). One spec for the flat form.
	spec := config.SpecForTable(table)
	if spec == nil {
		return fmt.Errorf("snapshot: no topic-spec routes table %q", table)
	}
	// lastPK, haveLastPK distinguish "no rows yet flushed" from
	// "last flushed pk was the empty string".
	lastPK, haveLastPK := progress.LastPkByTable[table]

	batchNum := 0
	totalRows := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchNum++

		rows, lastKey, count, err := readBatch(ctx, db, table, spec, lastPK, haveLastPK, "", batchSize)
		if err != nil {
			return err
		}
		if count == 0 {
			// Empty table, or we've advanced past the last row.
			break
		}

		// Stamp the snapshot rows with the refresh epoch (a purge re-snapshot
		// bumps it; the initial snapshot is epoch 1) so the closing
		// refresh-boundary marker can sweep the rows this positive enumeration
		// could not re-emit.
		stampGeneration(rows, epoch)

		// Advance the read cursor to this window, then hand the rows off with
		// inline checkpoints every sql.SnapshotCheckpointStride rows (and on the
		// window's final row) — see handOffSnapshotWindow. Each checkpoint
		// commits atomically with the row it rides (Proposal.Position), closing
		// the effectively-once gap a separate position proposal left open for
		// snapshot rows (SourceSeq 0, so the streaming dedup can't cover them).
		lastPK = lastKey
		haveLastPK = true
		if err := handOffSnapshotWindow(ctx, rows, table, progress, *pos, gtid, epoch, pr); err != nil {
			return err
		}
		totalRows += count

		// Deliberately no last_pk: a natural primary key is often source PII
		// (email, national id, account no), and this line is Info-level and
		// shipped to log aggregation. The batch/row counts give progress; the
		// resume cursor lives in progress.LastPkByTable, not the logs.
		zap.L().Info("snapshot: batch flushed",
			zap.String("table", table),
			zap.Int("batch", batchNum),
			zap.Int("rows_in_batch", count),
			zap.Int("rows_total", totalRows),
		)

		if count < batchSize {
			// A short batch means we've reached the end.
			break
		}
	}

	return nil
}

// readBatch opens a short REPEATABLE READ transaction and reads up to
// batchSize rows with pk > lastPK (or the first batchSize rows when
// haveLastPK is false). Returns the entities, the last pk value in the
// batch, and the count. An empty batch returns (nil, "", 0, nil).
func readBatch(
	ctx context.Context,
	db *gosql.DB,
	table string,
	spec *sql.TopicSpec,
	lastPK string,
	haveLastPK bool,
	upper string,
	batchSize int,
) ([]*cluster.Entity, string, int, error) {
	pkCols := spec.PrimaryKey
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{
		Isolation: gosql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, "", 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Read TIMESTAMP columns in UTC so the snapshot's text matches the CDC path,
	// which decodes TIMESTAMP in UTC too (see binlogSyncerConfig). Without this the
	// snapshot uses the server's session tz, so a TIMESTAMP diverges from CDC on a
	// non-UTC server. DATETIME/DATE/TIME are tz-agnostic literals and unaffected.
	if _, err := tx.ExecContext(ctx, "SET time_zone = '+00:00'"); err != nil {
		return nil, "", 0, fmt.Errorf("set session time_zone: %w", err)
	}

	// The batch predicate (FROM … WHERE … ORDER BY … LIMIT) is shared verbatim by
	// the row read below and the JSON type-resolution query in phase 2, so under
	// this REPEATABLE READ tx both see the same rows in the same order and zip
	// one-for-one. The type query prepends its own `?` JSON-path args ahead of
	// these cursor args (SELECT placeholders bind before WHERE placeholders).
	fromWhere, args, err := batchPredicate(table, pkCols, lastPK, haveLastPK, upper, batchSize)
	if err != nil {
		return nil, "", 0, err
	}

	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT * %s", fromWhere), args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, "", 0, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, "", 0, err
	}
	cats := make([]sql.JSONCategory, len(colTypes))
	jsonCol := make([]bool, len(colTypes))
	for i, ct := range colTypes {
		cats[i] = mysqlCategoryForTypeName(ct.DatabaseTypeName())
		jsonCol[i] = ct.DatabaseTypeName() == "JSON"
	}

	// Phase 1: read and buffer the whole batch. The JSON type-resolution query
	// (phase 2) can't run while this cursor is open on the same tx, so buffer
	// every row first. BIT/TIME are fixed inline here; JSON rendering is deferred
	// to phase 3 because it needs the per-leaf DECIMAL/DOUBLE types phase 2
	// resolves — the rendered text alone can't tell a DECIMAL 1.5 from a DOUBLE
	// 1.5, and they must be emitted differently to match CDC byte-for-byte.
	type pathKey struct {
		col  int
		path string
	}
	var (
		buffered  [][]any
		unionList []pathKey
		unionSeen = map[pathKey]bool{}
	)
	for rows.Next() {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, "", 0, err
		}

		// database/sql returns a BIT column as its raw bytes, but the binlog path
		// decodes BIT to an int64. Convert here so a BIT value renders as the same
		// JSON number (and keys identically) on both paths, instead of the snapshot
		// emitting the bytes as a text string ("A") while CDC emits the number (65).
		//
		// TIME(N): the snapshot pads a zero fraction to N digits
		// ("13:45:30.000000") but go-mysql's TIME2 decoder drops it entirely
		// ("13:45:30"); strip an all-zero fraction so both paths agree. (DATETIME2/
		// TIMESTAMP2 pad on both sides — only TIME2 diverges.)
		for i, ct := range colTypes {
			switch ct.DatabaseTypeName() {
			case "BIT":
				if b, ok := vals[i].([]byte); ok {
					vals[i] = bitToInt64(b)
				}
			case "TIME":
				if b, ok := vals[i].([]byte); ok {
					vals[i] = []byte(stripZeroTimeFraction(string(b)))
				}
			case "JSON":
				// Collect the float-syntax leaf paths whose DECIMAL-vs-DOUBLE type
				// phase 2 must resolve. Integer/string leaves are unambiguous and
				// never queried; a JSON cell with no fractional numbers adds nothing,
				// so the type query is skipped entirely when the batch has none.
				if b, ok := vals[i].([]byte); ok {
					for _, p := range sql.JSONNumberPaths(b) {
						k := pathKey{col: i, path: p}
						if !unionSeen[k] {
							unionSeen[k] = true
							unionList = append(unionList, k)
						}
					}
				}
			}
		}

		buffered = append(buffered, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	_ = rows.Close() // free the cursor before phase 2 queries the same tx

	// Phase 2: resolve each collected JSON leaf's real type (DECIMAL vs DOUBLE,
	// which the rendered text has lost) with a SINGLE query over the same rows in
	// the same tx, reusing the batch predicate so the result set matches the
	// buffered rows one-for-one. typesByRow[r][col] is the path->type map for that
	// cell. On any failure — query error, scan error, or a row-count mismatch — we
	// fall back to blanket float-normalization (CanonicalizeMySQLJSONNumbers),
	// which preserves the existing double parity with no regression, forfeiting
	// only the new decimal exactness.
	typesByRow := make([]map[int]map[string]string, len(buffered))
	typeResolveFailed := false
	if len(unionList) > 0 {
		sel := make([]string, len(unionList))
		typeArgs := make([]any, 0, len(unionList)+len(args))
		for i, uk := range unionList {
			sel[i] = fmt.Sprintf("JSON_TYPE(JSON_EXTRACT(%s, ?))", sqlident.MySQL.Ident(columns[uk.col]))
			typeArgs = append(typeArgs, uk.path)
		}
		typeArgs = append(typeArgs, args...)
		// The SELECT list is a JSON_TYPE(JSON_EXTRACT(`col`, ?)) expression per leaf
		// over result-set column names quoted through the shared sqlident seam (any
		// embedded backtick doubled), and fromWhere is the same quoted-identifier
		// predicate the batch query already used. Every runtime value — the JSON
		// paths and the cursor — is a bound parameter in typeArgs, so nothing
		// user-controlled is concatenated into the SQL.
		//nolint:gosec // G202: identifiers are quoted via sqlident (backticks doubled); all values are bound params.
		typeQuery := fmt.Sprintf("SELECT %s %s", strings.Join(sel, ", "), fromWhere)

		trows, terr := tx.QueryContext(ctx, typeQuery, typeArgs...)
		if terr != nil {
			typeResolveFailed = true
		} else {
			r := 0
			for trows.Next() {
				dest := make([]gosql.NullString, len(unionList))
				dptrs := make([]any, len(unionList))
				for i := range dest {
					dptrs[i] = &dest[i]
				}
				if err := trows.Scan(dptrs...); err != nil {
					typeResolveFailed = true
					break
				}
				if r < len(typesByRow) {
					for j, uk := range unionList {
						if !dest[j].Valid {
							continue // path absent in this row (JSON_EXTRACT -> NULL)
						}
						if typesByRow[r] == nil {
							typesByRow[r] = map[int]map[string]string{}
						}
						if typesByRow[r][uk.col] == nil {
							typesByRow[r][uk.col] = map[string]string{}
						}
						typesByRow[r][uk.col][uk.path] = dest[j].String
					}
				}
				r++
			}
			_ = trows.Close()
			if !typeResolveFailed && (trows.Err() != nil || r != len(buffered)) {
				typeResolveFailed = true
			}
		}
		if typeResolveFailed {
			zap.L().Warn("readBatch: JSON leaf type resolution failed; using float-normalization fallback",
				zap.String("table", table),
			)
		}
	}

	// Phase 3: build entities. Render JSON columns with the resolved per-leaf types
	// (decimals kept exact, doubles float64-normalized to match CDC); other
	// columns are unchanged from phase 1.
	var entities []*cluster.Entity
	var batchLastPK string
	scanned := 0
	for r, vals := range buffered {
		for i := range vals {
			if !jsonCol[i] {
				continue
			}
			b, ok := vals[i].([]byte)
			if !ok {
				continue // SQL NULL
			}
			if typeResolveFailed {
				vals[i] = sql.CanonicalizeMySQLJSONNumbers(b)
			} else {
				vals[i] = sql.RenderMySQLJSONByType(b, typesByRow[r][i])
			}
		}

		// m holds the text form, used only for the entity key (unchanged so keys
		// stay stable and match the CDC path). raw/catByName carry the value and
		// its type so the payload renders as natural JSON, matching what canal
		// produces on the binlog path.
		m := make(map[string]any)
		raw := make(map[string]any)
		catByName := make(map[string]sql.JSONCategory)
		for i, colName := range columns {
			lc := strings.ToLower(colName)
			v := vals[i]
			raw[lc] = v
			catByName[lc] = cats[i]
			if v == nil {
				m[lc] = nil
			} else if b, ok := v.([]byte); ok {
				m[lc] = string(b)
			} else {
				m[lc] = fmt.Sprintf("%v", v)
			}
		}

		toJSON := sql.BuildEntityJSON(spec.Mappings, raw, catByName)

		// The cursor advances for every SCANNED row — BEFORE the marshal — and
		// the returned count is the scanned count, not the emitted count. With
		// the cursor keyed to emission, a batch whose rows all skipped
		// returned count 0 and the caller's exhaustion check ended the table
		// scan — silently truncating every row after the bad one. No known
		// MySQL column type produces an unmarshalable value (the engine
		// rejects NaN/Inf at insert), so the skip branch is defensive; but a
		// defensive branch must be SAFE when it fires, not end the scan.
		// Mirrors the Postgres readBatch exactly.
		key := sql.CompositeKey(m, spec.PrimaryKey)
		batchLastPK = key
		scanned++

		jsonBytes, err := json.Marshal(toJSON)
		if err != nil {
			zap.L().Warn("readBatch: skipping row with unmarshalable data",
				zap.String("table", table),
				zap.Error(err),
			)
			continue
		}

		entities = append(entities, &cluster.Entity{
			Type: spec.Type,
			Key:  []byte(key),
			Data: jsonBytes,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, "", 0, err
	}

	return entities, batchLastPK, scanned, nil
}

// openMySQL opens a database/sql connection to the MySQL source (snapshot +
// preflight queries), applying the connection's TLS posture — sslmode / custom
// CA / client cert — via mysql.Config.TLS. That is the SAME *tls.Config the CDC
// binlog syncer uses (both from cluster.ParseMySQLConn), so the snapshot and the
// stream are secured identically, mirroring Postgres where pgx carries sslmode to
// both. A parse or TLS-build error surfaces here rather than as a silently
// plaintext connection, and is redaction-safe (never echoes the ${VAR}-resolved
// connection string).
func openMySQL(connectionString string) (*gosql.DB, error) {
	conn, err := cluster.ParseMySQLConn(connectionString)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := conn.TLSClientConfig()
	if err != nil {
		return nil, err
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = conn.User
	cfg.Passwd = conn.Password
	cfg.Net = "tcp"
	cfg.Addr = conn.Addr()
	cfg.DBName = conn.Database
	cfg.TLS = tlsCfg
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return gosql.OpenDB(connector), nil
}

// dsnDatabase returns the database (schema) named in a mysql:// connection
// string, or "" if it can't be parsed or names no database. The binlog row
// filter scopes to it (see MySQLEventHandler.watches) so a server-wide binlog
// doesn't bleed same-named tables from other databases into the topic.
func dsnDatabase(connectionString string) string {
	// cluster.ParseConnString, never a bare url.Parse — see buildDSN.
	u, err := cluster.ParseConnString(connectionString)
	if err != nil {
		return ""
	}
	return strings.TrimPrefix(u.Path, "/")
}
