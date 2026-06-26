package mysql

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
)

type MySQLDialect struct{}

// syncerBackoff{Min,Max} bound the retry interval for connecting the binlog
// syncer (StartSync) and reconnecting after a stream error. A transient MySQL
// outage (DNS blip, container restart, network partition) at Ingest startup
// used to immediately return an error, leaving the worker
// leader-but-not-ingesting with no recovery path. The retry loop below caps at
// Max and is bounded by ctx so a shutdown still propagates promptly.
const (
	syncerBackoffMin = 1 * time.Second
	syncerBackoffMax = 30 * time.Second

	// maxPendingEntities is the soft limit on buffered entities per
	// transaction. If a single MySQL transaction modifies more than
	// this many rows, the handler emits a partial proposal to avoid
	// unbounded memory growth. This breaks atomicity for oversized
	// transactions — an acceptable trade-off versus OOM-ing the process.
	maxPendingEntities = 10000

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	var rowImage string
	if err := db.QueryRowContext(ctx, `SELECT @@global.binlog_row_image`).Scan(&rowImage); err != nil {
		return fmt.Errorf("read binlog_row_image: %w", err)
	}
	if !strings.EqualFold(rowImage, "MINIMAL") {
		return nil // FULL / NOBLOB — the key (never a blob) is in the before-image
	}

	fix := "set `binlog_row_image=FULL`, or add a PRIMARY KEY covering the configured primaryKey"
	for _, table := range config.Tables {
		pkCols, err := mysqlPrimaryKey(ctx, db, table)
		if err != nil {
			return err
		}
		if err := sql.CheckKeyCoverage(config.PrimaryKey, pkCols, table, fix); err != nil {
			return err
		}
	}
	return nil
}

// mysqlPrimaryKey returns the PRIMARY KEY columns of a table in the connection's
// current database — exactly the columns a MINIMAL binlog row image carries on a
// DELETE.
func mysqlPrimaryKey(ctx context.Context, db *gosql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT k.column_name
		FROM information_schema.key_column_usage k
		JOIN information_schema.table_constraints t
		  ON t.constraint_schema = k.constraint_schema
		 AND t.constraint_name = k.constraint_name
		 AND t.table_name = k.table_name
		WHERE t.constraint_type = 'PRIMARY KEY'
		  AND k.table_schema = DATABASE()
		  AND k.table_name = ?
		ORDER BY k.ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// Status implements sql.Dialect: it decodes the binlog checkpoint into a
// point-in-time IngestableStatus (phase, per-table snapshot progress, and the
// binlog coordinate as "file:pos"). Lag is left nil: a MySQL binlog
// file+position has no single-number byte distance from the source write head
// the way a Postgres LSN does, so source-lag for MySQL is a deliberate
// follow-on (see the ingest-status-endpoint ticket). With Lag nil, CaughtUp is
// never true — an unknown lag is not a caught-up lag.
func (m *MySQLDialect) Status(_ context.Context, config *sql.Config, pos cluster.Position) (cluster.IngestableStatus, error) {
	var progress *dialectpb.SnapshotProgress
	var position string
	if len(pos) > 0 {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return cluster.IngestableStatus{}, fmt.Errorf("[mysql.status] decode position: %w", err)
		}
		progress = posProto.SnapshotProgress
		if posProto.Name != "" {
			position = fmt.Sprintf("%s:%d", posProto.Name, posProto.Pos)
		}
	}

	status := cluster.IngestableStatus{
		Position:         position,
		SnapshotProgress: sql.SnapshotTableStatus(config, progress),
	}
	if progress != nil {
		status.Phase = "snapshot"
	} else {
		status.Phase = "streaming"
	}
	return status, nil
}

// SourceColumns implements sql.Dialect: it introspects each watched table's
// columns (in ordinal order) so the parser can expand a MapAllColumns config
// into explicit mappings. Read-only; one short-lived connection.
func (m *MySQLDialect) SourceColumns(config *sql.Config) (map[string][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	out := make(map[string][]string, len(config.Tables))
	for _, table := range config.Tables {
		cols, err := mysqlTableColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("source table %q has no columns (does it exist?)", table)
		}
		out[table] = cols
	}
	return out, nil
}

// mysqlTableColumns returns a table's columns in ordinal order, from the
// connection's current database.
func mysqlTableColumns(ctx context.Context, db *gosql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = DATABASE()
		  AND table_name = ?
		ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	backoff := syncerBackoffMin

	// Parse the initial resume position, if any. snapshot_progress
	// being non-nil means a prior run was interrupted mid-snapshot
	// and we should resume from where it left off.
	var lastPos *mysql.Position
	var lastGTID string
	var resumeProgress *dialectpb.SnapshotProgress
	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return err
		}
		lastPos = &mysql.Position{Name: posProto.Name, Pos: posProto.Pos}
		lastGTID = posProto.GtidSet
		resumeProgress = posProto.SnapshotProgress
	}

	// Schema cache for the streaming decode path — committed sources column
	// metadata (names, JSON category, enum/set labels) from information_schema
	// rather than canal's tracking. Opened once and shared across reconnects;
	// the connection is lazy, so a bad source surfaces on the first row, not here.
	cacheDB, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
	if err != nil {
		return fmt.Errorf("open schema cache db: %w", err)
	}
	defer func() { _ = cacheDB.Close() }()
	cache := newSchemaCache(cacheDB)

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
		if lastPos == nil || resumeProgress != nil {
			snapshotPos, snapshotGTID, err := snapshot(ctx, config, pr, po, lastPos, lastGTID, resumeProgress)
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

			// Checkpoint the final snapshot position (no
			// snapshot_progress) so a restart after snapshot
			// completion but before the first binlog commit
			// starts streaming instead of re-running snapshot.
			posProto := &dialectpb.MySQLBinLogPosition{Name: lastPos.Name, Pos: lastPos.Pos, GtidSet: snapshotGTID}
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
		for {
			syncer = replication.NewBinlogSyncer(cfg)
			if gtidSet != nil && !gtidSet.IsEmpty() {
				streamer, err = syncer.StartSyncGTID(gtidSet)
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

		// --- set up the handler and run the stream ---
		handler := &MySQLEventHandler{
			config:       config,
			proposalChan: pr,
			positionChan: po,
			tables:       config.Tables,
			cache:        cache,
			// Seed the live coordinate from the resume position so a
			// mid-transaction flush before the first commit still stamps
			// a sane SourceSeq. lastPos is non-nil here (resume or
			// snapshot-derived).
			curFile: lastPos.Name,
			curPos:  lastPos.Pos,
		}
		// Seed the consumed GTID set (the same set we resumed by) so streaming
		// checkpoints carry the full set (snapshot ∪ streamed) and it keeps advancing
		// across reconnects. Clone so the handler's in-place Update never mutates the
		// set handed to StartSyncGTID. Empty (file:pos-only / gtid_mode=OFF) leaves
		// consumedGTID nil.
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
		if isGtidPurged(streamErr) {
			// The source purged binlogs past our consumed GTID set (error 1236) —
			// resume can't continue. Recover by re-snapshotting from the current
			// source state (the data re-applies idempotently downstream). Loud and
			// explicit, never a silent gap; retrying the same GTID would just loop.
			zap.L().Error("binlog purged past consumed position (error 1236) — re-snapshotting",
				zap.String("consumed_gtid", lastGTID),
				zap.Error(streamErr),
			)
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
	tables       []string

	// cache supplies per-column decode metadata (names, JSON category, enum/set
	// labels) from committed's own information_schema lookups — what the raw
	// binlog row lacks. Shared across reconnects; cleared on DDL.
	cache *schemaCache

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

	// pending accumulates entities from row events until the transaction commits
	// (an XID event), so one MySQL transaction maps to one cluster.Proposal.
	pending []*cluster.Entity

	// consumedGTID is the set of transactions fully processed and checkpointed —
	// the GTID resume cursor (Phase B). curTxnGTID holds the in-flight
	// transaction's GTID between its GTIDEvent and its commit, when it is merged
	// into consumedGTID. Both stay nil until the first GTIDEvent, so a file:pos-only
	// or gtid_mode=OFF source leaves the checkpoint GTID-free.
	consumedGTID mysql.GTIDSet
	curTxnGTID   mysql.GTIDSet
}

// mysqlCategoryForTypeName maps a MySQL type name (information_schema data_type
// for the binlog decode path; database/sql DatabaseTypeName for the snapshot
// path) to a JSON category. MySQL has no native bool — a tinyint(1) is a number —
// so there is no bool category.
func mysqlCategoryForTypeName(name string) sql.JSONCategory {
	switch strings.ToUpper(name) {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "YEAR",
		"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC":
		return sql.CatNumber
	case "JSON":
		return sql.CatJSON
	}
	return sql.CatText
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
func (h *MySQLEventHandler) handleRows(ctx context.Context, header *replication.EventHeader, e *replication.RowsEvent) error {
	table := string(e.Table.Table)
	if !slices.Contains(h.tables, strings.ToLower(table)) {
		return nil
	}
	action, ok := rowsAction(e.Type())
	if !ok {
		return nil // an unsupported rows-event variant — skip
	}

	ts, err := h.cache.get(ctx, table)
	if err != nil {
		return fmt.Errorf("schema of %q: %w", table, err)
	}

	// m holds the key form: coerce []byte (TEXT/BLOB/JSON) to string so the key is
	// text and matches the snapshot path. raw/catByName keep the value and its
	// category so the payload renders as natural JSON.
	m := make(map[string]any)
	raw := make(map[string]any)
	catByName := make(map[string]sql.JSONCategory)
	row := e.Rows[len(e.Rows)-1]
	for i, col := range ts.cols {
		if i >= len(row) {
			break // row image has fewer columns than the current schema (DDL skew)
		}
		val := decodeEnumSet(col, row[i])
		raw[col.name] = val
		catByName[col.name] = col.cat
		if b, ok := val.([]byte); ok {
			m[col.name] = string(b)
		} else {
			m[col.name] = val
		}
	}

	toJSON := make(map[string]any)
	for _, mapping := range h.config.Mappings {
		toJSON[mapping.JsonName] = sql.JSONValue(raw[mapping.SQLColumn], catByName[mapping.SQLColumn])
	}

	jsonString, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("handleRows: skipping row with unmarshalable data",
			zap.String("table", table),
			zap.Error(err),
		)
		return nil
	}

	key := sql.CompositeKey(m, h.config.PrimaryKey)

	// A source DELETE becomes a delete (tombstone) entity keyed by the row's
	// primary key. INSERT and UPDATE emit upserts of the post-image — the last
	// row of the event, which for an UPDATE is the after-image of the last pair.
	var entity *cluster.Entity
	if action == "delete" {
		entity = cluster.NewDeleteEntity(h.config.Type, []byte(key))
	} else {
		entity = &cluster.Entity{
			Type: h.config.Type,
			Key:  []byte(key),
			Data: []byte(jsonString),
		}
	}

	// Buffer until the transaction commits. Track the live offset (the row
	// event's end position) so a mid-transaction soft-limit flush stamps a
	// monotonic SourceSeq.
	h.pending = append(h.pending, entity)
	if header != nil {
		h.curPos = header.LogPos
	}

	// Soft limit: emit a partial batch to prevent OOM on a very large transaction.
	if len(h.pending) >= maxPendingEntities {
		if err := h.flushPending(ctx); err != nil {
			return err
		}
	}

	zap.L().Debug("handleRows", zap.String("table", table), zap.String("action", action))
	return nil
}

// flushPending emits all buffered entities as a single proposal and resets the
// buffer. No-op when the buffer is empty.
func (h *MySQLEventHandler) flushPending(ctx context.Context) error {
	if len(h.pending) == 0 {
		return nil
	}
	p := &cluster.Proposal{Entities: h.pending, SourceSeq: encodeSourceSeq(h.curFile, h.curPos)}
	h.pending = nil

	select {
	case h.proposalChan <- p:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

// handleXID handles a transaction commit (XID event): it stamps the post-commit
// coordinate, flushes the buffered entities as one proposal, and checkpoints the
// position so a restart resumes past a fully-committed transaction. The commit
// file is the live curFile (tracked from rotation); the offset is the XID event's
// end position.
func (h *MySQLEventHandler) handleXID(ctx context.Context, header *replication.EventHeader) error {
	if header != nil {
		h.curPos = header.LogPos
	}
	pos := mysql.Position{Name: h.curFile, Pos: h.curPos}

	if err := h.flushPending(ctx); err != nil {
		return err
	}

	// Merge the just-committed transaction's GTID into the consumed set so the
	// checkpoint carries the GTID resume cursor alongside file:pos. Resume still
	// uses file:pos until the cutover slice — the GTID is written, not yet read.
	merged, err := mergeGTID(h.consumedGTID, h.curTxnGTID)
	if err != nil {
		return fmt.Errorf("merge committed GTID: %w", err)
	}
	h.consumedGTID = merged
	h.curTxnGTID = nil

	posProto := &dialectpb.MySQLBinLogPosition{Name: pos.Name, Pos: pos.Pos}
	if h.consumedGTID != nil {
		posProto.GtidSet = h.consumedGTID.String()
	}
	bs, err := proto.Marshal(posProto)
	if err != nil {
		return err
	}

	select {
	case h.positionChan <- bs:
	case <-ctx.Done():
		return ctx.Err()
	}

	h.lastPos = &mysql.Position{Name: pos.Name, Pos: pos.Pos}
	zap.L().Debug("handleXID", zap.String("pos", pos.Name), zap.Uint32("offset", pos.Pos))
	return nil
}

// handleDDL drops the cached schema on any DDL — a column change on a watched
// table must be re-read before the next row decodes — and logs the statement.
func (h *MySQLEventHandler) handleDDL(e *replication.QueryEvent) {
	h.cache.clear()
	if e == nil {
		return
	}
	zap.L().Warn("handleDDL: DDL event received",
		zap.String("schema", string(e.Schema)),
		zap.String("query", string(e.Query)),
	)
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

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			// A real rotation moves curFile so subsequent commits checkpoint
			// against the right file; the start-of-stream fake rotate that only
			// restates the current file is ignored.
			if isSkippableFakeRotate(ev.Header.Timestamp, string(e.NextLogName), h.curFile) {
				continue
			}
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
			if err := h.handleRows(ctx, ev.Header, e); err != nil {
				return err
			}
		case *replication.XIDEvent:
			if err := h.handleXID(ctx, ev.Header); err != nil {
				return err
			}
		case *replication.QueryEvent:
			h.handleDDL(e)
		}
	}
}

// encodeSourceSeq maps a binlog coordinate (file, offset) to a
// strictly-monotonic uint64 used as a proposal's SourceSeq for
// effectively-once dedup. The file's numeric suffix occupies the high 32
// bits and the offset the low 32 bits, so ordering matches binlog order:
// a later file always outranks an earlier one regardless of offset, and
// within a file the offset orders. Returns 0 (which disables dedup for
// the proposal — never a false positive) when the file name has no
// parseable numeric suffix, e.g. an unexpected naming scheme.
func encodeSourceSeq(name string, pos uint32) uint64 {
	dot := strings.LastIndexByte(name, '.')
	if dot < 0 || dot == len(name)-1 {
		return 0
	}
	fileNum, err := strconv.ParseUint(name[dot+1:], 10, 32)
	if err != nil {
		return 0
	}
	return fileNum<<32 | uint64(pos)
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
) (*mysql.Position, string, error) {
	db, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
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
	}

	batchSize := parseBatchSize(config.Options)

	// Build the initial progress map from the resume checkpoint.
	progress := &dialectpb.SnapshotProgress{
		LastPkByTable:   map[string]string{},
		CompletedTables: nil,
	}
	completed := map[string]bool{}
	if resumeProgress != nil {
		maps.Copy(progress.LastPkByTable, resumeProgress.LastPkByTable)
		progress.CompletedTables = append(progress.CompletedTables, resumeProgress.CompletedTables...)
		for _, t := range resumeProgress.CompletedTables {
			completed[t] = true
		}
	}

	for _, table := range config.Tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table",
				zap.String("table", table),
			)
			continue
		}
		if err := snapshotTable(ctx, db, config, table, batchSize, progress, &pos, gtid, pr, po); err != nil {
			return nil, "", fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		// Mark complete and drop any partial cursor.
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		completed[table] = true

		if err := emitProgress(ctx, po, pos, gtid, progress); err != nil {
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
) error {
	// A shallow copy is enough — proto.Marshal will serialize the
	// current state of the maps/slices at call time.
	posProto := &dialectpb.MySQLBinLogPosition{
		Name:             pos.Name,
		Pos:              pos.Pos,
		GtidSet:          gtid,
		SnapshotProgress: progress,
	}
	bs, err := proto.Marshal(posProto)
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
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	pkCols := config.PrimaryKey
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

		rows, lastKey, count, err := readBatch(ctx, db, config, table, pkCols, lastPK, haveLastPK, batchSize)
		if err != nil {
			return err
		}
		if count == 0 {
			// Empty table, or we've advanced past the last row.
			break
		}

		p := &cluster.Proposal{Entities: rows}
		select {
		case pr <- p:
		case <-ctx.Done():
			return ctx.Err()
		}

		lastPK = lastKey
		haveLastPK = true
		progress.LastPkByTable[table] = lastPK
		totalRows += count

		if err := emitProgress(ctx, po, *pos, gtid, progress); err != nil {
			return err
		}

		zap.L().Info("snapshot: batch flushed",
			zap.String("table", table),
			zap.Int("batch", batchNum),
			zap.Int("rows_in_batch", count),
			zap.Int("rows_total", totalRows),
			zap.String("last_pk", lastPK),
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
	config *sql.Config,
	table string,
	pkCols []string,
	lastPK string,
	haveLastPK bool,
	batchSize int,
) ([]*cluster.Entity, string, int, error) {
	tx, err := db.BeginTx(ctx, &gosql.TxOptions{
		Isolation: gosql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, "", 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Keyset pagination ordered by the full PK. A single column is `c > ?`; a
	// composite is the row-value comparison `(c1, c2) > (?, ?)` — MySQL coerces
	// the bound string cursor values to each column's type. The cursor is the
	// prior batch's last entity key (CompositeKey), decoded to per-column
	// values here.
	orderCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		orderCols[i] = fmt.Sprintf("`%s` ASC", c)
	}
	orderBy := strings.Join(orderCols, ", ")

	var query string
	var args []any
	if haveLastPK {
		cursor, derr := sql.DecodeCompositeCursor(lastPK, len(pkCols))
		if derr != nil {
			return nil, "", 0, derr
		}
		cols := make([]string, len(pkCols))
		placeholders := make([]string, len(pkCols))
		args = make([]any, len(pkCols))
		for i, c := range pkCols {
			cols[i] = fmt.Sprintf("`%s`", c)
			placeholders[i] = "?"
			args[i] = cursor[i]
		}
		query = fmt.Sprintf(
			"SELECT * FROM `%s` WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
			table, strings.Join(cols, ", "), strings.Join(placeholders, ", "), orderBy, batchSize,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT * FROM `%s` ORDER BY %s LIMIT %d",
			table, orderBy, batchSize,
		)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
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
	for i, ct := range colTypes {
		cats[i] = mysqlCategoryForTypeName(ct.DatabaseTypeName())
	}

	var entities []*cluster.Entity
	var batchLastPK string

	for rows.Next() {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, "", 0, err
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

		toJSON := make(map[string]any)
		for _, mapping := range config.Mappings {
			toJSON[mapping.JsonName] = sql.JSONValue(raw[mapping.SQLColumn], catByName[mapping.SQLColumn])
		}

		jsonBytes, err := json.Marshal(toJSON)
		if err != nil {
			zap.L().Warn("readBatch: skipping row",
				zap.String("table", table),
				zap.Error(err),
			)
			continue
		}

		key := sql.CompositeKey(m, config.PrimaryKey)
		batchLastPK = key

		entities = append(entities, &cluster.Entity{
			Type: config.Type,
			Key:  []byte(key),
			Data: jsonBytes,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, "", 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, "", 0, err
	}

	return entities, batchLastPK, len(entities), nil
}

// buildDSN converts a mysql:// URL to a go-sql-driver/mysql DSN string.
func buildDSN(connectionString string) string {
	u, err := url.Parse(connectionString)
	if err != nil {
		return connectionString
	}

	username := u.User.Username()
	password, _ := u.User.Password()
	database := strings.TrimPrefix(u.Path, "/")

	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, u.Host, database)
}
