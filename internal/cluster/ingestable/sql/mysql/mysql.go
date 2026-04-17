package mysql

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql/dialectpb"
	"github.com/siddontang/go-log/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type MySQLDialect struct{}

// canalBackoff{Min,Max} bound the retry interval for createCanal.
// A transient MySQL outage (DNS blip, container restart, network
// partition) at Ingest startup used to immediately return an error,
// leaving the worker leader-but-not-ingesting with no recovery path.
// The retry loop below caps at Max and is bounded by ctx so a
// shutdown still propagates promptly.
const (
	canalBackoffMin = 1 * time.Second
	canalBackoffMax = 30 * time.Second

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

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	backoff := canalBackoffMin

	// Parse the initial resume position, if any. snapshot_progress
	// being non-nil means a prior run was interrupted mid-snapshot
	// and we should resume from where it left off.
	var lastPos *mysql.Position
	var resumeProgress *dialectpb.SnapshotProgress
	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return err
		}
		lastPos = &mysql.Position{Name: posProto.Name, Pos: posProto.Pos}
		resumeProgress = posProto.SnapshotProgress
	}

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
			snapshotPos, err := snapshot(ctx, config, pr, po, lastPos, resumeProgress)
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
				if backoff > canalBackoffMax {
					backoff = canalBackoffMax
				}
				continue
			}
			lastPos = snapshotPos
			resumeProgress = nil
			backoff = canalBackoffMin
			zap.L().Info("snapshot complete",
				zap.String("binlog_file", lastPos.Name),
				zap.Uint32("binlog_pos", lastPos.Pos),
			)

			// Checkpoint the final snapshot position (no
			// snapshot_progress) so a restart after snapshot
			// completion but before the first binlog commit
			// starts streaming instead of re-running snapshot.
			posProto := &dialectpb.MySQLBinLogPosition{Name: lastPos.Name, Pos: lastPos.Pos}
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

		// --- create canal with retry ---
		var c *canal.Canal
		var tables []string
		for {
			var err error
			c, tables, err = createCanal(config)
			if err == nil {
				backoff = canalBackoffMin
				break
			}

			zap.L().Warn("createCanal failed, retrying",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > canalBackoffMax {
				backoff = canalBackoffMax
			}
		}

		// --- set up handler and start the canal goroutine ---
		handler := &MySQLEventHandler{
			ctx:          ctx,
			config:       config,
			canal:        c,
			proposalChan: pr,
			positionChan: po,
			tables:       tables,
		}

		c.SetEventHandler(handler)

		// Always start from a known position — the snapshot or a
		// previously checkpointed position. canal's built-in
		// mysqldump is never used.
		canalDone := make(chan error, 1)
		go func() { canalDone <- c.RunFrom(*lastPos) }()

		// --- wait for canal exit or context cancel ---
		select {
		case <-ctx.Done():
			c.Close()
			return nil
		case err := <-canalDone:
			// Canal exited on its own — connection lost, fatal
			// binlog error, or an OnRow error. Capture the last
			// emitted position before closing so the next iteration
			// resumes correctly.
			if handler.lastPos != nil {
				lastPos = handler.lastPos
			}

			zap.L().Warn("canal exited, will reconnect",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)
			c.Close()
		}

		// --- backoff before reconnect ---
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > canalBackoffMax {
			backoff = canalBackoffMax
		}
	}
}

func (m *MySQLDialect) Close() error {
	return nil
}

type MySQLEventHandler struct {
	canal.DummyEventHandler
	// ctx is the Ingest caller's context. The OnRow / OnPosSynced
	// callbacks use it to abort their channel sends if the worker is
	// canceled mid-emit, otherwise the canal goroutine would leak
	// blocked on a no-receiver channel.
	//
	// Storing context.Context on a struct is an anti-pattern that
	// vet flags by default — Go's guidance is to pass ctx as the
	// first function argument so it's never long-lived state. We
	// accept the deviation here because the canal.EventHandler
	// interface (defined upstream in go-mysql-org/go-mysql) does not
	// take ctx in OnRow / OnPosSynced and we have no other way to
	// thread cancellation into those callbacks. The handler's lifetime
	// is exactly one Ingest call, so the stored ctx isn't long-lived
	// across calls and the usual "stale ctx" hazard doesn't apply.
	ctx          context.Context
	config       *sql.Config
	canal        *canal.Canal
	proposalChan chan<- *cluster.Proposal
	positionChan chan<- cluster.Position
	tables       []string

	// lastPos holds the most recently emitted binlog position so the
	// outer reconnect loop can resume from where it left off instead
	// of re-dumping from scratch or replaying from the original pos.
	lastPos *mysql.Position

	// pending accumulates entities from OnRow calls until the
	// transaction commits (OnXID). This ensures one MySQL transaction
	// maps to one cluster.Proposal, giving consumers atomic delivery
	// and enabling exactly-once semantics via post-commit position
	// checkpointing.
	pending []*cluster.Entity
}

func (h *MySQLEventHandler) OnRow(e *canal.RowsEvent) error {
	if !slices.Contains(h.tables, strings.ToLower(e.Table.Name)) {
		return nil
	}

	m := make(map[string]any)
	row := e.Rows[len(e.Rows)-1]
	for i, c := range e.Table.Columns {
		val := row[i]
		// The MySQL binlog stream delivers TEXT/BLOB columns as []byte
		// while VARCHAR columns come through as string. Coerce []byte
		// to string here so json.Marshal produces text rather than
		// base64 (json.Marshal of a []byte field base64-encodes it,
		// which would turn "one" into "b25l"). Binary BLOB columns
		// are not a concern for any current caller of this dialect.
		if b, ok := val.([]byte); ok {
			val = string(b)
		}
		m[strings.ToLower(c.Name)] = val
	}

	toJSON := make(map[string]any)
	for _, mapping := range h.config.Mappings {
		value := m[mapping.SQLColumn]
		toJSON[mapping.JsonName] = value
	}

	jsonString, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("OnRow: skipping row with unmarshalable data",
			zap.String("table", e.Table.Name),
			zap.Error(err),
		)
		return nil
	}

	primaryKey := h.config.PrimaryKey
	key := fmt.Sprintf("%v", m[primaryKey])

	// Buffer the entity until the transaction commits (OnXID). Stamp
	// the propose-time wall-clock so apply on every node records the
	// same time-series timestamp. See cluster.Entity.Timestamp.
	h.pending = append(h.pending, &cluster.Entity{
		Type:      h.config.Type,
		Key:       []byte(key),
		Data:      []byte(jsonString),
		Timestamp: time.Now().UnixMilli(),
	})

	// Soft limit: emit a partial batch to prevent OOM on very large
	// transactions. This breaks atomicity for oversized transactions
	// but is the right trade-off versus unbounded memory growth.
	if len(h.pending) >= maxPendingEntities {
		if err := h.flushPending(); err != nil {
			return err
		}
	}

	zap.L().Debug("OnRow", zap.String("table", e.Table.Name), zap.String("action", e.Action))
	return nil
}

// flushPending emits all buffered entities as a single proposal and
// resets the buffer. No-op when the buffer is empty.
func (h *MySQLEventHandler) flushPending() error {
	if len(h.pending) == 0 {
		return nil
	}
	p := &cluster.Proposal{Entities: h.pending}
	select {
	case h.proposalChan <- p:
	case <-h.ctx.Done():
		return h.ctx.Err()
	}
	h.pending = nil
	return nil
}

// OnXID is called when a MySQL transaction commits. It flushes all
// buffered entities as a single proposal and checkpoints the post-commit
// binlog position. This ensures one MySQL transaction maps to one
// cluster.Proposal and that the checkpoint position is always past a
// fully committed transaction — no duplicates or gaps on restart.
func (h *MySQLEventHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	if err := h.flushPending(); err != nil {
		return err
	}

	posProto := &dialectpb.MySQLBinLogPosition{Name: pos.Name, Pos: pos.Pos}
	bs, err := proto.Marshal(posProto)
	if err != nil {
		return err
	}

	select {
	case h.positionChan <- bs:
	case <-h.ctx.Done():
		return h.ctx.Err()
	}

	h.lastPos = &mysql.Position{Name: pos.Name, Pos: pos.Pos}

	zap.L().Debug("OnXID", zap.String("pos", pos.Name), zap.Uint32("offset", pos.Pos))
	return nil
}

func (h *MySQLEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// During binlog replication OnXID already flushed pending entities
	// before this callback is invoked, so pending will normally be
	// empty. Flush as a safety measure.
	return h.flushPending()
}

func (h *MySQLEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if queryEvent == nil {
		zap.L().Warn("OnDDL: DDL event with nil query",
			zap.String("pos", nextPos.Name),
			zap.Uint32("offset", nextPos.Pos),
		)
		return nil
	}
	zap.L().Warn("OnDDL: DDL event received",
		zap.String("schema", string(queryEvent.Schema)),
		zap.String("query", string(queryEvent.Query)),
		zap.String("pos", nextPos.Name),
		zap.Uint32("offset", nextPos.Pos),
	)
	return nil
}

func (h *MySQLEventHandler) String() string {
	return "MyEventHandler"
}

func createCanal(config *sql.Config) (*canal.Canal, []string, error) {
	u, err := url.Parse(config.ConnectionString)
	if err != nil {
		return nil, nil, err
	}

	username := u.User.Username()
	password, passwordExists := u.User.Password()

	tables := config.Tables

	cfg := canal.NewDefaultConfig()
	cfg.Addr = u.Host
	cfg.User = username
	if passwordExists {
		cfg.Password = password
	}
	// Dump config is intentionally omitted — the pure-SQL snapshot
	// replaces canal's built-in mysqldump phase.
	streamHandler, err := log.NewStreamHandler(os.Stdout)
	if err != nil {
		return nil, nil, err
	}
	logger := log.NewDefault(streamHandler)
	logger.SetLevel(log.LevelError)
	cfg.Logger = logger

	canal, err := canal.NewCanal(cfg)

	return canal, tables, err
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
	resumeProgress *dialectpb.SnapshotProgress,
) (*mysql.Position, error) {
	db, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
	if err != nil {
		return nil, fmt.Errorf("snapshot: open: %w", err)
	}
	defer db.Close()

	// Determine the binlog position to resume from. On a fresh start
	// we capture it now under a brief global lock; on resume we keep
	// the saved position so the binlog tail covers the pre-snapshot
	// window.
	var pos mysql.Position
	if resumePos != nil {
		pos = *resumePos
	} else {
		pos, err = captureBinlogPosition(ctx, db)
		if err != nil {
			return nil, err
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
		if err := snapshotTable(ctx, db, config, table, batchSize, progress, &pos, pr, po); err != nil {
			return nil, fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		// Mark complete and drop any partial cursor.
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		completed[table] = true

		if err := emitProgress(ctx, po, pos, progress); err != nil {
			return nil, err
		}
		zap.L().Info("snapshot: table complete", zap.String("table", table))
	}

	return &pos, nil
}

// captureBinlogPosition briefly acquires a global read lock to read the
// current binlog file and offset. The lock is released immediately; it
// only needs to be held long enough to sample a consistent position.
func captureBinlogPosition(ctx context.Context, db *gosql.DB) (mysql.Position, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("snapshot: conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return mysql.Position{}, fmt.Errorf("snapshot: FLUSH TABLES WITH READ LOCK: %w", err)
	}
	pos, err := binlogStatus(ctx, conn)
	// Always release the lock, regardless of binlogStatus success.
	if _, unlockErr := conn.ExecContext(ctx, "UNLOCK TABLES"); unlockErr != nil && err == nil {
		err = unlockErr
	}
	if err != nil {
		return mysql.Position{}, fmt.Errorf("snapshot: binlog status: %w", err)
	}
	return pos, nil
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
	progress *dialectpb.SnapshotProgress,
) error {
	// A shallow copy is enough — proto.Marshal will serialize the
	// current state of the maps/slices at call time.
	posProto := &dialectpb.MySQLBinLogPosition{
		Name:             pos.Name,
		Pos:              pos.Pos,
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
func binlogStatus(ctx context.Context, conn *gosql.Conn) (mysql.Position, error) {
	for _, query := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			continue
		}

		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}

		if !rows.Next() {
			rows.Close()
			continue
		}

		// Scan the first two columns (File, Position) into typed
		// destinations; discard the rest. database/sql handles the
		// int64→uint32 conversion for Position automatically.
		var file string
		var pos uint32
		dest := make([]any, len(cols))
		dest[0] = &file
		dest[1] = &pos
		for i := 2; i < len(cols); i++ {
			dest[i] = new(any)
		}

		if err := rows.Scan(dest...); err != nil {
			rows.Close()
			continue
		}
		rows.Close()

		return mysql.Position{Name: file, Pos: pos}, nil
	}

	return mysql.Position{}, fmt.Errorf("binlogStatus: could not determine binlog position")
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
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	pkCol := config.PrimaryKey
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

		rows, lastKey, count, err := readBatch(ctx, db, config, table, pkCol, lastPK, haveLastPK, batchSize)
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

		if err := emitProgress(ctx, po, *pos, progress); err != nil {
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
	pkCol string,
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
	defer tx.Rollback()

	var query string
	var args []any
	if haveLastPK {
		query = fmt.Sprintf(
			"SELECT * FROM `%s` WHERE `%s` > ? ORDER BY `%s` ASC LIMIT %d",
			table, pkCol, pkCol, batchSize,
		)
		args = []any{lastPK}
	} else {
		query = fmt.Sprintf(
			"SELECT * FROM `%s` ORDER BY `%s` ASC LIMIT %d",
			table, pkCol, batchSize,
		)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, "", 0, err
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

		m := make(map[string]any)
		for i, colName := range columns {
			v := vals[i]
			if v == nil {
				m[strings.ToLower(colName)] = nil
			} else if b, ok := v.([]byte); ok {
				m[strings.ToLower(colName)] = string(b)
			} else {
				m[strings.ToLower(colName)] = fmt.Sprintf("%v", v)
			}
		}

		toJSON := make(map[string]any)
		for _, mapping := range config.Mappings {
			toJSON[mapping.JsonName] = m[mapping.SQLColumn]
		}

		jsonBytes, err := json.Marshal(toJSON)
		if err != nil {
			zap.L().Warn("readBatch: skipping row",
				zap.String("table", table),
				zap.Error(err),
			)
			continue
		}

		key := fmt.Sprintf("%v", m[config.PrimaryKey])
		batchLastPK = key

		entities = append(entities, &cluster.Entity{
			Type:      config.Type,
			Key:       []byte(key),
			Data:      jsonBytes,
			Timestamp: time.Now().UnixMilli(),
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

