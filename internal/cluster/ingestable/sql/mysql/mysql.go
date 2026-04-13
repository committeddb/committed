package mysql

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
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
)

func (m *MySQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	backoff := canalBackoffMin

	// Parse the initial resume position, if any.
	var lastPos *mysql.Position
	if pos != nil {
		posProto := &dialectpb.MySQLBinLogPosition{}
		if err := proto.Unmarshal(pos, posProto); err != nil {
			return err
		}
		lastPos = &mysql.Position{Name: posProto.Name, Pos: posProto.Pos}
	}

	// Outer loop: each iteration either snapshots (first run) or
	// creates a canal, runs it until it exits, then reconnects with
	// backoff. Only ctx cancellation breaks out.
	for {
		// --- snapshot on first run ---
		// When there is no saved position, perform a pure-SQL initial
		// snapshot to capture existing data and determine the binlog
		// position to stream from. This replaces canal's built-in
		// mysqldump phase, eliminating the external binary dependency.
		if lastPos == nil {
			snapshotPos, err := snapshot(ctx, config, pr)
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
			backoff = canalBackoffMin
			zap.L().Info("snapshot complete",
				zap.String("binlog_file", lastPos.Name),
				zap.Uint32("binlog_pos", lastPos.Pos),
			)

			// Checkpoint the snapshot position so a restart after
			// the snapshot but before the first binlog commit does
			// not redo the entire snapshot.
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

// snapshot performs a pure-SQL initial dump of all watched tables. It
// replaces the external mysqldump binary that canal uses internally.
//
// The approach mirrors what mysqldump --single-transaction --master-data
// does, without shelling out:
//
//  1. FLUSH TABLES WITH READ LOCK — briefly blocks all writes.
//  2. SHOW BINARY LOG STATUS — captures the binlog position.
//  3. START TRANSACTION WITH CONSISTENT SNAPSHOT — establishes the
//     InnoDB read-view at the same point as the binlog position.
//  4. UNLOCK TABLES — releases the global lock.
//  5. SELECT * FROM each table — reads consistent snapshot data.
//  6. COMMIT.
//
// The returned position is where the binlog streamer should start to
// avoid gaps or duplicates between the snapshot and streaming phases.
func snapshot(
	ctx context.Context,
	config *sql.Config,
	pr chan<- *cluster.Proposal,
) (*mysql.Position, error) {
	db, err := gosql.Open("mysql", buildDSN(config.ConnectionString))
	if err != nil {
		return nil, fmt.Errorf("snapshot: open: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("snapshot: conn: %w", err)
	}
	defer conn.Close()

	// Ensure REPEATABLE READ so WITH CONSISTENT SNAPSHOT provides
	// point-in-time consistency (MySQL defaults to REPEATABLE READ,
	// but an explicit SET guards against changed server defaults).
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, fmt.Errorf("snapshot: set isolation: %w", err)
	}

	// Acquire a global read lock so the binlog position and InnoDB
	// snapshot are consistent. The lock is held only until the
	// snapshot transaction is established (microseconds).
	if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
		return nil, fmt.Errorf("snapshot: FLUSH TABLES WITH READ LOCK: %w", err)
	}

	pos, err := binlogStatus(ctx, conn)
	if err != nil {
		conn.ExecContext(ctx, "UNLOCK TABLES")
		return nil, fmt.Errorf("snapshot: binlog status: %w", err)
	}

	// Start a consistent snapshot. The global lock is still held, so
	// the snapshot's read-view corresponds exactly to the captured
	// binlog position.
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		conn.ExecContext(ctx, "UNLOCK TABLES")
		return nil, fmt.Errorf("snapshot: START TRANSACTION: %w", err)
	}

	// Release the lock — the snapshot is established and other
	// sessions can write again.
	if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		return nil, fmt.Errorf("snapshot: UNLOCK TABLES: %w", err)
	}

	for _, table := range config.Tables {
		if err := snapshotTable(ctx, conn, config, table, pr); err != nil {
			return nil, fmt.Errorf("snapshot: table %s: %w", table, err)
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, fmt.Errorf("snapshot: COMMIT: %w", err)
	}

	return &pos, nil
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

// snapshotTable reads all rows from a table and emits them as proposals.
// Column mapping and primary-key extraction mirror the binlog streaming
// path (OnRow) so consumers see identical entity shapes.
func snapshotTable(
	ctx context.Context,
	conn *gosql.Conn,
	config *sql.Config,
	table string,
	pr chan<- *cluster.Proposal,
) error {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM `%s`", table))
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	var pending []*cluster.Entity

	for rows.Next() {
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
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
			zap.L().Warn("snapshotTable: skipping row",
				zap.String("table", table),
				zap.Error(err),
			)
			continue
		}

		key := fmt.Sprintf("%v", m[config.PrimaryKey])

		pending = append(pending, &cluster.Entity{
			Type:      config.Type,
			Key:       []byte(key),
			Data:      jsonBytes,
			Timestamp: time.Now().UnixMilli(),
		})

		if len(pending) >= maxPendingEntities {
			if err := flushPending(ctx, &pending, pr); err != nil {
				return err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return flushPending(ctx, &pending, pr)
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

// flushPending emits all buffered entities as a single proposal and
// resets the buffer. No-op when the buffer is empty. Used by the
// snapshot path; the binlog path uses MySQLEventHandler.flushPending.
func flushPending(ctx context.Context, pending *[]*cluster.Entity, pr chan<- *cluster.Proposal) error {
	if len(*pending) == 0 {
		return nil
	}
	p := &cluster.Proposal{Entities: *pending}
	select {
	case pr <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	*pending = nil
	return nil
}
