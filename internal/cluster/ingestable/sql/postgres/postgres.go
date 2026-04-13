package postgres

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/philborlin/committed/internal/cluster"
	"github.com/philborlin/committed/internal/cluster/ingestable/sql"
	"go.uber.org/zap"
)

// PostgreSQLDialect implements sql.Dialect for Postgres logical replication
// via the pgoutput plugin. It connects using the replication protocol,
// decodes INSERT/UPDATE/DELETE row events, groups them by transaction, and
// emits one cluster.Proposal per committed transaction.
type PostgreSQLDialect struct{}

const (
	backoffMin = 1 * time.Second
	backoffMax = 30 * time.Second

	// maxPendingEntities is the soft limit on buffered entities per
	// transaction. Mirrors the MySQL dialect's behavior: if a single
	// Postgres transaction modifies more rows than this, a partial
	// proposal is emitted to cap memory usage.
	maxPendingEntities = 10000

	// standbyTimeout controls how often we send standby status updates
	// to Postgres. Must be shorter than wal_sender_timeout (default 60s)
	// to prevent the server from dropping the connection.
	standbyTimeout = 10 * time.Second
)

// pgConfig holds Postgres-specific connection parameters derived from
// sql.Config. Fields are populated from Config.Options first, falling
// back to URL query params for backward compatibility.
type pgConfig struct {
	connString    string   // cleaned connection string with replication=database
	sqlConnString string   // cleaned connection string without replication param (for regular SQL)
	slotName      string   // logical replication slot name
	publication   string   // publication name
	tables        []string // tables to watch (schema-qualified, e.g. "public.orders")
}

// buildPgConfig constructs a pgConfig from the sql.Config. It reads
// slot_name and publication from Config.Options, and tables from
// Config.Tables. The connection string should be a plain Postgres URL
// with no application-level params.
func buildPgConfig(config *sql.Config) (*pgConfig, error) {
	u, err := url.Parse(config.ConnectionString)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	cfg := &pgConfig{
		tables: config.Tables,
	}

	options := config.Options
	if options == nil {
		options = map[string]string{}
	}

	cfg.slotName = options["slot_name"]
	cfg.publication = options["publication"]

	// Build the SQL connection string (no replication param).
	q.Del("replication")
	u.RawQuery = q.Encode()
	cfg.sqlConnString = u.String()

	// Build the replication connection string.
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	cfg.connString = u.String()

	if cfg.slotName == "" {
		cfg.slotName = "committed_slot"
	}
	if cfg.publication == "" {
		cfg.publication = "committed_pub"
	}

	return cfg, nil
}

func (d *PostgreSQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return err
	}

	var startLSN pglogrepl.LSN
	if len(pos) == 8 {
		startLSN = pglogrepl.LSN(binary.BigEndian.Uint64(pos))
	}

	backoff := backoffMin

	for {
		err := d.stream(ctx, config, pgCfg, &startLSN, pr, po)
		if ctx.Err() != nil {
			return nil
		}

		zap.L().Warn("postgres replication stream exited, will reconnect",
			zap.Duration("backoff", backoff),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// stream runs one replication session. It connects, ensures the publication
// and slot exist, starts streaming, and processes messages until the
// connection breaks or ctx is canceled. On commit boundaries it updates
// *lastLSN so the outer retry loop can resume from the correct position.
func (d *PostgreSQLDialect) stream(
	ctx context.Context,
	config *sql.Config,
	pgCfg *pgConfig,
	lastLSN *pglogrepl.LSN,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	conn, err := pgconn.Connect(ctx, pgCfg.connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if err := ensurePublication(ctx, conn, pgCfg); err != nil {
		return err
	}

	// Create the replication slot if it doesn't already exist.
	// When newly created (not resuming), the result contains a
	// SnapshotName we can use to dump pre-existing rows.
	slotResult, err := pglogrepl.CreateReplicationSlot(ctx, conn, pgCfg.slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{})
	slotIsNew := err == nil
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// If the slot was just created and we're not resuming from a
	// checkpoint, snapshot existing data before starting streaming.
	if slotIsNew && *lastLSN == 0 && slotResult.SnapshotName != "" {
		if err := d.snapshot(ctx, config, pgCfg, slotResult.SnapshotName, pr); err != nil {
			return err
		}
	}

	err = pglogrepl.StartReplication(ctx, conn, pgCfg.slotName, *lastLSN,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", pgCfg.publication),
			},
		})
	if err != nil {
		return err
	}

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	var pending []*cluster.Entity
	clientXLogPos := *lastLSN
	nextStandby := time.Now().Add(standbyTimeout)

	for {
		rawMsg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return err
			}
			if pkm.ReplyRequested || time.Now().After(nextStandby) {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
				})
				if err != nil {
					return err
				}
				nextStandby = time.Now().Add(standbyTimeout)
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return err
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return err
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m

			case *pglogrepl.BeginMessage:
				// Transaction start — pending should already be empty.

			case *pglogrepl.InsertMessage:
				if e := tupleToEntity(m.Tuple, m.RelationID, relations, config, pgCfg); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr); err != nil {
						return err
					}
				}

			case *pglogrepl.UpdateMessage:
				if e := tupleToEntity(m.NewTuple, m.RelationID, relations, config, pgCfg); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr); err != nil {
						return err
					}
				}

			case *pglogrepl.DeleteMessage:
				// With REPLICA IDENTITY DEFAULT only the primary key
				// columns are available in the old tuple. Non-key
				// columns will be null in the resulting JSON.
				if e := tupleToEntity(m.OldTuple, m.RelationID, relations, config, pgCfg); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr); err != nil {
						return err
					}
				}

			case *pglogrepl.CommitMessage:
				if err := flushPending(ctx, &pending, pr); err != nil {
					return err
				}

				// Use TransactionEndLSN (past the end of the
				// transaction) so a resume from this position does
				// not replay the already-processed transaction.
				endLSN := m.TransactionEndLSN
				var lsnBytes [8]byte
				binary.BigEndian.PutUint64(lsnBytes[:], uint64(endLSN))

				select {
				case po <- lsnBytes[:]:
				case <-ctx.Done():
					return nil
				}

				clientXLogPos = endLSN
				*lastLSN = endLSN

				// Acknowledge the commit position to Postgres.
				if time.Now().After(nextStandby) {
					err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: clientXLogPos,
					})
					if err != nil {
						return err
					}
					nextStandby = time.Now().Add(standbyTimeout)
				}

			case *pglogrepl.TruncateMessage:
				zap.L().Warn("TruncateMessage received, ignoring")
			}

			// Advance the WAL position for standby feedback.
			if xld.WALStart > 0 {
				walEnd := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				if walEnd > clientXLogPos {
					clientXLogPos = walEnd
				}
			}
		}
	}
}

// quoteIdent double-quotes a PostgreSQL identifier to safely handle
// special characters (hyphens, spaces, etc.) in names.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ensurePublication creates the publication if it does not already exist.
// The connection must be in replication=database mode which allows SQL.
func ensurePublication(ctx context.Context, conn *pgconn.PgConn, pgCfg *pgConfig) error {
	quoted := make([]string, len(pgCfg.tables))
	for i, t := range pgCfg.tables {
		quoted[i] = quoteIdent(t)
	}
	tableList := strings.Join(quoted, ", ")
	query := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdent(pgCfg.publication), tableList)
	result := conn.Exec(ctx, query)
	_, err := result.ReadAll()
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil
		}
		return err
	}
	return nil
}

// tupleToEntity converts a pgoutput tuple into a cluster.Entity using the
// column mappings from the sql.Config. Returns nil if the tuple is nil or
// the relation is not in the watched table list.
func tupleToEntity(
	tuple *pglogrepl.TupleData,
	relationID uint32,
	relations map[uint32]*pglogrepl.RelationMessage,
	config *sql.Config,
	pgCfg *pgConfig,
) *cluster.Entity {
	if tuple == nil {
		return nil
	}

	rel, ok := relations[relationID]
	if !ok {
		zap.L().Warn("tupleToEntity: unknown relation ID", zap.Uint32("relationID", relationID))
		return nil
	}

	// Filter by watched tables.
	tableName := strings.ToLower(rel.RelationName)
	qualifiedName := strings.ToLower(rel.Namespace + "." + rel.RelationName)
	found := false
	for _, t := range pgCfg.tables {
		tLower := strings.ToLower(t)
		if tLower == tableName || tLower == qualifiedName {
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	// Build column name → value map from the tuple.
	m := make(map[string]any)
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := strings.ToLower(rel.Columns[i].Name)
		switch col.DataType {
		case 'n': // null
			m[colName] = nil
		case 'u': // unchanged TOASTed value — skip
		case 't': // text representation
			m[colName] = string(col.Data)
		}
	}

	// Apply column mappings.
	toJSON := make(map[string]any)
	for _, mapping := range config.Mappings {
		toJSON[mapping.JsonName] = m[mapping.SQLColumn]
	}

	jsonBytes, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("tupleToEntity: skipping row with unmarshalable data",
			zap.String("table", rel.RelationName),
			zap.Error(err),
		)
		return nil
	}

	key := fmt.Sprintf("%v", m[config.PrimaryKey])

	return &cluster.Entity{
		Type:      config.Type,
		Key:       []byte(key),
		Data:      jsonBytes,
		Timestamp: time.Now().UnixMilli(),
	}
}

// snapshot dumps all existing rows from watched tables using a consistent
// snapshot tied to the replication slot's creation point. This ensures no
// gap between the snapshot and streaming: the slot starts capturing WAL
// changes at the same point the snapshot reads from.
func (d *PostgreSQLDialect) snapshot(
	ctx context.Context,
	config *sql.Config,
	pgCfg *pgConfig,
	snapshotName string,
	pr chan<- *cluster.Proposal,
) error {
	db, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return fmt.Errorf("snapshot: open connection: %w", err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, &gosql.TxOptions{
		Isolation: gosql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("snapshot: begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
		return fmt.Errorf("snapshot: set snapshot: %w", err)
	}

	for _, table := range pgCfg.tables {
		if err := d.snapshotTable(ctx, tx, config, table, pr); err != nil {
			return fmt.Errorf("snapshot: table %s: %w", table, err)
		}
	}

	return tx.Commit()
}

// snapshotTable scans all rows from a single table and emits them as
// proposals in batches. Column mappings and primary key extraction use
// the same logic as the streaming path (tupleToEntity).
func (d *PostgreSQLDialect) snapshotTable(
	ctx context.Context,
	tx *gosql.Tx,
	config *sql.Config,
	table string,
	pr chan<- *cluster.Proposal,
) error {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", quoteTable(table)))
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
		// Scan into []any — the pgx stdlib driver returns concrete Go
		// types (string, int64, etc.) for non-null columns.
		vals := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		// Build column name → string value map, matching pgoutput's
		// text representation where all values are strings.
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

		// Apply column mappings.
		toJSON := make(map[string]any)
		for _, mapping := range config.Mappings {
			toJSON[mapping.JsonName] = m[mapping.SQLColumn]
		}

		jsonBytes, err := json.Marshal(toJSON)
		if err != nil {
			zap.L().Warn("snapshotTable: skipping row with unmarshalable data",
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

// quoteTable quotes a potentially schema-qualified table name for use
// in SQL queries. "public.orders" becomes "public"."orders".
func quoteTable(table string) string {
	parts := strings.Split(table, ".")
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteIdent(p)
	}
	return strings.Join(quoted, ".")
}

// flushPending emits all buffered entities as a single proposal and
// resets the buffer. No-op when the buffer is empty.
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
