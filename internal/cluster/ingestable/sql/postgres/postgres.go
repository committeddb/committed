package postgres

import (
	"context"
	gosql "database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql"
	"github.com/committeddb/committed/internal/cluster/ingestable/sql/dialectpb"
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

	// defaultSnapshotBatchSize is the default number of rows per snapshot
	// batch when Config.Options has no "batch_size" override. See the
	// corresponding constant in the MySQL dialect for rationale.
	defaultSnapshotBatchSize = 10000

	// pgPositionProtoMagic distinguishes the new proto-encoded position
	// format from the legacy raw 8-byte big-endian LSN format. Legacy
	// positions are exactly 8 bytes with arbitrary first-byte values;
	// using 0xFF as a prefix ensures no collision unless the LSN exceeds
	// 2^56 bytes (72 PB) of WAL — well beyond any plausible operational
	// range. See encode/decodePosition.
	pgPositionProtoMagic byte = 0xFF
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

const preflightTimeout = 10 * time.Second

// Preflight implements sql.Dialect: it verifies each watched table's REPLICA
// IDENTITY carries the configured primaryKey on a DELETE, so the ingest can emit
// a keyed tombstone. It is NOT "require FULL" — REPLICA IDENTITY DEFAULT is fine
// as long as the table's primary key covers primaryKey.
func (d *PostgreSQLDialect) Preflight(config *sql.Config) error {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), preflightTimeout)
	defer cancel()

	db, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }()

	for _, table := range pgCfg.tables {
		if err := checkReplicaIdentity(ctx, db, table, config.PrimaryKey); err != nil {
			return err
		}
	}
	return nil
}

// checkReplicaIdentity verifies the table's replica identity covers primaryKey
// in a DELETE's old-row image: FULL covers every column; DEFAULT covers the
// primary key; USING INDEX covers that index's columns; NOTHING covers nothing.
func checkReplicaIdentity(ctx context.Context, db *gosql.DB, table string, primaryKey []string) error {
	fix := fmt.Sprintf("set the table's REPLICA IDENTITY to carry the key "+
		"(e.g. `ALTER TABLE %s REPLICA IDENTITY FULL`) or add a PRIMARY KEY on the configured column(s)", table)

	var ident string
	if err := db.QueryRowContext(ctx,
		`SELECT relreplident FROM pg_class WHERE oid = $1::regclass`, table,
	).Scan(&ident); err != nil {
		return fmt.Errorf("read replica identity of %q: %w", table, err)
	}

	switch ident {
	case "f": // FULL — the whole old row is in the WAL
		return nil
	case "n": // NOTHING — no old-row image at all
		return sql.CheckKeyCoverage(primaryKey, nil, table, fix)
	}

	// DEFAULT ('d') → the primary-key columns; USING INDEX ('i') → that index's
	// columns. One query covers both.
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_index ix
		JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
		WHERE ix.indrelid = $1::regclass
		  AND (($2 = 'd' AND ix.indisprimary) OR ($2 = 'i' AND ix.indisreplident))`,
		table, ident)
	if err != nil {
		return fmt.Errorf("read key columns of %q: %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	var surviving []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return err
		}
		surviving = append(surviving, col)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return sql.CheckKeyCoverage(primaryKey, surviving, table, fix)
}

func (d *PostgreSQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return err
	}

	startLSN, resumeProgress, err := decodePosition(pos)
	if err != nil {
		return err
	}

	backoff := backoffMin

	for {
		err := d.stream(ctx, config, pgCfg, &startLSN, &resumeProgress, pr, po)
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

// decodePosition parses a checkpoint position. The legacy format is a
// raw 8-byte big-endian LSN; the new format is proto-encoded
// PostgresPosition prefixed with pgPositionProtoMagic so a resume can
// carry snapshot progress alongside the LSN.
func decodePosition(pos cluster.Position) (pglogrepl.LSN, *dialectpb.SnapshotProgress, error) {
	if len(pos) == 0 {
		return 0, nil, nil
	}
	if len(pos) > 0 && pos[0] == pgPositionProtoMagic {
		pp := &dialectpb.PostgresPosition{}
		if err := proto.Unmarshal(pos[1:], pp); err != nil {
			return 0, nil, fmt.Errorf("decode position: %w", err)
		}
		return pglogrepl.LSN(pp.Lsn), pp.SnapshotProgress, nil
	}
	if len(pos) == 8 {
		return pglogrepl.LSN(binary.BigEndian.Uint64(pos)), nil, nil
	}
	return 0, nil, fmt.Errorf("unrecognized position format (len=%d)", len(pos))
}

// encodePosition writes a position using the new proto format with the
// magic byte prefix. Passing progress=nil omits the snapshot section so
// streaming-phase checkpoints stay compact.
func encodePosition(lsn pglogrepl.LSN, progress *dialectpb.SnapshotProgress) ([]byte, error) {
	pp := &dialectpb.PostgresPosition{
		Lsn:              uint64(lsn),
		SnapshotProgress: progress,
	}
	raw, err := proto.Marshal(pp)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(raw)+1)
	out = append(out, pgPositionProtoMagic)
	out = append(out, raw...)
	return out, nil
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
	resumeProgress **dialectpb.SnapshotProgress,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	conn, err := pgconn.Connect(ctx, pgCfg.connString)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := ensurePublication(ctx, conn, pgCfg); err != nil {
		return err
	}

	// Create the replication slot if it doesn't already exist.
	// When newly created (not resuming), the slot's starting LSN is
	// captured so streaming resumes from there.
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, pgCfg.slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{})
	slotIsNew := err == nil
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	// Snapshot existing data if either (a) the slot was just created
	// and we have no prior checkpoint, or (b) a prior run was
	// interrupted mid-snapshot and left a progress checkpoint.
	if (slotIsNew && *lastLSN == 0) || *resumeProgress != nil {
		if err := d.snapshot(ctx, config, pgCfg, *resumeProgress, *lastLSN, pr, po); err != nil {
			return err
		}
		*resumeProgress = nil
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

	// resumeFloor is the LSN we asked StartReplication to resume from.
	// pgoutput is allowed to re-stream messages from the slot's
	// restart_lsn (which moves with acks, not with our explicit start
	// LSN), so the server can hand us transactions whose COMMIT LSN
	// has already been processed. Track resumeFloor here and drop any
	// transaction whose BEGIN reports a final LSN <= resumeFloor.
	//
	// skippingTxn becomes true on BEGIN when finalLSN <= resumeFloor
	// and resets on the matching COMMIT. While true, Insert/Update/
	// Delete messages for the txn are dropped, the txn's COMMIT does
	// NOT emit a proposal or position checkpoint, and pending is left
	// untouched (it was empty at BEGIN by invariant, since the
	// preceding non-skipped COMMIT flushed).
	resumeFloor := *lastLSN
	skippingTxn := false

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

			// curLSN is this message's WAL start — a strictly-increasing,
			// resume-deterministic position stamped onto any proposal
			// flushed while handling this message (the proposal's
			// SourceSeq for effectively-once dedup).
			curLSN := xld.WALStart

			switch m := logicalMsg.(type) {
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m

			case *pglogrepl.BeginMessage:
				// Transaction start — pending should already be empty.
				// Compare against resumeFloor to decide whether this
				// txn was already processed in a previous session and
				// should be silently dropped (see resumeFloor doc above).
				skippingTxn = resumeFloor > 0 && m.FinalLSN <= resumeFloor

			case *pglogrepl.InsertMessage:
				if skippingTxn {
					break
				}
				if e := tupleToEntity(m.Tuple, m.RelationID, relations, config, pgCfg, false); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN); err != nil {
						return err
					}
				}

			case *pglogrepl.UpdateMessage:
				if skippingTxn {
					break
				}
				if e := tupleToEntity(m.NewTuple, m.RelationID, relations, config, pgCfg, false); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN); err != nil {
						return err
					}
				}

			case *pglogrepl.DeleteMessage:
				if skippingTxn {
					break
				}
				// A source DELETE becomes a delete (tombstone) entity keyed
				// by the row's primary key — not an upsert of the old row.
				// Only the PK is needed from the old tuple (available under
				// both REPLICA IDENTITY DEFAULT and FULL); the rest of the
				// pre-image is not a payload to write downstream. The
				// syncable removes the keyed record (cluster.Syncable
				// honor-deletes contract).
				if e := tupleToEntity(m.OldTuple, m.RelationID, relations, config, pgCfg, true); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN); err != nil {
						return err
					}
				}

			case *pglogrepl.CommitMessage:
				if skippingTxn {
					skippingTxn = false
					// pending is empty by invariant (we dropped every
					// row in the txn). No proposal to flush, no
					// position checkpoint to emit — the supervisor
					// already has a position past this LSN, which is
					// why we're skipping.
					break
				}
				if err := flushPending(ctx, &pending, pr, curLSN); err != nil {
					return err
				}

				// Use TransactionEndLSN (past the end of the
				// transaction) so a resume from this position does
				// not replay the already-processed transaction.
				endLSN := m.TransactionEndLSN
				posBytes, err := encodePosition(endLSN, nil)
				if err != nil {
					return err
				}

				select {
				case po <- posBytes:
				case <-ctx.Done():
					return nil
				}

				clientXLogPos = endLSN
				*lastLSN = endLSN

				// Acknowledge the commit position to Postgres on every
				// commit (not just every standbyTimeout). The throttle
				// here was the root cause of TestPostgresPositionResume:
				// after one commit, the supervisor / test had a position
				// checkpoint to resume from, but the server's
				// confirmed_flush_lsn hadn't moved, so a resume from that
				// checkpoint LSN re-streamed already-processed messages
				// from the slot's older restart_lsn. Per-commit acking
				// keeps the server's view of our progress in sync with
				// the position we publish on the position channel.
				//
				// Cost is one TCP message per Postgres COMMIT, which is
				// negligible — commits are inherently rate-limited by
				// the upstream workload, not by us. The throttle still
				// applies to keepalive-driven acks below where there
				// is no new commit to report.
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: clientXLogPos,
				})
				if err != nil {
					return err
				}
				nextStandby = time.Now().Add(standbyTimeout)

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
//
// Table names are quoted via quoteTable (not quoteIdent) so that
// schema-qualified entries like "public.orders" become "public"."orders"
// — a schema-qualified reference — instead of a single literal
// identifier "public.orders" with a dot in the name. The TOML examples
// in this repo (postgres_ingestable.toml, postgres_multi_table_ingestable.toml)
// all use schema-qualified names; quoteIdent here used to break them.
func ensurePublication(ctx context.Context, conn *pgconn.PgConn, pgCfg *pgConfig) error {
	quoted := make([]string, len(pgCfg.tables))
	for i, t := range pgCfg.tables {
		quoted[i] = quoteTable(t)
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
// the relation is not in the watched table list. When isDelete is true the
// result is a delete (tombstone) entity keyed by the row's primary key — the
// tuple supplies only the key, not a payload (a source DELETE removes the
// downstream record; see the cluster.Syncable honor-deletes contract).
// pgCategoryForOID maps a PostgreSQL type OID (carried in a logical-replication
// relation message, the CDC path) to a JSON category. Unlisted OIDs render as
// text. Kept in sync with pgCategoryForTypeName so snapshot and CDC agree.
func pgCategoryForOID(oid uint32) sql.JSONCategory {
	switch oid {
	case pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		pgtype.Float4OID, pgtype.Float8OID, pgtype.NumericOID:
		return sql.CatNumber
	case pgtype.BoolOID:
		return sql.CatBool
	case pgtype.JSONOID, pgtype.JSONBOID:
		return sql.CatJSON
	}
	return sql.CatText
}

// pgCategoryForTypeName maps a database/sql DatabaseTypeName (the snapshot path,
// where the driver reports a type name not an OID) to a JSON category. Kept in
// sync with pgCategoryForOID.
func pgCategoryForTypeName(name string) sql.JSONCategory {
	switch strings.ToUpper(name) {
	case "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT",
		"FLOAT4", "FLOAT8", "REAL", "DOUBLE PRECISION", "NUMERIC", "DECIMAL":
		return sql.CatNumber
	case "BOOL", "BOOLEAN":
		return sql.CatBool
	case "JSON", "JSONB":
		return sql.CatJSON
	}
	return sql.CatText
}

func tupleToEntity(
	tuple *pglogrepl.TupleData,
	relationID uint32,
	relations map[uint32]*pglogrepl.RelationMessage,
	config *sql.Config,
	pgCfg *pgConfig,
	isDelete bool,
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

	key := sql.CompositeKey(m, config.PrimaryKey)

	// A delete carries no payload — emit a tombstone keyed by the PK.
	if isDelete {
		return cluster.NewDeleteEntity(config.Type, []byte(key))
	}

	// Each relation column carries its type OID; render mapped values as their
	// natural JSON type rather than the pgoutput text.
	cat := make(map[string]sql.JSONCategory, len(rel.Columns))
	for _, rc := range rel.Columns {
		cat[strings.ToLower(rc.Name)] = pgCategoryForOID(rc.DataType)
	}
	toJSON := make(map[string]any)
	for _, mapping := range config.Mappings {
		toJSON[mapping.JsonName] = sql.JSONValue(m[mapping.SQLColumn], cat[mapping.SQLColumn])
	}

	jsonBytes, err := json.Marshal(toJSON)
	if err != nil {
		zap.L().Warn("tupleToEntity: skipping row with unmarshalable data",
			zap.String("table", rel.RelationName),
			zap.Error(err),
		)
		return nil
	}

	return &cluster.Entity{
		Type: config.Type,
		Key:  []byte(key),
		Data: jsonBytes,
	}
}

// snapshot dumps all existing rows from watched tables using keyset
// pagination with one short REPEATABLE READ transaction per batch.
// Each batch becomes a single cluster.Proposal followed by a position
// checkpoint that records per-table progress so a restart mid-snapshot
// resumes without re-scanning completed rows.
//
// Per-batch transactions trade point-in-time consistency for bounded
// MVCC/xmin pressure on the source. The replication slot's WAL stream
// (started from the slot creation LSN, well before any snapshot read)
// will replay all concurrent changes once streaming begins, converging
// consumers to the correct end state.
func (d *PostgreSQLDialect) snapshot(
	ctx context.Context,
	config *sql.Config,
	pgCfg *pgConfig,
	resumeProgress *dialectpb.SnapshotProgress,
	lsn pglogrepl.LSN,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	db, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return fmt.Errorf("snapshot: open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	batchSize := parseBatchSize(config.Options)

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

	for _, table := range pgCfg.tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table",
				zap.String("table", table),
			)
			continue
		}
		if err := d.snapshotTable(ctx, db, config, table, batchSize, progress, lsn, pr, po); err != nil {
			return fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		completed[table] = true

		if err := emitSnapshotProgress(ctx, po, lsn, progress); err != nil {
			return err
		}
		zap.L().Info("snapshot: table complete", zap.String("table", table))
	}

	return nil
}

// snapshotTable reads one table in batches using keyset pagination.
// Each batch runs inside its own short transaction and produces one
// proposal + one progress checkpoint.
func (d *PostgreSQLDialect) snapshotTable(
	ctx context.Context,
	db *gosql.DB,
	config *sql.Config,
	table string,
	batchSize int,
	progress *dialectpb.SnapshotProgress,
	lsn pglogrepl.LSN,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) error {
	pkCols := config.PrimaryKey
	lastPK, haveLastPK := progress.LastPkByTable[table]

	batchNum := 0
	totalRows := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batchNum++

		entities, batchLastPK, count, err := readBatch(ctx, db, config, table, pkCols, lastPK, haveLastPK, batchSize)
		if err != nil {
			return err
		}
		if count == 0 {
			break
		}

		p := &cluster.Proposal{Entities: entities}
		select {
		case pr <- p:
		case <-ctx.Done():
			return ctx.Err()
		}

		lastPK = batchLastPK
		haveLastPK = true
		progress.LastPkByTable[table] = lastPK
		totalRows += count

		if err := emitSnapshotProgress(ctx, po, lsn, progress); err != nil {
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
			break
		}
	}

	return nil
}

// readBatch opens a short REPEATABLE READ transaction and reads up to
// batchSize rows with pk > lastPK (or the first batchSize rows when
// haveLastPK is false). Returns the entities, the last pk scanned, and
// the count.
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

	quotedTable := quoteTable(table)

	// Keyset pagination ordered by the full PK. For a single column this is
	// `c > $1`; for a composite it's the row-value comparison `(c1, c2) > ($1,
	// $2)` — Postgres infers each placeholder's type from its column, so the
	// cursor values bind fine as strings. The cursor itself is the prior batch's
	// last entity key (CompositeKey), decoded back to per-column values here.
	orderCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		orderCols[i] = quoteIdent(c) + " ASC"
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
			cols[i] = quoteIdent(c)
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = cursor[i]
		}
		query = fmt.Sprintf(
			"SELECT * FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
			quotedTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "), orderBy, batchSize,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT * FROM %s ORDER BY %s LIMIT %d",
			quotedTable, orderBy, batchSize,
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
		cats[i] = pgCategoryForTypeName(ct.DatabaseTypeName())
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
		// its type so the payload renders as natural JSON.
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
			zap.L().Warn("readBatch: skipping row with unmarshalable data",
				zap.String("table", table),
				zap.Error(err),
			)
			continue
		}

		key := sql.CompositeKey(m, pkCols)
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

// emitSnapshotProgress sends a position checkpoint that captures the
// pre-snapshot LSN plus in-progress snapshot state. Streaming-phase
// checkpoints omit the progress field so they stay compact.
func emitSnapshotProgress(
	ctx context.Context,
	po chan<- cluster.Position,
	lsn pglogrepl.LSN,
	progress *dialectpb.SnapshotProgress,
) error {
	bs, err := encodePosition(lsn, progress)
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
// resets the buffer. No-op when the buffer is empty. lsn is the WAL
// position of the message that triggered the flush; it becomes the
// proposal's SourceSeq, a strictly-monotonic per-proposal key the
// ingest worker uses to dedup re-emitted proposals after a crash/flap
// (effectively-once). Monotonic because clientXLogPos only advances, and
// deterministic because a resume re-reads the same messages in the same
// order, producing the same flush LSNs.
func flushPending(ctx context.Context, pending *[]*cluster.Entity, pr chan<- *cluster.Proposal, lsn pglogrepl.LSN) error {
	if len(*pending) == 0 {
		return nil
	}
	p := &cluster.Proposal{Entities: *pending, SourceSeq: uint64(lsn)}
	select {
	case pr <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	*pending = nil
	return nil
}
