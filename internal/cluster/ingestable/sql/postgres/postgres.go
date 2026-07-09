package postgres

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
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

// statusLagTimeout bounds the source query Status makes to read replication
// lag. Status is a read endpoint, so it must not hang on an unreachable source —
// a failed/late query simply leaves Lag nil.
const statusLagTimeout = 5 * time.Second

// Status implements sql.Dialect: it decodes the checkpoint position into a
// point-in-time IngestableStatus and, once streaming, queries the slot for
// replication lag. A position that still carries snapshot progress is in the
// snapshot phase (lag isn't meaningful yet — the slot is parked at its creation
// LSN); once snapshot progress is gone the worker is streaming and lag is the
// slot's distance behind the source write head.
func (d *PostgreSQLDialect) Status(ctx context.Context, config *sql.Config, pos cluster.Position) (cluster.IngestableStatus, error) {
	lsn, progress, err := decodePosition(pos)
	if err != nil {
		return cluster.IngestableStatus{}, fmt.Errorf("[postgres.status] decode position: %w", err)
	}

	status := cluster.IngestableStatus{
		Position:         lsn.String(),
		SnapshotProgress: sql.SnapshotTableStatus(config, progress),
	}

	if progress != nil {
		status.Phase = "snapshot"
		return status, nil
	}
	status.Phase = "streaming"

	// Streaming: read the slot's lag. A failure (source unreachable, slot not
	// yet created) leaves Lag nil — the rest of the status is still useful — so
	// it is logged, not returned.
	lag, ok, lagErr := d.replicationLag(ctx, config)
	if lagErr != nil {
		zap.L().Debug("[postgres.status] replication lag query failed",
			zap.String("slot", config.Options["slot_name"]), zap.Error(lagErr))
	} else if ok {
		status.Lag = &lag
		status.CaughtUp = lag == 0
	}

	return status, nil
}

// replicationLag returns how many bytes the source's write head is ahead of the
// slot's confirmed flush — the durable backlog this ingest still has to consume.
// ok is false (lag unknown, not an error) when the slot does not exist yet or
// has no confirmed flush LSN; a real query failure returns the error so the
// caller can log it and leave Lag nil.
func (d *PostgreSQLDialect) replicationLag(ctx context.Context, config *sql.Config) (uint64, bool, error) {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return 0, false, err
	}

	cctx, cancel := context.WithTimeout(ctx, statusLagTimeout)
	defer cancel()

	db, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return 0, false, fmt.Errorf("open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	var diff gosql.NullInt64
	err = db.QueryRowContext(cctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
		 FROM pg_replication_slots WHERE slot_name = $1`, pgCfg.slotName).Scan(&diff)
	switch {
	case errors.Is(err, gosql.ErrNoRows):
		return 0, false, nil // slot not created yet — lag not yet meaningful
	case err != nil:
		return 0, false, fmt.Errorf("query slot lag: %w", err)
	case !diff.Valid:
		return 0, false, nil // confirmed_flush_lsn is NULL — slot never confirmed
	}

	// A healthy slot can momentarily report a tiny negative diff; clamp.
	if diff.Int64 < 0 {
		return 0, true, nil
	}
	return uint64(diff.Int64), true, nil
}

// SourceColumns implements sql.Dialect: it introspects each watched table's
// columns (in source order) so the parser can expand a MapAllColumns config
// into explicit mappings. Read-only; one short-lived connection bounded by the
// build-time timeout.
func (d *PostgreSQLDialect) SourceColumns(config *sql.Config) (map[string][]string, error) {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), preflightTimeout)
	defer cancel()

	db, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return nil, fmt.Errorf("open connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	out := make(map[string][]string, len(config.Tables))
	for _, table := range config.Tables {
		cols, err := pgTableColumns(ctx, db, table)
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

// pgTableColumns returns a table's user columns in attribute order. It resolves
// the table via ::regclass so a schema-qualified name ("ingress.movie") works,
// and skips dropped and system columns.
func pgTableColumns(ctx context.Context, db *gosql.DB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		WHERE a.attrelid = $1::regclass
		  AND a.attnum > 0
		  AND NOT a.attisdropped
		ORDER BY a.attnum`, table)
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

// stream runs one replication session. It connects, ensures the publication
// and slot exist, starts streaming, and processes messages until the
// connection breaks or ctx is canceled. On commit boundaries it updates
// *lastLSN so the outer retry loop can resume from the correct position.
// decodeLogicalMessage wraps pglogrepl.Parse with a recover. The pgoutput
// decoder indexes tuple fields and reads fixed-width integers without
// bounds-checking (pglogrepl message.go), so a malformed or truncated frame
// panics rather than returning an error. Convert that panic into an error so the
// caller reconnects (stream's own defer is the belt to this suspenders) instead
// of the panic escaping the ingest goroutine and crashing the whole node.
func decodeLogicalMessage(walData []byte) (msg pglogrepl.Message, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic parsing pgoutput message (%d bytes): %v", len(walData), r)
		}
	}()
	return pglogrepl.Parse(walData)
}

func (d *PostgreSQLDialect) stream(
	ctx context.Context,
	config *sql.Config,
	pgCfg *pgConfig,
	lastLSN *pglogrepl.LSN,
	resumeProgress **dialectpb.SnapshotProgress,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
) (err error) {
	// A malformed or truncated pgoutput frame can panic the pglogrepl decoder
	// (it doesn't bounds-check every tuple field) — but external input across the
	// CDC trust boundary must never crash the node. Recover any decode panic into
	// a stream error so the reconnect loop re-establishes from the last checkpoint
	// instead: a transient corruption clears on the re-read, a persistent one just
	// retries under the loop's backoff. (decodeLogicalMessage wraps the most
	// panic-prone call with the same guard, and is unit-tested.)
	defer func() {
		if r := recover(); r != nil {
			zap.L().Error("recovered from a panic decoding the Postgres replication stream; reconnecting",
				zap.Strings("tables", config.Tables), zap.Any("panic", r), zap.Stack("stack"))
			err = fmt.Errorf("panic decoding replication stream: %v", r)
		}
	}()

	conn, err := pgconn.Connect(ctx, pgCfg.connString)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	// A normal (non-replication) connection for re-selecting columns a pgoutput
	// UPDATE omitted as unchanged TOASTed values — the replication conn above
	// cannot run queries. See tupleToEntity / fillUnchanged.
	sqlDB, err := gosql.Open("pgx", pgCfg.sqlConnString)
	if err != nil {
		return err
	}
	defer func() { _ = sqlDB.Close() }()

	resolve := func(ctx context.Context, table string, pk map[string]string, cols []string) (map[string]string, error) {
		sel := make([]string, len(cols))
		for i, c := range cols {
			sel[i] = quoteIdent(c) + "::text"
		}
		where := make([]string, 0, len(pk))
		args := make([]any, 0, len(pk))
		i := 1
		for k, v := range pk {
			where = append(where, fmt.Sprintf("%s = $%d", quoteIdent(k), i))
			args = append(args, v)
			i++
		}
		//nolint:gosec // G201: identifiers are quoteIdent/quoteTable-escaped and all values are bound as $N parameters
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
			strings.Join(sel, ", "), quoteTable(table), strings.Join(where, " AND "))
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for j := range dest {
			ptrs[j] = &dest[j]
		}
		if err := sqlDB.QueryRowContext(ctx, query, args...).Scan(ptrs...); err != nil {
			return nil, err
		}
		out := make(map[string]string, len(cols))
		for j, c := range cols {
			switch v := dest[j].(type) {
			case string:
				out[c] = v
			case []byte:
				out[c] = string(v)
			}
		}
		return out, nil
	}

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
		if len(msg.Data) == 0 {
			continue // an empty CopyData body carries no message-type byte — msg.Data[0] would panic
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

			logicalMsg, err := decodeLogicalMessage(xld.WALData)
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
				if e := tupleToEntity(ctx, m.Tuple, m.RelationID, relations, config, pgCfg, false, resolve); e != nil {
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
				if e := tupleToEntity(ctx, m.NewTuple, m.RelationID, relations, config, pgCfg, false, resolve); e != nil {
					pending = append(pending, e)
					// A PK-changing UPDATE writes the new-key row above but would
					// orphan the old-key row downstream (two rows for one source
					// row — a divergence and an un-deletable stale record, RTBF
					// concern). When the old tuple carries a different key, emit a
					// delete tombstone for the old key too, mirroring DeleteMessage.
					// REPLICA IDENTITY DEFAULT sends OldTuple only on a key change;
					// FULL sends it always, so the key-equality guard suppresses a
					// spurious tombstone on a non-key UPDATE.
					if m.OldTuple != nil {
						if old := tupleToEntity(ctx, m.OldTuple, m.RelationID, relations, config, pgCfg, true, resolve); old != nil && !bytes.Equal(old.Key, e.Key) {
							pending = append(pending, old)
						}
					}
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
				if e := tupleToEntity(ctx, m.OldTuple, m.RelationID, relations, config, pgCfg, true, resolve); e != nil {
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

// unchangedResolver fetches the current text values of columns a pgoutput UPDATE
// left unchanged (Postgres omits unchanged TOASTed values from the new tuple),
// keyed by lowercased column name, by re-selecting the row by primary key.
type unchangedResolver func(ctx context.Context, table string, pk map[string]string, cols []string) (map[string]string, error)

// fillUnchanged re-selects the current values of columns an UPDATE left unchanged
// and merges them into m so the emitted row is complete. On any failure it logs
// and leaves those columns absent (the pre-fix behavior, now visible) rather than
// dropping the whole row.
func fillUnchanged(ctx context.Context, resolve unchangedResolver, rel *pglogrepl.RelationMessage, primaryKey []string, m map[string]any, cols []string) {
	pk := make(map[string]string, len(primaryKey))
	for _, k := range primaryKey {
		v, ok := m[strings.ToLower(k)]
		if !ok || v == nil {
			zap.L().Warn("cannot re-select unchanged TOAST columns: primary key absent from the tuple",
				zap.String("table", rel.RelationName), zap.String("pk", k))
			return
		}
		pk[strings.ToLower(k)] = fmt.Sprint(v)
	}
	vals, err := resolve(ctx, rel.Namespace+"."+rel.RelationName, pk, cols)
	if err != nil {
		zap.L().Warn("re-select of unchanged TOAST columns failed; emitting row without them",
			zap.String("table", rel.RelationName), zap.Error(err))
		return
	}
	for c, v := range vals {
		m[c] = v
	}
}

func tupleToEntity(
	ctx context.Context,
	tuple *pglogrepl.TupleData,
	relationID uint32,
	relations map[uint32]*pglogrepl.RelationMessage,
	config *sql.Config,
	pgCfg *pgConfig,
	isDelete bool,
	resolve unchangedResolver,
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
	var unchanged []string // columns pgoutput omitted as unchanged TOASTed ('u')
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colName := strings.ToLower(rel.Columns[i].Name)
		switch col.DataType {
		case 'n': // null
			m[colName] = nil
		case 'u': // unchanged TOASTed value — not sent; re-select it below
			unchanged = append(unchanged, colName)
		case 't': // text representation
			m[colName] = string(col.Data)
		}
	}

	key := sql.CompositeKey(m, config.PrimaryKey)

	// A delete carries no payload — emit a tombstone keyed by the PK.
	if isDelete {
		return cluster.NewDeleteEntity(config.Type, []byte(key))
	}

	// An UPDATE that left a TOASTed column unchanged omits it from the new tuple
	// (pgoutput status 'u'). Re-select the current values so the emitted row is a
	// complete image — otherwise the full-row payload nulls those columns and
	// clobbers them downstream on every such UPDATE.
	if len(unchanged) > 0 && resolve != nil {
		fillUnchanged(ctx, resolve, rel, config.PrimaryKey, m, unchanged)
	}

	// Each relation column carries its type OID; render mapped values as their
	// natural JSON type rather than the pgoutput text.
	cat := make(map[string]sql.JSONCategory, len(rel.Columns))
	for _, rc := range rel.Columns {
		cat[strings.ToLower(rc.Name)] = pgCategoryForOID(rc.DataType)
	}
	toJSON := sql.BuildEntityJSON(config.Mappings, m, cat)

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

		toJSON := sql.BuildEntityJSON(config.Mappings, raw, catByName)

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
