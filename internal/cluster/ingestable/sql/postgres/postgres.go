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
	"github.com/committeddb/committed/internal/cluster/sqlident"
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
	// cluster.ParseConnString, not url.Parse: on a parse failure url.Parse's
	// *url.Error embeds the raw (already ${VAR}-resolved) connection string —
	// password and all — and this error reaches an HTTP 400 body via
	// SourceColumns -> ParseIngestable. The helper strips the value.
	u, err := cluster.ParseConnString(config.ConnectionString)
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

// TeardownSource drops the replication slot and publication this ingestable
// created on the source. The slot is what pins the source's WAL (its restart_lsn
// holds WAL back), so an orphaned slot from a deleted ingestable grows the
// source's disk without bound — this releases it. Called by the ingest delete
// path on the owner node only, AFTER the worker has stopped, so the slot is
// inactive and droppable. Idempotent: the slot drop is guarded on existence
// (pg_drop_replication_slot errors on a missing name) and the publication uses IF
// EXISTS, so a re-run after a leadership flap is a clean no-op.
func (d *PostgreSQLDialect) TeardownSource(config *sql.Config) error {
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

	// Terminate any walsender still holding the slot active before dropping it.
	// The delete path tears down AFTER cancelling the worker, but a worker that
	// wedged on its source (its drain timed out and the delete path abandoned it)
	// can keep its replication connection — and thus the slot — active. An active
	// slot cannot be dropped (pg_drop_replication_slot raises "replication slot
	// ... is active for PID ..."), so without this the slot orphans and pins the
	// source's WAL indefinitely. A no-op when the slot is already inactive
	// (active_pid IS NULL) or absent (no row matches).
	if _, err := db.ExecContext(ctx,
		`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
			WHERE slot_name = $1 AND active_pid IS NOT NULL`,
		pgCfg.slotName); err != nil {
		return fmt.Errorf("terminate walsender holding replication slot %q: %w", pgCfg.slotName, err)
	}

	// Drop the slot — it's what holds WAL. The WHERE-guarded form drops it only if
	// present, so a missing slot is not an error (unlike a bare
	// pg_drop_replication_slot call). The terminated walsender clears the slot's
	// active flag asynchronously as it exits, so a drop immediately after the
	// terminate can still observe a not-yet-released slot; retry on the ctx cadence
	// until it releases (or the deadline fires). On the common already-inactive
	// path the first attempt succeeds, so the retry costs nothing.
	if err := dropReplicationSlotWithRetry(ctx, db, pgCfg.slotName); err != nil {
		return fmt.Errorf("drop replication slot %q: %w", pgCfg.slotName, err)
	}
	// Then the publication — it pins no WAL, but leaving it clutters the source.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", quoteIdent(pgCfg.publication))); err != nil {
		return fmt.Errorf("drop publication %q: %w", pgCfg.publication, err)
	}
	return nil
}

// dropReplicationSlotWithRetry drops the named slot, retrying while it is still
// active. TeardownSource terminates the walsender holding the slot before calling
// this, but that backend releases the slot asynchronously as it exits, so the
// first drop can still race a not-yet-cleared active flag ("replication slot ...
// is active for PID ..."). Retry on the ctx cadence until the drop succeeds — the
// WHERE guard makes an already-gone slot a zero-row no-op — or ctx expires, in
// which case the last error is returned. Mirrors cleanReplication's async-death
// handling in the tests.
func dropReplicationSlotWithRetry(ctx context.Context, db *gosql.DB, slotName string) error {
	const retryInterval = 100 * time.Millisecond
	for {
		_, err := db.ExecContext(ctx,
			`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`,
			slotName)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(retryInterval):
		}
	}
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

func (d *PostgreSQLDialect) Ingest(ctx context.Context, config *sql.Config, pos cluster.Position, epochFloor uint64, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error {
	pgCfg, err := buildPgConfig(config)
	if err != nil {
		return err
	}

	startLSN, resumeProgress, epoch, err := decodePosition(pos)
	if err != nil {
		return err
	}

	backoff := backoffMin

	for {
		err := d.stream(ctx, config, pgCfg, &startLSN, &resumeProgress, &epoch, epochFloor, pr, po)
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
func decodePosition(pos cluster.Position) (pglogrepl.LSN, *dialectpb.SnapshotProgress, uint64, error) {
	if len(pos) == 0 {
		return 0, nil, 0, nil
	}
	if len(pos) > 0 && pos[0] == pgPositionProtoMagic {
		pp := &dialectpb.PostgresPosition{}
		if err := proto.Unmarshal(pos[1:], pp); err != nil {
			return 0, nil, 0, fmt.Errorf("decode position: %w", err)
		}
		return pglogrepl.LSN(pp.Lsn), pp.SnapshotProgress, pp.RefreshEpoch, nil
	}
	if len(pos) == 8 {
		return pglogrepl.LSN(binary.BigEndian.Uint64(pos)), nil, 0, nil
	}
	return 0, nil, 0, fmt.Errorf("unrecognized position format (len=%d)", len(pos))
}

// encodePosition writes a position using the new proto format with the
// magic byte prefix. Passing progress=nil omits the snapshot section so
// streaming-phase checkpoints stay compact. epoch is the reconciling-refresh
// generation carried across checkpoints (see PostgresPosition.refresh_epoch).
func encodePosition(lsn pglogrepl.LSN, progress *dialectpb.SnapshotProgress, epoch uint64) ([]byte, error) {
	pp := &dialectpb.PostgresPosition{
		Lsn:              uint64(lsn),
		SnapshotProgress: progress,
		RefreshEpoch:     epoch,
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
	lsn, progress, _, err := decodePosition(pos)
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
	epoch *uint64,
	epochFloor uint64,
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

	// Resolves relation-message type OIDs to JSON categories, following user
	// DOMAIN types to their base type so a domain column classifies the same as
	// it does on the snapshot path. Backed by the same non-replication conn.
	catResolver := newOIDCategoryResolver(sqlDB)

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

	addedTables, err := ensurePublication(ctx, conn, pgCfg)
	if err != nil {
		return err
	}

	// Create the replication slot if it doesn't already exist. The result's
	// ConsistentPoint is the new slot's starting LSN — the earliest point from
	// which it retains WAL.
	slotRes, err := pglogrepl.CreateReplicationSlot(ctx, conn, pgCfg.slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{})
	slotIsNew := err == nil
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}

	switch {
	case slotIsNew && *lastLSN != 0:
		// The slot was recreated (dropped, expired, or reaped by
		// max_slot_wal_keep_size) while a prior checkpoint survived. That LSN is
		// now stale: the WAL between it and the new slot's ConsistentPoint was
		// released with the old slot, so resuming from it would silently skip
		// every change in that window (or wedge StartReplication on a missing
		// LSN). The new slot only retains WAL from its ConsistentPoint, so the
		// only recoverable path is a full re-snapshot starting there.
		cp, perr := pglogrepl.ParseLSN(slotRes.ConsistentPoint)
		if perr != nil {
			return fmt.Errorf("parse recreated-slot ConsistentPoint %q: %w", slotRes.ConsistentPoint, perr)
		}
		// Bump the refresh epoch: the re-snapshot re-emits every live row at the
		// new epoch, and the closing refresh-boundary marker sweeps rows left at
		// an older one — reconciling the deletes this upsert-only enumeration
		// cannot signal. Floor to max(checkpoint, topic highwater, 1) first so the
		// bump lands strictly above every generation already on the sink (the
		// highwater survives a DeleteIngestable that cleared the checkpoint epoch).
		*epoch = max(*epoch, epochFloor, 1) + 1
		zap.L().Warn("replication slot was recreated while a checkpoint survived; re-snapshotting from the new slot's consistent point. "+
			"Rows deleted at the source in the lost WAL window (RTBF-erased subjects among them) are reconciled on KEYED sinks by the "+
			"refresh-boundary sweep that closes this re-snapshot; keyless/append and projection sinks are NOT reconciled and require "+
			"an operator rebuild for gap recovery.",
			zap.String("slot", pgCfg.slotName),
			zap.Stringer("staleLSN", *lastLSN),
			zap.String("consistentPoint", slotRes.ConsistentPoint),
			zap.Uint64("refreshEpoch", *epoch))
		*lastLSN = cp
		*resumeProgress = nil
		if err := d.snapshot(ctx, config, pgCfg, pgCfg.tables, nil, *lastLSN, *epoch, pr, po); err != nil {
			return err
		}
		if err := emitRefreshBoundary(ctx, config, pr, po, *lastLSN, *epoch); err != nil {
			return err
		}
	case (slotIsNew && *lastLSN == 0) || *resumeProgress != nil:
		// (a) the slot was just created and we have no prior checkpoint (first
		// run — StartReplication(0) below starts from the new slot's consistent
		// point), or (b) a prior run was interrupted mid-snapshot: full snapshot
		// of every configured table. The closing marker is a no-op on a fresh sink
		// but makes a rebuild against a pre-populated sink reconcile.
		if *resumeProgress != nil {
			// Mid-snapshot resume: continue the in-progress epoch the checkpoint
			// carries (its closing marker has not committed, so the topic highwater
			// does not yet reflect this refresh) — do NOT bump.
			*epoch = max(*epoch, 1)
		} else {
			// Fresh full snapshot, no prior checkpoint. refreshSnapshotEpoch stamps
			// strictly above the delete-surviving topic highwater on a same-topic
			// recreate (whose cleared position reset the checkpoint epoch to 0 while
			// the sink still holds rows up to the highwater), else epoch 1.
			*epoch = sql.RefreshSnapshotEpoch(*epoch, epochFloor)
		}
		if err := d.snapshot(ctx, config, pgCfg, pgCfg.tables, *resumeProgress, *lastLSN, *epoch, pr, po); err != nil {
			return err
		}
		*resumeProgress = nil
		if err := emitRefreshBoundary(ctx, config, pr, po, *lastLSN, *epoch); err != nil {
			return err
		}
	case len(addedTables) > 0:
		// Resuming an existing slot, but ensurePublication just (re-)added tables
		// to the publication (a re-POST added one, or a dropped-then-recreated
		// source table was re-added). Backfill ONLY those — the rest are already
		// streaming — then StartReplication below picks up their ongoing changes
		// from *lastLSN. Add-to-publication-happened-first, so the overlap window
		// is covered by the sink's keyed upsert, not lost. Stamp the backfilled
		// rows with the current epoch but emit NO marker: this is a PARTIAL
		// refresh, and a topic-level sweep would delete the sibling tables' rows
		// this backfill did not re-emit. Floor to the topic highwater so the
		// backfilled rows never land below a generation already on the sink.
		*epoch = max(*epoch, epochFloor, 1)
		if err := d.snapshot(ctx, config, pgCfg, addedTables, nil, *lastLSN, *epoch, pr, po); err != nil {
			return err
		}
	}

	// Any streaming path that did not snapshot still stamps entities, so ensure
	// the epoch is at least max(highwater, 1): never stamp a streamed row below a
	// generation already on the sink (a pre-feature checkpoint resumes at 0).
	if lo := max(epochFloor, 1); *epoch < lo {
		*epoch = lo
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
				if e := tupleToEntity(ctx, m.Tuple, m.RelationID, relations, config, pgCfg, false, resolve, catResolver); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN, *epoch); err != nil {
						return err
					}
				}

			case *pglogrepl.UpdateMessage:
				if skippingTxn {
					break
				}
				if e := tupleToEntity(ctx, m.NewTuple, m.RelationID, relations, config, pgCfg, false, resolve, catResolver); e != nil {
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
						if old := tupleToEntity(ctx, m.OldTuple, m.RelationID, relations, config, pgCfg, true, resolve, catResolver); old != nil && !bytes.Equal(old.Key, e.Key) {
							pending = append(pending, old)
						}
					}
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN, *epoch); err != nil {
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
				if e := tupleToEntity(ctx, m.OldTuple, m.RelationID, relations, config, pgCfg, true, resolve, catResolver); e != nil {
					pending = append(pending, e)
				}
				if len(pending) >= maxPendingEntities {
					if err := flushPending(ctx, &pending, pr, curLSN, *epoch); err != nil {
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
				if err := flushPending(ctx, &pending, pr, curLSN, *epoch); err != nil {
					return err
				}

				// Use TransactionEndLSN (past the end of the
				// transaction) so a resume from this position does
				// not replay the already-processed transaction.
				endLSN := m.TransactionEndLSN
				posBytes, err := encodePosition(endLSN, nil, *epoch)
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
				// committed has no clear-all primitive yet, so a source TRUNCATE is
				// not propagated: the sink keeps the truncated rows until a
				// re-snapshot. Do NOT swallow it silently (the old behavior) — name
				// the affected tables at Warn so an operator can reconcile. Full
				// propagation is tracked separately; see the TRUNCATE caveat in
				// docs/operations/cdc-setup.md.
				names := make([]string, 0, len(m.RelationIDs))
				for _, relID := range m.RelationIDs {
					if rel, ok := relations[relID]; ok {
						names = append(names, rel.Namespace+"."+rel.RelationName)
					}
				}
				zap.L().Warn("TRUNCATE on a watched table is not propagated to the sink; "+
					"the sink now diverges from the source and must be re-snapshotted to reconcile",
					zap.Strings("tables", names))
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
// special characters (hyphens, spaces, etc.) in names. It delegates to the shared
// sqlident seam so the ingest read path and the syncable write path escape
// identifiers identically.
func quoteIdent(s string) string {
	return sqlident.Postgres.Ident(s)
}

// ensurePublication creates the publication if it does not exist, or — if it
// already exists — reconciles its table set so every configured table is
// published. It returns the tables that were (re-)added, so the caller can
// backfill just those on a resumed slot. The connection must be in
// replication=database mode which allows SQL.
//
// Without reconciliation, pgoutput only streams the tables the publication had
// when first created, so two silent-row-loss cases slip through: (a) a re-POST
// adds a table to sql.tables — the publication is untouched, and because the
// slot already exists no snapshot runs either, so the table is never streamed;
// (b) a watched table is DROPped + reCREATEd on the source — Postgres auto-drops
// it from the publication and the recreated table (new OID) is never re-added.
// ADD-ing each configured table (skipping "already member") covers both.
//
// Table names are quoted via quoteTable (not quoteIdent) so schema-qualified
// entries like "public.orders" become "public"."orders" — a schema-qualified
// reference — instead of one literal identifier with a dot in it.
func ensurePublication(ctx context.Context, conn *pgconn.PgConn, pgCfg *pgConfig) ([]string, error) {
	quoted := make([]string, len(pgCfg.tables))
	for i, t := range pgCfg.tables {
		quoted[i] = quoteTable(t)
	}
	tableList := strings.Join(quoted, ", ")
	create := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", quoteIdent(pgCfg.publication), tableList)
	if _, err := conn.Exec(ctx, create).ReadAll(); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return nil, err
		}
		// Publication exists — reconcile. ADD each configured table; Postgres
		// reports the ones already present ("already member"), which we skip. The
		// ones that ADD accepts are new to the publication and need a backfill.
		var added []string
		for _, t := range pgCfg.tables {
			alter := fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s", quoteIdent(pgCfg.publication), quoteTable(t))
			if _, err := conn.Exec(ctx, alter).ReadAll(); err != nil {
				if strings.Contains(err.Error(), "already member") {
					continue
				}
				return nil, fmt.Errorf("reconcile publication: add table %s: %w", t, err)
			}
			added = append(added, t)
		}
		return added, nil
	}
	// Freshly created with every configured table. The caller's slot-is-new
	// branch does the full snapshot; if the slot somehow already exists, these
	// are treated as newly-added and backfilled.
	return pgCfg.tables, nil
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
	case pgtype.ByteaOID:
		return sql.CatBinary
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
	case "BYTEA":
		return sql.CatBinary
	}
	return sql.CatText
}

// oidCategoryResolver maps a relation-message type OID to a JSON category,
// resolving user-defined DOMAIN types to the base type they wrap. A domain's OID
// is dynamic and unknown to pgCategoryForOID, so a domain-over-numeric column
// would fall through to CatText (a JSON string) on the CDC path while the snapshot
// classifies it by its base type (DatabaseTypeName reports "NUMERIC") and renders
// a number — a byte divergence on the first CDC update after snapshot. Resolving
// the domain to its base makes both paths agree. Lookups are cached; the CDC
// stream is single-goroutine, so no lock is needed. A nil resolver (tests, or if
// the connection is unavailable) falls back to pgCategoryForOID via columnCategory.
type oidCategoryResolver struct {
	cache map[uint32]sql.JSONCategory
	// lookupType returns oid's pg_type (typtype, typbasetype). It's a field so a
	// test can inject a transient error; production wires it to the DB in
	// newOIDCategoryResolver.
	lookupType func(ctx context.Context, oid uint32) (typtype string, typbasetype uint32, err error)
}

func newOIDCategoryResolver(db *gosql.DB) *oidCategoryResolver {
	return &oidCategoryResolver{
		cache: make(map[uint32]sql.JSONCategory),
		lookupType: func(ctx context.Context, oid uint32) (string, uint32, error) {
			var typtype string
			var typbasetype uint32
			err := db.QueryRowContext(ctx,
				"SELECT typtype, typbasetype FROM pg_type WHERE oid = $1", oid).Scan(&typtype, &typbasetype)
			return typtype, typbasetype, err
		},
	}
}

// category returns the JSON category for oid, resolving a domain to its base type.
func (r *oidCategoryResolver) category(ctx context.Context, oid uint32) sql.JSONCategory {
	// A base type pgCategoryForOID already recognizes needs no lookup.
	if c := pgCategoryForOID(oid); c != sql.CatText {
		return c
	}
	if c, ok := r.cache[oid]; ok {
		return c
	}
	c, resolved := r.resolveDomain(ctx, oid)
	// Cache only a REAL classification. A transient pg_type lookup error resolves
	// to CatText for THIS value but must NOT be cached: caching it would render
	// every later value of a domain-over-numeric/bool/json column as a JSON string
	// for the rest of the session — diverging from the snapshot (which classifies
	// by base type) and from other nodes, which defeats dedup. Leaving it uncached
	// makes the next value retry the lookup.
	if resolved {
		r.cache[oid] = c
	}
	return c
}

// resolveDomain follows a chain of domains (a domain can wrap another domain) to
// the underlying base type and classifies by it. The bool reports whether the
// classification is REAL and cacheable: true when every pg_type lookup answered
// (the OID is a base type, a resolved domain, or a bounded-out chain), false when
// a lookup ERRORED — a transient blip that must not be cached as CatText.
// Anything that is not a domain keeps the CatText default — the pre-fix behavior.
func (r *oidCategoryResolver) resolveDomain(ctx context.Context, oid uint32) (sql.JSONCategory, bool) {
	cur := oid
	for range 16 { // bound the walk; a domain chain is short and can't cycle
		typtype, typbasetype, err := r.lookupType(ctx, cur)
		if err != nil {
			// A relation-message OID always exists in pg_type, so this is a
			// transient failure (connection/timeout), not a missing type: don't
			// cache, retry on the next value.
			return sql.CatText, false
		}
		if typtype != "d" || typbasetype == 0 {
			return pgCategoryForOID(cur), true // not a domain — classify by this OID
		}
		cur = typbasetype // domain over cur's base — follow it
	}
	return sql.CatText, true // chain too deep — a bounded (non-error) result, cacheable
}

// columnCategory resolves a column's OID to a category, using the domain-aware
// resolver when present and falling back to the plain OID map otherwise.
func columnCategory(ctx context.Context, r *oidCategoryResolver, oid uint32) sql.JSONCategory {
	if r == nil {
		return pgCategoryForOID(oid)
	}
	return r.category(ctx, oid)
}

// unchangedResolver fetches the current text values of columns a pgoutput UPDATE
// left unchanged (Postgres omits unchanged TOASTed values from the new tuple),
// keyed by lowercased column name, by re-selecting the row by primary key.
type unchangedResolver func(ctx context.Context, table string, pk map[string]string, cols []string) (map[string]string, error)

// fillUnchanged re-selects the current values of columns an UPDATE left unchanged
// and merges them into m so the emitted row is complete. On any failure it logs
// and leaves those columns absent (the pre-fix behavior, now visible) rather than
// dropping the whole row.
//
// m is keyed by lowercased column name, but the re-SELECT must quote each
// column's PHYSICAL (stored-case) identifier: a quoted mixed-case column
// ("CreatedAt") does not resolve from a lowercased "createdat", the re-select
// errors, and that error is swallowed below — the column is then emitted null,
// silently clobbering the real value on every such UPDATE. So translate the
// lowercased column and primary-key names to their physical form (from the
// relation metadata) for the query, then map the results back to the lowercased
// keys m uses.
func fillUnchanged(ctx context.Context, resolve unchangedResolver, rel *pglogrepl.RelationMessage, primaryKey []string, m map[string]any, cols []string) {
	physical := make(map[string]string, len(rel.Columns))
	for _, rc := range rel.Columns {
		physical[strings.ToLower(rc.Name)] = rc.Name
	}
	phys := func(lower string) string {
		if p, ok := physical[lower]; ok {
			return p
		}
		return lower // not in the relation (shouldn't happen) — best effort
	}

	pk := make(map[string]string, len(primaryKey))
	for _, k := range primaryKey {
		lk := strings.ToLower(k)
		v, ok := m[lk]
		if !ok || v == nil {
			zap.L().Warn("cannot re-select unchanged TOAST columns: primary key absent from the tuple",
				zap.String("table", rel.RelationName), zap.String("pk", k))
			return
		}
		pk[phys(lk)] = fmt.Sprint(v)
	}

	// Request physical column names; remember each one's lowercased key so the
	// results merge back into m (which is lowercased-keyed).
	physCols := make([]string, len(cols))
	lowerByPhys := make(map[string]string, len(cols))
	for i, c := range cols {
		p := phys(c)
		physCols[i] = p
		lowerByPhys[p] = c
	}

	vals, err := resolve(ctx, rel.Namespace+"."+rel.RelationName, pk, physCols)
	if err != nil {
		zap.L().Warn("re-select of unchanged TOAST columns failed; emitting row without them",
			zap.String("table", rel.RelationName), zap.Error(err))
		return
	}
	for c, v := range vals {
		if lc, ok := lowerByPhys[c]; ok {
			m[lc] = v
		} else {
			m[strings.ToLower(c)] = v
		}
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
	catResolver *oidCategoryResolver,
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
		cat[strings.ToLower(rc.Name)] = columnCategory(ctx, catResolver, rc.DataType)
	}
	// bytea arrives as "\xDEADBEEF" text; decode CatBinary columns to raw bytes so
	// the payload renders base64. Safe to mutate m in place here — the key was
	// already computed above from the \x hex form, so it stays byte-stable and
	// matches the snapshot path.
	for name, c := range cat {
		if c == sql.CatBinary {
			m[name] = sql.DecodeByteaText(m[name])
		}
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
	tables []string,
	resumeProgress *dialectpb.SnapshotProgress,
	lsn pglogrepl.LSN,
	epoch uint64,
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

	for _, table := range tables {
		if completed[table] {
			zap.L().Info("snapshot: skipping already-completed table",
				zap.String("table", table),
			)
			continue
		}
		if err := d.snapshotTable(ctx, db, config, table, batchSize, progress, lsn, epoch, pr, po); err != nil {
			return fmt.Errorf("snapshot: table %s: %w", table, err)
		}
		progress.CompletedTables = append(progress.CompletedTables, table)
		delete(progress.LastPkByTable, table)
		completed[table] = true

		if err := emitSnapshotProgress(ctx, po, lsn, progress, epoch); err != nil {
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
	epoch uint64,
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

		// Stamp the snapshot rows with the refresh epoch (a re-snapshot bumps it;
		// the initial snapshot is epoch 1) so a closing refresh-boundary marker
		// can sweep the rows this positive enumeration could not re-emit.
		stampGeneration(entities, epoch)

		// Advance the resume cursor to this batch, then carry the encoded
		// checkpoint INLINE on the batch proposal (Proposal.Position) so the rows
		// and their checkpoint commit in ONE raft entry. Atomic apply means a
		// crash can never land between a committed batch and its checkpoint — the
		// effectively-once gap a separate position proposal left open for snapshot
		// rows (SourceSeq 0, so the streaming dedup can't cover them). No
		// out-of-band emitSnapshotProgress for a batch.
		lastPK = batchLastPK
		haveLastPK = true
		progress.LastPkByTable[table] = lastPK

		posBytes, err := encodePosition(lsn, progress, epoch)
		if err != nil {
			return err
		}
		p := &cluster.Proposal{Entities: entities, Position: posBytes}
		select {
		case pr <- p:
		case <-ctx.Done():
			return ctx.Err()
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
			break
		}
	}

	return nil
}

// snapshotColumnMeta returns a table's column names (in order) and each column's
// JSON category, read from a zero-row probe (SELECT * … LIMIT 0) inside the given
// snapshot transaction. readBatch casts the value columns to text, which would
// erase the real types from the result set's ColumnTypes, so the categories are
// captured here from the untyped-cast probe instead.
func snapshotColumnMeta(ctx context.Context, tx *gosql.Tx, quotedTable string) ([]string, []sql.JSONCategory, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", quotedTable))
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	cats := make([]sql.JSONCategory, len(colTypes))
	for i, ct := range colTypes {
		cats[i] = pgCategoryForTypeName(ct.DatabaseTypeName())
	}
	return columns, cats, nil
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

	// Discover the columns and their real JSON categories once per batch via a
	// zero-row probe. The value query then casts every column to text so the
	// snapshot emits byte-identical payloads to the CDC path (pgoutput is text): a
	// timestamp/date/bytea/numeric column otherwise diverges — pgx typed-decodes it
	// (time.Time/[]byte/float64) and json.Marshal renders it differently than the
	// Postgres text form, so a column's bytes would flip on its first CDC update
	// after snapshot. Categories must come from the real types, not the text cast.
	columns, cats, err := snapshotColumnMeta(ctx, tx, quotedTable)
	if err != nil {
		return nil, "", 0, err
	}
	selected := make([]string, len(columns))
	for i, c := range columns {
		selected[i] = quoteIdent(c) + "::text AS " + quoteIdent(c)
	}
	selectList := strings.Join(selected, ", ")

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
		// WHERE/ORDER BY reference the real columns, so keyset ordering and the
		// cursor comparison are unaffected by the text cast in the SELECT list.
		query = fmt.Sprintf(
			"SELECT %s FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
			selectList, quotedTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "), orderBy, batchSize,
		)
	} else {
		query = fmt.Sprintf(
			"SELECT %s FROM %s ORDER BY %s LIMIT %d",
			selectList, quotedTable, orderBy, batchSize,
		)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", 0, err
	}
	defer func() { _ = rows.Close() }()

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
			// bytea arrives ::text as "\xDEADBEEF"; decode it to raw bytes for the
			// payload so CatBinary renders base64. The key (m) keeps the \x hex form
			// unchanged, so existing entity keys stay byte-stable.
			if cats[i] == sql.CatBinary {
				raw[lc] = sql.DecodeByteaText(v)
			} else {
				raw[lc] = v
			}
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
	epoch uint64,
) error {
	bs, err := encodePosition(lsn, progress, epoch)
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
// in SQL queries. "public.orders" becomes "public"."orders". Delegates to the
// shared sqlident seam (the syncable side quotes tables the same way).
func quoteTable(table string) string {
	return sqlident.Postgres.Table(table)
}

// flushPending emits all buffered entities as a single proposal and
// resets the buffer. No-op when the buffer is empty. lsn is the WAL
// position of the message that triggered the flush; it becomes the
// proposal's SourceSeq, a strictly-monotonic per-proposal key the
// ingest worker uses to dedup re-emitted proposals after a crash/flap
// (effectively-once). Monotonic because clientXLogPos only advances, and
// deterministic because a resume re-reads the same messages in the same
// order, producing the same flush LSNs.
func flushPending(ctx context.Context, pending *[]*cluster.Entity, pr chan<- *cluster.Proposal, lsn pglogrepl.LSN, epoch uint64) error {
	if len(*pending) == 0 {
		return nil
	}
	// Stamp every entity with the current refresh epoch so a later
	// refresh-boundary marker can sweep rows a re-snapshot left behind. All
	// entities in a flush share the epoch (it only changes on a reconnect that
	// triggers a gap-recovery re-snapshot).
	stampGeneration(*pending, epoch)
	p := &cluster.Proposal{Entities: *pending, SourceSeq: uint64(lsn)}
	select {
	case pr <- p:
	case <-ctx.Done():
		return ctx.Err()
	}
	*pending = nil
	return nil
}

// stampGeneration sets the reconciling-refresh epoch on every entity in a batch
// (see cluster.Entity.Generation). The whole batch shares one epoch: the worker
// holds a single "current epoch" that only changes on a gap-recovery
// re-snapshot, so stamping at flush time is enough.
func stampGeneration(entities []*cluster.Entity, epoch uint64) {
	for _, e := range entities {
		e.Generation = epoch
	}
}

// emitRefreshBoundary emits the refresh-boundary marker that closes a full
// re-snapshot: a single control entity carrying the topic type and the epoch
// the refresh reached, so a keyed sink sweeps every row still at an older epoch
// (a row deleted at the source in the lost window, never re-emitted). It then
// checkpoints the position with no snapshot progress at this epoch, transitioning
// the worker to streaming; a restart after the marker resumes streaming rather
// than re-running the refresh. Emitted ONLY after an all-tables refresh — never a
// partial added-table backfill, which would sweep the topic's sibling tables.
func emitRefreshBoundary(
	ctx context.Context,
	config *sql.Config,
	pr chan<- *cluster.Proposal,
	po chan<- cluster.Position,
	lsn pglogrepl.LSN,
	epoch uint64,
) error {
	marker := cluster.NewRefreshBoundaryEntity(config.Type, epoch)
	select {
	case pr <- &cluster.Proposal{Entities: []*cluster.Entity{marker}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	bs, err := encodePosition(lsn, nil, epoch)
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
