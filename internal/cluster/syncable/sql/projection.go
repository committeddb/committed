package sql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"

	"github.com/PaesslerAG/jsonpath"
	"go.uber.org/zap"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/internal/cluster/metrics"
)

// Projection is the stateful half of the SQL story: where the plain
// sql syncable lands one row per event (history), a Projection folds
// events into one row per entity (current state) — the fold lives in
// the one place that sees every event exactly in log order. The log
// stays the source of truth: the table is disposable and rebuildable
// by replaying from index 0, and amendments are a fresh table + fresh
// syncable, not ALTER (DDL here is CREATE TABLE IF NOT EXISTS only).
type Projection struct {
	db      *gosql.DB
	config  *ProjectionConfig
	dialect Dialect
	// name is the syncable's TOML name, used as the metric attribute
	// for unmatched-rule ticks (the config ID never reaches this
	// layer).
	name    string
	metrics *metrics.Metrics
	// sources is keyed by topic id to the sources that consume it. The topic is
	// the discriminator; an entity routes to every source on its topic, and each
	// source's When (and, for rule sources, per-rule when) decides whether it
	// folds the event. Several sources may share a topic — that is how one topic
	// splits into different columns (filtered aggregates).
	sources map[string][]*projectionSource
	// delete is the shared prepared DELETE-by-key. It serves sources whose
	// onDelete is "delete-row" (and so any RTBF delete on such a topic). Always
	// prepared: primaryKey is mandatory. Self-healing closure: if an entity's
	// creating event was scrubbed before a fresh replay, surviving events build a
	// partial row and the scrub's delete Actual removes it.
	delete *Delete
}

// projectionSource is the prepared runtime of one ProjectionSource. Exactly one
// of rules (scalar fold), agg (collection fold), or lkp (dimension) is set.
type projectionSource struct {
	topic    string
	keyPath  string
	onDelete string
	// when is the source-level filter (empty = consume every event of the
	// topic), evaluated on upsert before the rules/aggregate apply.
	when  []WhenClause
	rules []*projectionStmt
	// clear is the prepared "UPDATE … SET ownedCols = NULL WHERE pk = ?", set
	// only when onDelete == "clear".
	clear    *gosql.Stmt
	clearSQL string
	// agg is the prepared aggregate runtime, set only for a collection-fold
	// source (nil otherwise).
	agg *aggregateRuntime
	// lkp is the prepared lookup (dimension) runtime, set only for a lookup
	// source (nil otherwise).
	lkp *lookupRuntime
}

// aggregateRuntime is the prepared runtime of one ProjectionAggregate: the
// stored (plain) element fields plus the statements that maintain the sidecar
// and re-materialize the parent's array column from it. Enriched fields are not
// stored — they are joined in by the materialize/rebuild SQL.
type aggregateRuntime struct {
	column     string
	elementKey string
	fields     []ProjectionElementField // plain fields only (stored in the sidecar)
	sidecar    string

	upsertSidecar    *gosql.Stmt
	upsertSidecarSQL string
	deleteSidecar    *gosql.Stmt
	deleteSidecarSQL string
	materialize      *gosql.Stmt
	materializeSQL   string
	rebuild          *gosql.Stmt
	rebuildSQL       string
	lookup           *gosql.Stmt
	lookupSQL        string
}

// lookupRuntime is the prepared runtime of one ProjectionLookup: the dimension
// key/fields, the statements maintaining the dimension table, and the dependent
// aggregates to re-materialize when a dimension row changes (the fan-out).
type lookupRuntime struct {
	name      string
	fields    []ProjectionElementField
	dimension string

	upsertDim    *gosql.Stmt
	upsertDimSQL string
	deleteDim    *gosql.Stmt
	deleteDimSQL string

	dependents []*aggregateDependent
}

// aggregateDependent is one aggregate that enriches from a lookup: when a
// dimension key changes, affected finds the parents whose elements reference it
// (on onField) and rebuild re-materializes each (shared with the aggregate's own
// rebuild).
type aggregateDependent struct {
	onField     string
	affected    *gosql.Stmt
	affectedSQL string
	rebuild     *gosql.Stmt
	rebuildSQL  string
}

// projectionStmt pairs one rule with its prepared upsert.
type projectionStmt struct {
	rule ProjectionRule
	SQL  string
	Stmt *gosql.Stmt
}

// NewProjection constructs a Projection. m may be nil (no metrics);
// name is the syncable's TOML name for metric attribution.
func NewProjection(d *DB, config *ProjectionConfig, m *metrics.Metrics, name string) *Projection {
	return &Projection{db: d.DB, config: config, dialect: d.dialect, metrics: m, name: name}
}

// projectionIdentity is the SyncableIdentity of a projection config — its source
// topics, database, and table. It makes the projection's checkpoint meaningful; a
// re-POST that changes it re-points to a destination whose inherited checkpoint is
// stale. Used by the config-alone schema parse (SchemaFromConfig). Reads the
// shorthand Topic when Sources is not yet folded, so it is correct whether or not
// applyDefaults has run.
func projectionIdentity(c *ProjectionConfig) SyncableIdentity {
	topics := make([]string, 0, len(c.Sources))
	for _, s := range c.Sources {
		topics = append(topics, s.Topic)
	}
	if len(topics) == 0 && c.Topic != "" {
		topics = append(topics, c.Topic)
	}
	return SyncableIdentity{Topics: topics, Database: c.DatabaseID, Table: c.Table}
}

// Teardown implements cluster.Teardownable: it drops the projection's
// destination table (DROP TABLE IF EXISTS), the destructive mirror of Init's
// CREATE. It is idempotent — dropping an already-absent table is a no-op — and
// reconstructable from the persisted config alone (it needs only the table
// name + DB handle), which is what the delete/rebuild paths rely on. It never
// touches prepared statements or the connection pool; call Close for those.
func (p *Projection) Teardown() error {
	// Self-bounded — see Syncable.Teardown for the rationale.
	ctx, cancel := context.WithTimeout(context.Background(), teardownTimeout)
	defer cancel()

	p.config.applyDefaults()
	// Drop each aggregate source's sidecar, then the projection table. Order is
	// not load-bearing (DROP IF EXISTS is independent), but dropping sidecars
	// first keeps teardown's footprint a strict subset of Init's.
	for _, src := range p.config.Sources {
		var housekeeping string
		switch {
		case src.Aggregate != nil:
			housekeeping = sidecarName(p.config.Table, src.Aggregate.Column)
		case src.Lookup != nil:
			housekeeping = dimensionName(p.config.Table, src.Lookup.Name)
		default:
			continue
		}
		drop := p.dialect.DropDDL(&Config{Table: housekeeping})
		if _, err := p.db.ExecContext(ctx, drop); err != nil {
			return fmt.Errorf("teardown [%s]: %w", drop, err)
		}
	}
	dropString := p.dialect.DropDDL(p.config.ddlConfig())
	if _, err := p.db.ExecContext(ctx, dropString); err != nil {
		return fmt.Errorf("teardown [%s]: %w", dropString, err)
	}
	return nil
}

func (p *Projection) Init() error {
	// Re-validate even though ParseConfig already did: directly
	// constructed configs (tests, future callers) must hit the same
	// wall before any DDL reaches the destination database.
	p.config.applyDefaults()
	if err := validateProjectionConfig(p.config); err != nil {
		return err
	}

	ddlConfig := p.config.ddlConfig()
	ddlString := p.dialect.CreateDDL(ddlConfig)
	if _, err := p.db.Exec(ddlString); err != nil {
		return fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	// Prepare per-source statements. enrichRefs collects, per lookup name, the
	// aggregates that enrich from it (their spec + on-field), so the second pass
	// can wire each lookup's fan-out to its dependents' rebuilds.
	p.sources = make(map[string][]*projectionSource, len(p.config.Sources))
	var lookupSources []*projectionSource
	enrichRefs := map[string][]enrichRef{}
	for si, src := range p.config.Sources {
		ps := &projectionSource{topic: src.Topic, keyPath: src.KeyPath, onDelete: src.OnDelete, when: src.When}
		switch {
		case src.Aggregate != nil:
			spec := p.config.aggregateSpec(src.Aggregate)
			agg, err := p.initAggregate(si, src, spec)
			if err != nil {
				return err
			}
			ps.agg = agg
			// Register one ref per distinct (lookup, on) this aggregate enriches.
			seen := map[[2]string]bool{}
			for _, f := range src.Aggregate.Element {
				if !f.enriched() {
					continue
				}
				k := [2]string{f.Lookup, f.On}
				if seen[k] {
					continue
				}
				seen[k] = true
				enrichRefs[f.Lookup] = append(enrichRefs[f.Lookup], enrichRef{agg: agg, spec: spec, onField: f.On})
			}
		case src.Lookup != nil:
			lkp, err := p.initLookup(si, src)
			if err != nil {
				return err
			}
			ps.lkp = lkp
			lookupSources = append(lookupSources, ps)
		default:
			for i, r := range src.Rules {
				sqlString := p.dialect.CreateSQL(p.config.ruleConfig(r))
				stmt, err := p.db.Prepare(sqlString)
				if err != nil {
					return fmt.Errorf("prepare source %d (topic %q) rule %d sql [%s]: %w", si+1, src.Topic, i+1, sqlString, err)
				}
				ps.rules = append(ps.rules, &projectionStmt{rule: r, SQL: sqlString, Stmt: stmt})
			}
			if src.OnDelete == onDeleteClear {
				ps.clearSQL = p.dialect.CreateClearSQL(ddlConfig, src.ownedColumns())
				clearStmt, err := p.db.Prepare(ps.clearSQL)
				if err != nil {
					return fmt.Errorf("prepare source %d (topic %q) clear sql [%s]: %w", si+1, src.Topic, ps.clearSQL, err)
				}
				ps.clear = clearStmt
			}
		}
		p.sources[src.Topic] = append(p.sources[src.Topic], ps)
	}

	// Second pass: wire each lookup's fan-out. For every aggregate that enriches
	// from this lookup, prepare the affected-parents query and point it at that
	// aggregate's rebuild, so a dimension change re-materializes its dependents.
	for _, ps := range lookupSources {
		for _, ref := range enrichRefs[ps.lkp.name] {
			affSQL := p.dialect.CreateAggregateAffectedParentsSQL(ref.spec, ref.onField)
			affStmt, err := p.db.Prepare(affSQL)
			if err != nil {
				return fmt.Errorf("prepare lookup %q affected-parents sql [%s]: %w", ps.lkp.name, affSQL, err)
			}
			ps.lkp.dependents = append(ps.lkp.dependents, &aggregateDependent{
				onField:     ref.onField,
				affected:    affStmt,
				affectedSQL: affSQL,
				rebuild:     ref.agg.rebuild,
				rebuildSQL:  ref.agg.rebuildSQL,
			})
		}
	}

	// Shared row-delete (onDelete=delete-row, and any RTBF delete on such a topic).
	deleteString := p.dialect.CreateDeleteSQL(ddlConfig)
	deleteStmt, err := p.db.Prepare(deleteString)
	if err != nil {
		return fmt.Errorf("prepare delete sql [%s]: %w", deleteString, err)
	}
	p.delete = &Delete{deleteString, deleteStmt}

	return nil
}

// enrichRef records, for the fan-out wiring, that aggregate agg (with the given
// spec) enriches on element field onField from some lookup.
type enrichRef struct {
	agg     *aggregateRuntime
	spec    AggregateSpec
	onField string
}

// initLookup creates one lookup source's dimension table and prepares its upsert
// and delete (ordinary key shapes reusing the dialect's CreateSQL /
// CreateDeleteSQL). The fan-out wiring (dependents) is attached in Init's second
// pass, once every aggregate is built.
func (p *Projection) initLookup(si int, src ProjectionSource) (*lookupRuntime, error) {
	lk := src.Lookup
	spec := p.config.lookupSpec(lk)
	where := fmt.Sprintf("source %d (topic %q) lookup %q", si+1, src.Topic, lk.Name)

	ddl := p.dialect.CreateLookupDimensionDDL(spec)
	if _, err := p.db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("%s dimension ddl [%s]: %w", where, ddl, err)
	}

	rt := &lookupRuntime{
		name:      lk.Name,
		fields:    lk.Fields,
		dimension: spec.Dimension,
	}
	dimConfig := dimensionConfig(spec.Dimension)
	var err error
	rt.upsertDimSQL = p.dialect.CreateSQL(dimConfig)
	if rt.upsertDim, err = p.db.Prepare(rt.upsertDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension upsert [%s]: %w", where, rt.upsertDimSQL, err)
	}
	rt.deleteDimSQL = p.dialect.CreateDeleteSQL(dimConfig)
	if rt.deleteDim, err = p.db.Prepare(rt.deleteDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension delete [%s]: %w", where, rt.deleteDimSQL, err)
	}
	return rt, nil
}

// initAggregate creates one aggregate source's sidecar table and prepares the
// five statements that maintain it and re-materialize the parent column: the
// sidecar upsert and delete (ordinary key shapes, reusing the dialect's
// CreateSQL / CreateDeleteSQL), the parent-key lookup (read back a deleted
// child's parent), and the materialize / rebuild (re-aggregate the parent's
// array from the sidecar on upsert / delete).
func (p *Projection) initAggregate(si int, src ProjectionSource, spec AggregateSpec) (*aggregateRuntime, error) {
	ag := src.Aggregate
	where := fmt.Sprintf("source %d (topic %q) aggregate %q", si+1, src.Topic, ag.Column)

	ddl := p.dialect.CreateAggregateSidecarDDL(spec)
	if _, err := p.db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("%s sidecar ddl [%s]: %w", where, ddl, err)
	}

	rt := &aggregateRuntime{
		column:     ag.Column,
		elementKey: ag.ElementKey,
		fields:     plainElementFields(ag.Element), // enriched fields are joined in, not stored
		sidecar:    spec.Sidecar,
	}
	scConfig := sidecarConfig(spec.Sidecar)
	prepare := func(label, sqlString string) (*gosql.Stmt, error) {
		stmt, err := p.db.Prepare(sqlString)
		if err != nil {
			return nil, fmt.Errorf("%s prepare %s [%s]: %w", where, label, sqlString, err)
		}
		return stmt, nil
	}

	var err error
	rt.upsertSidecarSQL = p.dialect.CreateSQL(scConfig)
	if rt.upsertSidecar, err = prepare("sidecar upsert", rt.upsertSidecarSQL); err != nil {
		return nil, err
	}
	rt.deleteSidecarSQL = p.dialect.CreateDeleteSQL(scConfig)
	if rt.deleteSidecar, err = prepare("sidecar delete", rt.deleteSidecarSQL); err != nil {
		return nil, err
	}
	rt.lookupSQL = p.dialect.CreateAggregateParentLookupSQL(spec)
	if rt.lookup, err = prepare("parent lookup", rt.lookupSQL); err != nil {
		return nil, err
	}
	rt.materializeSQL = p.dialect.CreateAggregateMaterializeSQL(spec)
	if rt.materialize, err = prepare("materialize", rt.materializeSQL); err != nil {
		return nil, err
	}
	rt.rebuildSQL = p.dialect.CreateAggregateRebuildSQL(spec)
	if rt.rebuild, err = prepare("rebuild", rt.rebuildSQL); err != nil {
		return nil, err
	}
	return rt, nil
}

func (p *Projection) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	// Skip an Actual that carries no entity for one of our source topics before
	// BeginTx, so a non-matching Actual costs no transaction.
	relevant := false
	for _, e := range a.Entities {
		if _, ok := p.sources[e.Type.ID]; ok {
			relevant = true
			break
		}
	}
	if !relevant {
		return false, nil
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		// A failed BeginTx returns the driver's raw connect error, which embeds
		// user=/database=/host:port; redact it (as every other Sync driver call
		// is) so the replicated stuck status and any permanent dead-letter carry
		// only the classifier. Transient: a begin failure is a connection issue to
		// retry and surface as stuck, never a permanent dead-letter.
		return false, execFailure("[sql-projection.apply] begin", err, false)
	}

	for _, e := range a.Entities {
		for _, src := range p.sources[e.Type.ID] {
			if err := p.applyEntity(ctx, tx, src, e); err != nil {
				_ = tx.Rollback()
				return false, err
			}
		}
	}

	// CAVEAT: tx.Commit() takes no context — see the matching comment
	// in Syncable.Sync (sql.go): a hung commit is uninterruptible, a
	// database/sql limitation.
	if err := tx.Commit(); err != nil {
		// A deferred-constraint violation surfaces here (past the per-exec
		// RedactedError coverage) and can echo Key (col)=(value); redact it. No
		// rollback: a failed Commit already finalized the tx and freed the
		// connection, so a Rollback now only returns ErrTxDone and would mask
		// this error.
		return false, execFailure("[sql-projection.apply] commit", err, p.dialect.IsPermanent(err))
	}

	return true, nil
}

func (p *Projection) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		// Redact the raw connect error (user=/database=/host:port); transient — see
		// the matching note in Sync.
		return false, execFailure("[sql-projection.apply] begin", err, false)
	}

	for _, a := range as {
		for _, e := range a.Entities {
			for _, src := range p.sources[e.Type.ID] {
				if err := p.applyEntity(ctx, tx, src, e); err != nil {
					_ = tx.Rollback()
					return false, err
				}
			}
		}
	}

	zap.L().Debug("sql projection batch committing", zap.Int("batch_size", len(as)))
	if err := tx.Commit(); err != nil {
		// A deferred-constraint violation surfaces here (past the per-exec
		// RedactedError coverage) and can echo Key (col)=(value); redact it. No
		// rollback: a failed Commit already finalized the tx and freed the
		// connection, so a Rollback now only returns ErrTxDone and would mask
		// this error.
		return false, execFailure("[sql-projection.apply] commit", err, p.dialect.IsPermanent(err))
	}

	return true, nil
}

// columnType returns the declared SQL type of a projection column, or "" if the
// name is not a declared column. coerceForColumn treats "" as text-binding — the
// safe default — so an unknown column falls back to the pre-typed all-strings
// shape rather than erroring. The column set is small and fixed, so a linear scan
// per bind is cheaper than maintaining a map.
func (p *Projection) columnType(name string) string {
	for _, c := range p.config.Columns {
		if c.Name == name {
			return c.SQLType
		}
	}
	return ""
}

// applyEntity applies one entity from source src to an open transaction. A
// delete follows the source's onDelete (see applyDelete); the sentinel payload
// is never unmarshaled. Any other entity is matched against that source's rules
// and every matching rule's upsert executes in manifest order — each rule sets
// only its own columns, so two sources fold into one row without clobbering.
// Zero matching rules → zero SQL (no ghost rows) plus an unmatched metric tick.
// Returns cluster.Permanent for non-retryable failures so the worker skips
// rather than retries. The caller owns the transaction.
func (p *Projection) applyEntity(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	// A refresh-boundary marker (reconciling full-refresh) is a no-op for a
	// projection: one source entity fans out to many/aggregated sink rows, so a
	// topic-level generation sweep does not map onto the projection's shape.
	// Projection reconciliation is a separate, later design; until then a gap
	// recovery on a projected topic still needs an operator rebuild.
	switch v := e.Variant(); v {
	case cluster.EntityVariantRefresh:
		// A re-snapshot boundary (generation > 1) just recovered a source gap that
		// this projection could NOT reconcile (one source entity fans out to
		// many/aggregated sink rows, so a topic sweep doesn't map onto it) — the
		// gap's source-side deletes are not reflected, and recovery is a rebuild.
		// The initial snapshot (generation 1) has nothing to reconcile, so stay quiet.
		if e.Generation > 1 {
			zap.L().Warn("refresh boundary on a projection sink is NOT reconciled: a re-snapshot recovered a source gap, so rows the source deleted in that window (RTBF/GDPR erasures among them) remain here. A rebuild does NOT fix this (the delete was never captured, and the marker no-ops on replay too) — manual reconciliation is required (and manual erasure of any source-side-forgotten subject) until projection reconciliation is implemented.",
				zap.String("syncable", p.name), zap.String("topic", e.Type.ID), zap.Uint64("generation", e.Generation))
		}
		return nil
	case cluster.EntityVariantDelete:
		// A delete carries no payload, so the source-level when cannot be
		// evaluated — route it to every source on the topic. An aggregate's
		// remove is keyed by the child Key in its sidecar, so it self-selects:
		// the one source that folded this child removes its element, the others
		// are no-ops. (For a split by when, this is how the right column shrinks.)
		if src.lkp != nil {
			return p.removeFromDimension(ctx, tx, src, e)
		}
		if src.agg != nil {
			if src.onDelete == onDeleteIgnore {
				return nil
			}
			return p.removeFromAggregate(ctx, tx, src, e)
		}
		return p.applyDelete(ctx, tx, src, e)
	case cluster.EntityVariantRow:
		// Fall through to the row fold below.
	default:
		// Future-proofing: a variant this binary does not implement
		// dead-letters loudly instead of folding as a row.
		return cluster.Permanent(fmt.Errorf(
			"[sql-projection.apply] entity variant %q is not supported by this binary (topic %q); upgrade the node before syncing this topic",
			v, e.Type.ID))
	}

	// UseNumber keeps every numeric leaf as its exact source digits (json.Number)
	// instead of a lossy float64, so a Snowflake id above 2^53 or a high-precision
	// decimal survives the fold. The bind sites below (key, rule values) then hand
	// those digits to the driver via coerceForColumn / bindable, never a float64.
	dec := json.NewDecoder(bytes.NewReader(e.Data))
	dec.UseNumber()
	var jsonData any
	if err := dec.Decode(&jsonData); err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.apply] unmarshal entity data: %w", err))
	}

	// Source-level when prefilter: a source consumes only the events it matches,
	// so several sources can split one topic into different columns. An empty
	// when matches every event of the topic.
	if !matchWhen(src.when, jsonData) {
		return nil
	}

	if src.lkp != nil {
		return p.applyLookup(ctx, tx, src, jsonData, e)
	}
	if src.agg != nil {
		return p.applyAggregate(ctx, tx, src, jsonData, e)
	}

	var matched []*projectionStmt
	for _, r := range src.rules {
		if matchWhen(r.rule.When, jsonData) {
			matched = append(matched, r)
		}
	}
	if len(matched) == 0 {
		// No ghost rows: zero matching rules → zero SQL. The tick is
		// the signal that a new event variant shipped without a rule.
		zap.L().Debug("[sql-projection] event matched no rules",
			zap.String("syncable", p.name), zap.String("topic", src.topic))
		if p.metrics != nil {
			p.metrics.SyncRulesUnmatched(p.name, src.topic)
		}
		return nil
	}

	// Key resolution is deliberately lazy — after matching — so an
	// unmatched foreign event missing the keyPath is a non-event, not
	// a dead letter. A matched event without a key is a permanent
	// misconfiguration.
	key, err := jsonpath.Get(src.keyPath, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.apply] keyPath [%s]: %w", src.keyPath, err))
	}

	for _, r := range matched {
		values := make([]any, 0, len(r.rule.Set)+1)
		// The key binds into the primaryKey column; coerce it to that column's
		// declared type so a numeric key reaches the driver as a native scalar
		// (or exact text), not a raw json.Number.
		values = append(values, coerceForColumn(key, p.columnType(p.config.PrimaryKey)))
		for _, s := range r.rule.Set {
			switch {
			case s.From != "":
				v, err := jsonpath.Get(s.From, jsonData)
				if err != nil {
					return cluster.Permanent(fmt.Errorf("[sql-projection.apply] jsonpath [%s]: %w", s.From, err))
				}
				values = append(values, coerceForColumn(v, p.columnType(s.Column)))
			case s.Null:
				values = append(values, nil)
			default:
				values = append(values, s.Value)
			}
		}
		args := p.dialect.BindArgs(values)
		if _, err := tx.StmtContext(ctx, r.Stmt).ExecContext(ctx, args...); err != nil {
			return execFailure(fmt.Sprintf("[sql-projection.apply] exec [%s]", r.SQL), err, p.dialect.IsPermanent(err))
		}
	}
	return nil
}

// applyDelete honors a delete entity per its source's onDelete: ignore drops it;
// clear NULLs the source's owned columns for the keyed row (the folded row
// survives); delete-row removes the row entirely. The single bound argument is
// the entity Key, so the sentinel payload is never unmarshaled.
func (p *Projection) applyDelete(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	var stmt *gosql.Stmt
	var sqlStr string
	switch src.onDelete {
	case onDeleteIgnore:
		return nil
	case onDeleteClear:
		stmt, sqlStr = src.clear, src.clearSQL
	default: // onDeleteRow
		stmt, sqlStr = p.delete.Stmt, p.delete.SQL
	}
	if stmt == nil {
		// Do NOT put e.Key in this message — it lands in a permanent, Raft-replicated
		// dead-letter record, and for an RTBF delete the key is the subject being
		// erased. The dead-letter's syncable id + raft index identify the row.
		return cluster.Permanent(fmt.Errorf(
			"[sql-projection.apply] cannot honor delete: no statement prepared (topic %q)",
			src.topic))
	}
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, string(e.Key)); err != nil {
		return execFailure(fmt.Sprintf("[sql-projection.apply] exec [%s]", sqlStr), err, p.dialect.IsPermanent(err))
	}
	return nil
}

// applyAggregate folds one child upsert into its parent's array column. It
// records the child in the sidecar (keyed by the child's entity Key, so a
// re-delivered child replaces rather than duplicates) and then re-materializes
// the parent's column from the sidecar — an upsert, so a child arriving before
// its spine lands the collection on a fresh partial row. The parent key binds
// both materialize placeholders.
func (p *Projection) applyAggregate(ctx context.Context, tx *gosql.Tx, src *projectionSource, jsonData any, e *cluster.Entity) error {
	ag := src.agg

	parentKey, err := jsonpath.Get(src.keyPath, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] keyPath [%s]: %w", src.keyPath, err))
	}

	// Capture the child's prior parent before the sidecar upsert overwrites it. A
	// child re-delivered under a different parent (re-parenting) must have its old
	// parent rebuilt too, or that parent's array keeps an element the child no
	// longer belongs to — and never self-corrects.
	oldParent, hadOldParent, err := p.aggPriorParent(ctx, tx, ag, string(e.Key))
	if err != nil {
		return err
	}

	elementKey, err := jsonpath.Get(ag.elementKey, jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] elementKey [%s]: %w", ag.elementKey, err))
	}
	element := make(map[string]any, len(ag.fields))
	for _, f := range ag.fields {
		v, err := jsonpath.Get(f.From, jsonData)
		if err != nil {
			return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] element field %q from [%s]: %w", f.Field, f.From, err))
		}
		element[f.Field] = v
	}
	elementJSON, err := json.Marshal(element)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.aggregate] marshal element: %w", err))
	}

	// Sidecar columns are text/JSON; bind the keys as strings (elementKey is
	// stored as text and ordered with an optional numeric cast) so a numeric
	// jsonpath value never mismatches the column type.
	pk := bindable(parentKey)
	scValues := []any{string(e.Key), pk, keyString(elementKey), string(elementJSON)}
	if err := p.aggExec(ctx, tx, ag.upsertSidecar, ag.upsertSidecarSQL, p.dialect.BindArgs(scValues)...); err != nil {
		return err
	}
	// Materialize the (new) parent's array from the sidecar. Both materialize
	// placeholders bind the parent key (insert value + subquery filter); the
	// dialect repeats the placeholder so the arg shape is uniform.
	if err := p.aggExec(ctx, tx, ag.materialize, ag.materializeSQL, pk, pk); err != nil {
		return err
	}
	// Re-parented: rebuild the old parent so its array drops the moved element.
	// Mirrors removeFromAggregate — an UPDATE that no-ops if the old parent has no
	// row, never a ghost. Skipped when the child is new or its parent is unchanged.
	if hadOldParent && oldParent != keyString(parentKey) {
		return p.aggExec(ctx, tx, ag.rebuild, ag.rebuildSQL, oldParent, oldParent)
	}
	return nil
}

// removeFromAggregate honors a child delete: recover the child's parent from the
// sidecar (a no-op if this source never folded the child — which is how a split
// self-selects), delete the sidecar row, and rebuild the parent's array from
// what remains. The rebuild is an UPDATE, so emptying an absent parent is a
// no-op, never a ghost row.
func (p *Projection) removeFromAggregate(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	ag := src.agg
	childKey := string(e.Key)

	parentKey, folded, err := p.aggPriorParent(ctx, tx, ag, childKey)
	if err != nil {
		return err
	}
	if !folded {
		return nil // this source never folded the child — nothing to remove
	}

	if err := p.aggExec(ctx, tx, ag.deleteSidecar, ag.deleteSidecarSQL, childKey); err != nil {
		return err
	}
	return p.aggExec(ctx, tx, ag.rebuild, ag.rebuildSQL, parentKey, parentKey)
}

// aggPriorParent returns the parent key currently recorded for childKey in the
// sidecar, or ("", false) if this aggregate has never folded the child. It reuses
// the parent-lookup statement, classifying a query error the same way the rest of
// the aggregate path does. Both re-parenting (applyAggregate) and child deletes
// (removeFromAggregate) need the old parent, so the read lives here once.
func (p *Projection) aggPriorParent(ctx context.Context, tx *gosql.Tx, ag *aggregateRuntime, childKey string) (string, bool, error) {
	var parentKey string
	row := tx.StmtContext(ctx, ag.lookup).QueryRowContext(ctx, childKey)
	switch err := row.Scan(&parentKey); err {
	case nil:
		return parentKey, true, nil
	case gosql.ErrNoRows:
		return "", false, nil
	default:
		// The lookup binds childKey (an entity key = an RTBF subject); a driver
		// error can echo it, so route through execFailure (a RedactedError) like
		// the sibling exec sites — otherwise the raw text lands in the replicated
		// dead-letter + stuck status.
		return "", false, execFailure(
			fmt.Sprintf("[sql-projection.aggregate] exec [%s]", ag.lookupSQL), err, p.dialect.IsPermanent(err))
	}
}

// aggExec runs one prepared aggregate statement, classifying a permanent error
// the same way the rule path does.
func (p *Projection) aggExec(ctx context.Context, tx *gosql.Tx, stmt *gosql.Stmt, sqlStr string, args ...any) error {
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...); err != nil {
		return execFailure(fmt.Sprintf("[sql-projection.aggregate] exec [%s]", sqlStr), err, p.dialect.IsPermanent(err))
	}
	return nil
}

// applyLookup folds one dimension-entity upsert: store its key → fields object
// in the dimension table, then fan out — re-materialize every parent whose
// folded children reference this key, so a value that arrives after the facts
// that reference it fills in (and a changed value updates them).
func (p *Projection) applyLookup(ctx context.Context, tx *gosql.Tx, src *projectionSource, jsonData any, e *cluster.Entity) error {
	lk := src.lkp

	fields := make(map[string]any, len(lk.fields))
	for _, f := range lk.fields {
		v, err := jsonpath.Get(f.From, jsonData)
		if err != nil {
			return cluster.Permanent(fmt.Errorf("[sql-projection.lookup] field %q from [%s]: %w", f.Field, f.From, err))
		}
		fields[f.Field] = v
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[sql-projection.lookup] marshal fields: %w", err))
	}

	// The dimension key is the entity's own Key — the value aggregate elements
	// reference in `on`, and the only key a (payload-less) delete can use.
	key := string(e.Key)
	dimValues := []any{key, string(fieldsJSON)}
	if err := p.aggExec(ctx, tx, lk.upsertDim, lk.upsertDimSQL, p.dialect.BindArgs(dimValues)...); err != nil {
		return err
	}
	return p.fanOut(ctx, tx, lk, key)
}

// removeFromDimension honors a dimension-entity delete: drop the dimension row,
// then fan out — the parents that referenced it re-materialize, their enriched
// fields now null (the materialize LEFT JOIN finds no dimension row).
func (p *Projection) removeFromDimension(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	lk := src.lkp
	key := string(e.Key)
	if err := p.aggExec(ctx, tx, lk.deleteDim, lk.deleteDimSQL, key); err != nil {
		return err
	}
	return p.fanOut(ctx, tx, lk, key)
}

// fanOut re-materializes every parent whose folded children reference the
// changed dimension key. For each dependent aggregate it collects the affected
// parent keys (fully draining the query before any rebuild — a tx holds one
// connection, so a rebuild cannot run while the cursor is open) and rebuilds
// each. Synchronous in the dimension change's transaction (so the read model is
// consistent at every checkpoint); bounded by the fan-out degree.
func (p *Projection) fanOut(ctx context.Context, tx *gosql.Tx, lk *lookupRuntime, dimKey string) error {
	for _, dep := range lk.dependents {
		// The fan-out query binds dimKey (the changed dimension entity key = an
		// RTBF subject); a driver error can echo it, so route these three egress
		// points through execFailure (a RedactedError) like the sibling exec sites.
		rows, err := tx.StmtContext(ctx, dep.affected).QueryContext(ctx, dimKey)
		if err != nil {
			return execFailure(
				fmt.Sprintf("[sql-projection.lookup] exec [%s]", dep.affectedSQL), err, p.dialect.IsPermanent(err))
		}
		var parents []string
		for rows.Next() {
			var pk string
			if err := rows.Scan(&pk); err != nil {
				_ = rows.Close()
				return execFailure("[sql-projection.lookup] scan affected parent", err, p.dialect.IsPermanent(err))
			}
			parents = append(parents, pk)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return execFailure("[sql-projection.lookup] affected parents", err, p.dialect.IsPermanent(err))
		}
		_ = rows.Close()

		for _, pk := range parents {
			if err := p.aggExec(ctx, tx, dep.rebuild, dep.rebuildSQL, pk, pk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Projection) Close() error {
	// Close every prepared statement; report the first error but
	// always attempt the rest so nothing leaks when one close fails.
	var err error
	closeStmt := func(s *gosql.Stmt) {
		if s != nil {
			if cerr := s.Close(); err == nil {
				err = cerr
			}
		}
	}
	for _, list := range p.sources {
		for _, src := range list {
			for _, r := range src.rules {
				closeStmt(r.Stmt)
			}
			closeStmt(src.clear)
			if src.agg != nil {
				closeStmt(src.agg.upsertSidecar)
				closeStmt(src.agg.deleteSidecar)
				closeStmt(src.agg.lookup)
				closeStmt(src.agg.materialize)
				closeStmt(src.agg.rebuild)
			}
			if src.lkp != nil {
				closeStmt(src.lkp.upsertDim)
				closeStmt(src.lkp.deleteDim)
				// dependents' rebuild stmts belong to the aggregate runtimes (closed
				// above); only the affected-parents queries are the lookup's own.
				for _, dep := range src.lkp.dependents {
					closeStmt(dep.affected)
				}
			}
		}
	}
	if p.delete != nil {
		if cerr := p.delete.Stmt.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
