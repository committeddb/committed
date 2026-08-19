package sql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/committeddb/committed/internal/cluster/syncable/stagestore"

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
	// initCtx is Init's bounded deadline, threaded to the nested
	// aggregate/lookup inits (set at the top of Init, valid only during it).
	initCtx context.Context
	db      *gosql.DB
	config  *ProjectionConfig
	dialect Dialect
	// name is the syncable's TOML name, used as the metric attribute
	// for unmatched-rule ticks (the config ID never reaches this
	// layer).
	name    string
	metrics *metrics.Metrics
	// storeDir is the node's stage-store directory; set by the parser
	// (never by tests constructing directly — those exercise stages via
	// their own tempdir or not at all).
	storeDir string
	// stages/stageStore are the compiled stage graph and its state, set at
	// Init when the config declares stages. stageSinks routes stage deltas
	// to the table sources consuming each stage (from = "<stage>").
	stages     *stageGraph
	stageStore *stagestore.Store
	stageSinks map[string][]*projectionSource
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

// projectionUnmatchedWarnRun is how many CONSECUTIVE unmatched events a
// rule source absorbs before warning that its when clauses probably match
// nothing (exported-adjacent knob deliberately avoided: the number only
// gates a log line). High enough that a topic's occasional foreign
// variants never trip it; low enough to fire early in a misconfigured
// backfill.
const projectionUnmatchedWarnRun = 1000

// projectionSource is the prepared runtime of one ProjectionSource. Exactly one
// of rules (scalar fold), agg (collection fold), or lkp (dimension) is set.
type projectionSource struct {
	topic string
	// keyPaths locate the key value(s) in the payload, positionally aligned
	// with the projection's PrimaryKey columns (one path for a single key,
	// several for a composite).
	keyPaths []string
	onDelete string
	// when is the source-level filter (empty = consume every event of the
	// topic), evaluated on upsert before the rules/aggregate apply.
	when  []WhenClause
	rules []*projectionStmt
	// unmatchedRun counts CONSECUTIVE events that matched no rule (worker
	// goroutine only — no lock). At projectionUnmatchedWarnRun it triggers
	// the one-shot misconfiguration warning; any match resets it.
	unmatchedRun int
	// clear is the prepared "UPDATE … SET ownedCols = NULL WHERE pk = ?", set
	// only when onDelete == "clear".
	clear    *gosql.Stmt
	clearSQL string
	// forEach is the source's fan-out path ("" for a plain source); fe is
	// its prepared reconciliation runtime.
	forEach string
	fe      *forEachRuntime
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
	parentBinds      int
}

// forEachRuntime is the prepared runtime of one forEach source: the
// reconciliation sidecar (parent entity key → fanned row keys, reusing the
// aggregate sidecar shape with its element columns unused) and the three
// statements that maintain it. The fanned rows themselves go through the
// source's ordinary rule statements and the projection's shared delete.
type forEachRuntime struct {
	sidecar          string
	upsertSidecar    *gosql.Stmt
	upsertSidecarSQL string
	deleteSidecar    *gosql.Stmt
	deleteSidecarSQL string
	children         *gosql.Stmt
	childrenSQL      string
}

func (rt *forEachRuntime) closeStmts() {
	for _, st := range []*gosql.Stmt{rt.upsertSidecar, rt.deleteSidecar, rt.children} {
		if st != nil {
			_ = st.Close()
		}
	}
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

	// spineDeps are the enriched SPINE columns fed from this dimension: on a
	// dimension upsert/delete, each runs a direct UPDATE (value, key) — the
	// fan-out half of the join-equivalence contract for scalar enrichment.
	// Simpler than the aggregate dependents: no re-materialization, one
	// indexed UPDATE per dependent.
	spineDeps []*spineDependent
}

// spineDependent is one enriched spine column fed from a lookup dimension:
// the prepared fan-out UPDATE, the dimension field it selects, and the two
// declared column types the canonical-join-space contract coerces through
// (the enriched column's, for the value; the on column's, for the key).
type spineDependent struct {
	column    string
	selectFld string
	colType   string
	onColType string
	update    *gosql.Stmt
	updateSQL string
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
	parentBinds int
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

// SetStoreDir sets the node's stage-store directory — threaded by the
// parser in production; direct constructors (tests, embedders) set it
// before Init when their config declares stages.
func (p *Projection) SetStoreDir(dir string) { p.storeDir = dir }

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
	// If Init fails after preparing one or more statements, close them before
	// returning: the parser discards this half-built Projection, but the
	// statements live server-side on the shared, re-POST-preserved *sql.DB pool,
	// so without this they accumulate on the destination across every
	// restart/reconcile re-parse until it hits its prepared-statement ceiling.
	// Every source is appended to p.sources at the top of the loop below (before
	// its statements are prepared), and the two nested inits self-clean, so Close
	// reaches everything this Init built. Projections prepare far more statements
	// than a plain syncable, so this path matters more here.
	success := false
	defer func() {
		if !success {
			_ = p.Close()
		}
	}()

	// Re-validate even though ParseConfig already did: directly
	// constructed configs (tests, future callers) must hit the same
	// wall before any DDL reaches the destination database.
	p.config.applyDefaults()
	if err := validateProjectionConfig(p.config); err != nil {
		return err
	}

	// One bounded deadline over every destination operation this Init
	// performs (DDL, dimension DDL, index ensures, dozens of prepares) —
	// see InitTimeout for the apply-loop wedge this guards. Threaded to the
	// nested initAggregate/initLookup through the struct field below.
	ctx, cancel := context.WithTimeout(context.Background(), InitTimeout)
	defer cancel()
	p.initCtx = ctx

	ddlConfig := p.config.ddlConfig()
	ddlString := p.dialect.CreateDDL(ddlConfig)
	if _, err := p.db.ExecContext(ctx, ddlString); err != nil {
		return fmt.Errorf("ddl [%s]: %w", ddlString, err)
	}

	// Dimension-DDL pre-pass: enriched RULE statements subquery dimension
	// tables at prepare time, and a rule source may precede its lookup source
	// in manifest order — so every dimension table must exist before any rule
	// prepares. initLookup's own DDL exec later is IF NOT EXISTS-idempotent.
	// Conditional on enrichment so an enrichment-free config's SQL traffic
	// stays byte-identical to before the feature (compat, and the sqlmock
	// suites pin exact sequences).
	hasEnrichment := false
	for _, src := range p.config.Sources {
		for _, r := range src.Rules {
			for _, s := range r.Set {
				if s.IsEnrichment() {
					hasEnrichment = true
				}
			}
		}
	}
	for _, src := range p.config.Sources {
		if !hasEnrichment || src.Lookup == nil {
			continue
		}
		dimDDL := p.dialect.CreateLookupDimensionDDL(p.config.lookupSpec(src.Lookup))
		if _, err := p.db.ExecContext(ctx, dimDDL); err != nil {
			return fmt.Errorf("dimension ddl [%s]: %w", dimDDL, err)
		}
	}

	// Auto-index every enrichment on column (deduped): the dimension fan-out's
	// WHERE would otherwise seq-scan the projection per dimension event.
	indexed := map[string]bool{}
	for _, src := range p.config.Sources {
		for _, r := range src.Rules {
			for _, s := range r.Set {
				if !s.IsEnrichment() || indexed[s.On] {
					continue
				}
				if err := p.dialect.EnsureSpineIndex(ctx, p.db, ddlConfig, s.On); err != nil {
					return err
				}
				indexed[s.On] = true
			}
		}
	}

	if len(p.config.Stages) > 0 {
		if p.storeDir == "" {
			return fmt.Errorf("this projection declares stages but no stage-store directory is configured (storeDir is threaded from the node's data dir; direct constructions must set it)")
		}
		store, reset, err := stagestore.Open(p.storeDir, p.name, stageFingerprint(p.config))
		if err != nil {
			return err
		}
		p.stageStore = store
		p.stages = buildStageGraph(p.config.Stages)
		if reset {
			zap.L().Warn("stage store reset — stage state re-derives as the log replays; pair stage-definition changes with a rebuild so replay starts from index 0",
				zap.String("syncable", p.name), zap.String("path", store.Path()))
		}
	}

	// Prepare per-source statements. enrichRefs collects, per lookup name, the
	// aggregates that enrich from it (their spec + on-field), so the second pass
	// can wire each lookup's fan-out to its dependents' rebuilds.
	p.sources = make(map[string][]*projectionSource, len(p.config.Sources))
	var lookupSources []*projectionSource
	enrichRefs := map[string][]enrichRef{}
	p.stageSinks = map[string][]*projectionSource{}
	for si, src := range p.config.Sources {
		ps := &projectionSource{topic: src.Topic, keyPaths: src.KeyPath, onDelete: src.OnDelete, when: src.When}
		// Register ps NOW, before preparing its statements, so a partway failure
		// leaves the statements it did prepare reachable from p.sources for the
		// error-cleanup defer's Close (a failed nested init self-cleans separately).
		// A stage-fed source registers under its stage in stageSinks instead —
		// its input is stage deltas, not topic entities — but joins p.sources
		// under the empty key too so Close reaches its statements.
		if src.FromStage != "" {
			p.stageSinks[src.FromStage] = append(p.stageSinks[src.FromStage], ps)
		}
		p.sources[src.Topic] = append(p.sources[src.Topic], ps)
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
				if enrich := p.config.ruleEnrichments(r); len(enrich) > 0 {
					sqlString = p.dialect.CreateEnrichedUpsertSQL(p.config.ruleConfig(r), enrich)
				}
				stmt, err := p.db.PrepareContext(p.initCtx, sqlString)
				if err != nil {
					return fmt.Errorf("prepare source %d (topic %q) rule %d sql [%s]: %w", si+1, src.Topic, i+1, sqlString, err)
				}
				ps.rules = append(ps.rules, &projectionStmt{rule: r, SQL: sqlString, Stmt: stmt})
			}
			if src.OnDelete == onDeleteClear {
				ps.clearSQL = p.dialect.CreateClearSQL(ddlConfig, src.ownedColumns())
				clearStmt, err := p.db.PrepareContext(p.initCtx, ps.clearSQL)
				if err != nil {
					return fmt.Errorf("prepare source %d (topic %q) clear sql [%s]: %w", si+1, src.Topic, ps.clearSQL, err)
				}
				ps.clear = clearStmt
			}
			if src.ForEach != "" {
				fe, err := p.initForEach(si, src)
				if err != nil {
					return err
				}
				ps.forEach = src.ForEach
				ps.fe = fe
			}
		}
	}

	// Second pass: wire each lookup's fan-out. For every aggregate that enriches
	// from this lookup, prepare the affected-parents query and point it at that
	// aggregate's rebuild, so a dimension change re-materializes its dependents.
	for _, ps := range lookupSources {
		for _, ref := range enrichRefs[ps.lkp.name] {
			affSQL := p.dialect.CreateAggregateAffectedParentsSQL(ref.spec, ref.onField)
			affStmt, err := p.db.PrepareContext(p.initCtx, affSQL)
			if err != nil {
				return fmt.Errorf("prepare lookup %q affected-parents sql [%s]: %w", ps.lkp.name, affSQL, err)
			}
			ps.lkp.dependents = append(ps.lkp.dependents, &aggregateDependent{
				onField:     ref.onField,
				affected:    affStmt,
				affectedSQL: affSQL,
				rebuild:     ref.agg.rebuild,
				rebuildSQL:  ref.agg.rebuildSQL,
				parentBinds: ref.agg.parentBinds,
			})
		}
	}

	// Wire each lookup's SPINE fan-out: one prepared UPDATE per distinct
	// enriched (column, on, select) referencing it, across every rule source.
	for _, ps := range lookupSources {
		seen := map[string]bool{}
		for _, src := range p.config.Sources {
			for _, r := range src.Rules {
				for _, s := range r.Set {
					if !s.IsEnrichment() || s.Lookup != ps.lkp.name {
						continue
					}
					k := s.Column + "\x00" + s.On + "\x00" + s.Select
					if seen[k] {
						continue
					}
					seen[k] = true
					upSQL := p.dialect.CreateSpineFanOutSQL(ddlConfig, s.Column, s.On)
					upStmt, err := p.db.PrepareContext(p.initCtx, upSQL)
					if err != nil {
						return fmt.Errorf("prepare lookup %q spine fan-out sql [%s]: %w", ps.lkp.name, upSQL, err)
					}
					ps.lkp.spineDeps = append(ps.lkp.spineDeps, &spineDependent{
						column:    s.Column,
						selectFld: s.Select,
						colType:   p.columnType(s.Column),
						onColType: p.columnType(s.On),
						update:    upStmt,
						updateSQL: upSQL,
					})
				}
			}
		}
	}

	// Shared row-delete (onDelete=delete-row, and any RTBF delete on such a topic).
	deleteString := p.dialect.CreateDeleteSQL(ddlConfig)
	deleteStmt, err := p.db.PrepareContext(p.initCtx, deleteString)
	if err != nil {
		return fmt.Errorf("prepare delete sql [%s]: %w", deleteString, err)
	}
	p.delete = &Delete{deleteString, deleteStmt}

	success = true
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
	if _, err := p.db.ExecContext(p.initCtx, ddl); err != nil {
		return nil, fmt.Errorf("%s dimension ddl [%s]: %w", where, ddl, err)
	}

	rt := &lookupRuntime{
		name:      lk.Name,
		fields:    lk.Fields,
		dimension: spec.Dimension,
	}
	// A partway failure below leaves rt unreturned (never stored on its source),
	// so Projection.Close can't reach its statements — close them here.
	success := false
	defer func() {
		if !success {
			rt.closeStmts()
		}
	}()
	dimConfig := dimensionConfig(spec.Dimension)
	var err error
	rt.upsertDimSQL = p.dialect.CreateSQL(dimConfig)
	if rt.upsertDim, err = p.db.PrepareContext(p.initCtx, rt.upsertDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension upsert [%s]: %w", where, rt.upsertDimSQL, err)
	}
	rt.deleteDimSQL = p.dialect.CreateDeleteSQL(dimConfig)
	if rt.deleteDim, err = p.db.PrepareContext(p.initCtx, rt.deleteDimSQL); err != nil {
		return nil, fmt.Errorf("%s prepare dimension delete [%s]: %w", where, rt.deleteDimSQL, err)
	}
	success = true
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
	if _, err := p.db.ExecContext(p.initCtx, ddl); err != nil {
		return nil, fmt.Errorf("%s sidecar ddl [%s]: %w", where, ddl, err)
	}

	rt := &aggregateRuntime{
		column:     ag.Column,
		elementKey: ag.ElementKey,
		fields:     plainElementFields(ag.Element), // enriched fields are joined in, not stored
		sidecar:    spec.Sidecar,
		// materialize and rebuild carry one parent-key placeholder per value
		// column plus one (the inserted key / the WHERE) — bind the parent
		// key that many times, whichever dialect rendered the statement.
		parentBinds: 1 + len(spec.Scalars),
	}
	if spec.Column != "" {
		rt.parentBinds++
	}
	// A partway failure below leaves rt unreturned (never stored on its source),
	// so Projection.Close can't reach its statements — close them here.
	success := false
	defer func() {
		if !success {
			rt.closeStmts()
		}
	}()
	scConfig := sidecarConfig(spec.Sidecar)
	prepare := func(label, sqlString string) (*gosql.Stmt, error) {
		stmt, err := p.db.PrepareContext(p.initCtx, sqlString)
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
	success = true
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
		if p.stages != nil && p.stages.ConsumesTopic(e.Type.ID) {
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
		return false, execFailure("[projection.apply] begin", err, false)
	}

	// Stage folds run FIRST, in one store transaction per Actual: entities
	// route through the stage graph, table sources consuming stages apply
	// their deltas on this same SQL tx, and the frontier advances with the
	// fold. Store commits before SQL: if the SQL commit then fails, the
	// redelivered Actual re-folds idempotently (same inputs, same bytes).
	if p.stages != nil {
		if err := p.foldStages(ctx, tx, a); err != nil {
			_ = tx.Rollback()
			return false, err
		}
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
		return false, execFailure("[projection.apply] commit", err, p.dialect.IsPermanent(err))
	}

	return true, nil
}

func (p *Projection) SyncBatch(ctx context.Context, as []*cluster.Actual) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		// Redact the raw connect error (user=/database=/host:port); transient — see
		// the matching note in Sync.
		return false, execFailure("[projection.apply] begin", err, false)
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
		return false, execFailure("[projection.apply] commit", err, p.dialect.IsPermanent(err))
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
		if src.forEach != "" {
			if src.onDelete == onDeleteIgnore {
				return nil
			}
			return p.cascadeForEachDelete(ctx, tx, src, e)
		}
		return p.applyDelete(ctx, tx, src, e)
	case cluster.EntityVariantRow:
		// Fall through to the row fold below.
	default:
		// Future-proofing: a variant this binary does not implement
		// dead-letters loudly instead of folding as a row.
		return cluster.Permanent(fmt.Errorf(
			"[projection.apply] entity variant %q is not supported by this binary (topic %q); upgrade the node before syncing this topic",
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
		return cluster.Permanent(fmt.Errorf("[projection.apply] unmarshal entity data: %w", err))
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
	if src.forEach != "" {
		return p.applyForEach(ctx, tx, src, jsonData, e)
	}

	matchedAny, _, err := p.applyRowFold(ctx, tx, src, jsonData, nil, nil)
	if err != nil {
		return err
	}
	if !matchedAny {
		// No ghost rows: zero matching rules → zero SQL. The tick is
		// the signal that a new event variant shipped without a rule.
		zap.L().Debug("[projection] event matched no rules",
			zap.String("syncable", p.name), zap.String("topic", src.topic))
		if p.metrics != nil {
			p.metrics.SyncRulesUnmatched(p.name, src.topic)
		}
		// A LONG RUN of consecutive misses is a different signal than the
		// odd foreign variant: it usually means every `when` in the source
		// is wrong — most treacherously a type-mismatched equals (the field
		// case: `equals = "true"` against a JSON boolean matched 0 of
		// 248,854 rows, silently — `equals = true` matched all). Warn once
		// per run so the empty table has a named cause in the logs.
		src.unmatchedRun++
		if src.unmatchedRun == projectionUnmatchedWarnRun {
			zap.L().Warn("[projection] source has matched no rules for a long run of events — if this table should be filling, check the source's when clauses (a type-mismatched `equals` matches nothing silently: `equals = \"true\"` never equals JSON boolean true)",
				zap.String("syncable", p.name), zap.String("topic", src.topic),
				zap.Int("consecutive_unmatched", src.unmatchedRun))
		}
		return nil
	}
	src.unmatchedRun = 0
	return nil
}

// resolveScopedPath resolves a value-position jsonpath in the rule scope: a
// `$parent.` prefix reaches the enclosing event payload (the forEach element
// scope); everything else resolves against data. Outside forEach, parent is
// nil and a $parent path is a loud misconfiguration.
func resolveScopedPath(path string, data, parent any) (any, error) {
	if rest, ok := strings.CutPrefix(path, "$parent"); ok {
		if parent == nil {
			return nil, fmt.Errorf("path [%s]: $parent is only meaningful inside a forEach source", path)
		}
		return jsonpath.Get("$"+rest, parent)
	}
	return jsonpath.Get(path, data)
}

// applyRowFold matches src's rules against data and applies the matched
// sets as one row upsert. data is the rule scope — the event payload for a
// plain source, one array element for a forEach source (whose enclosing
// event payload rides in parent for `$parent.` paths). Returns whether any
// rule matched, so the caller owns the unmatched bookkeeping and forEach
// counts only matched elements in its reconciliation set.
// presetKeys, when non-nil, override key resolution entirely — a
// stage-fed source's row key IS the stage's key (already resolved by the
// fold; an aggregate stage's emit carries only folds, never the key).
func (p *Projection) applyRowFold(ctx context.Context, tx *gosql.Tx, src *projectionSource, data, parent any, presetKeys []any) (bool, string, error) {
	var matched []*projectionStmt
	for _, r := range src.rules {
		if matchWhen(r.rule.When, data) {
			matched = append(matched, r)
		}
	}
	if len(matched) == 0 {
		return false, "", nil
	}

	// Key resolution is deliberately lazy — after matching — so an
	// unmatched foreign event missing the keyPath is a non-event, not
	// a dead letter. A matched event without a key is a permanent
	// misconfiguration. One value per primaryKey column, positionally
	// aligned, each coerced to its own column's declared type.
	keys := presetKeys
	if keys == nil {
		keys = make([]any, len(src.keyPaths))
		for i, kp := range src.keyPaths {
			v, err := resolveScopedPath(kp, data, parent)
			if err != nil {
				return true, "", cluster.Permanent(fmt.Errorf("[projection.apply] keyPath [%s]: %w", kp, err))
			}
			keys[i] = coerceForColumn(v, p.columnType(p.config.PrimaryKey[i]))
		}
	}

	for _, r := range matched {
		// Pass 1: compute the coerced value of every from/value/null entry —
		// enrichment entries need their on column's COERCED value (pass 2), so
		// the plain values must exist first regardless of Set order.
		plain := make(map[string]any, len(r.rule.Set))
		for _, s := range r.rule.Set {
			switch {
			case s.From != "":
				v, err := resolveScopedPath(s.From, data, parent)
				if err != nil {
					return true, "", cluster.Permanent(fmt.Errorf("[projection.apply] jsonpath [%s]: %w", s.From, err))
				}
				plain[s.Column] = coerceForColumn(v, p.columnType(s.Column))
			case s.Expr != "":
				v, err := evalExpr(s.compiled, data, parent)
				if err == nil {
					if r, ok := v.(*big.Rat); ok {
						var text string
						if text, err = formatRat(r); err == nil {
							v = json.Number(text)
						}
					}
				}
				if err != nil {
					return true, "", cluster.Permanent(fmt.Errorf("[projection.apply] expr for column %q: %w", s.Column, err))
				}
				plain[s.Column] = coerceForColumn(v, p.columnType(s.Column))
			case s.Null:
				plain[s.Column] = nil
			case s.IsEnrichment():
				// resolved in pass 2
			default:
				plain[s.Column] = s.Value
			}
		}

		// Pass 2: bind in Set order. An enrichment entry's placeholder sits
		// INSIDE its dimension subquery and binds the canonical string
		// rendering of the on column's coerced value — the canonical-join-
		// space contract: the on column's declared type is the one space both
		// this join and the dimension fan-out compare in, so the rendering
		// comes from the TYPED value, never the raw payload text (which could
		// spell one value several ways).
		values := make([]any, 0, len(r.rule.Set)+len(keys))
		values = append(values, keys...)
		for _, s := range r.rule.Set {
			if s.IsEnrichment() {
				values = append(values, canonicalKeyString(plain[s.On]))
				continue
			}
			values = append(values, plain[s.Column])
		}
		args := p.dialect.BindArgs(values)
		if _, err := tx.StmtContext(ctx, r.Stmt).ExecContext(ctx, args...); err != nil {
			// Same NUL hint as the plain sink's row apply: name the offending
			// payload field(s) — names only, never values.
			err = withNulFieldHint(err, p.dialect, data)
			return true, "", execFailure(fmt.Sprintf("[projection.apply] exec [%s]", r.SQL), err, p.dialect.IsPermanent(err))
		}
	}
	// The canonical single-column key rendering, for forEach's
	// reconciliation set (composite-keyed folds return "" — no forEach).
	rowKey := ""
	if len(keys) == 1 {
		rowKey = keyString(keys[0])
	}
	return true, rowKey, nil
}

// applyDelete honors a delete entity per its source's onDelete: ignore drops it;
// clear NULLs the source's owned columns for the keyed row (the folded row
// survives); delete-row removes the row entirely. The bound arguments come from
// the entity Key alone — decoded to one value per primaryKey column for a
// composite key — so the sentinel payload is never unmarshaled.
//
// Key-shape guards (both directions, both loud): a tombstone whose key doesn't
// decode at this projection's arity — a composite encoding against a single-key
// projection, or a bare key against a composite one — dead-letters instead of
// executing a WHERE that matches nothing. The silent no-op is the worst outcome
// here: deleted source rows would persist in the sink forever and an RTBF
// erasure would quietly fail downstream. NO message below carries e.Key — it
// lands in a permanent, Raft-replicated dead-letter record, and for an RTBF
// delete the key IS the subject being erased.
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
		return cluster.Permanent(fmt.Errorf(
			"[projection.apply] cannot honor delete: no statement prepared (topic %q)",
			src.topic))
	}

	n := len(p.config.PrimaryKey)
	if n == 1 && cluster.IsCompositeEncoded(string(e.Key)) {
		return cluster.Permanent(fmt.Errorf(
			"[projection.apply] delete tombstone carries a composite entity key; this projection keys by the single column %q and cannot address the row (topic %q) — the producer keys this topic by several columns; match the producer's key columns in primaryKey, or set onDelete = \"ignore\" if this fold deliberately collapses the composite identity",
			p.config.PrimaryKey[0], src.topic))
	}
	keyVals, derr := cluster.DecodeCompositeKey(string(e.Key), n)
	if derr != nil {
		// DecodeCompositeKey reports parse reason and arity numbers only,
		// never the key value.
		return cluster.Permanent(fmt.Errorf(
			"[projection.apply] delete tombstone key does not decode for this projection's %d-column primaryKey (topic %q) — producer and projection key shapes disagree: %w",
			n, src.topic, derr))
	}
	args := make([]any, len(keyVals))
	for i, v := range keyVals {
		args[i] = v
	}
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...); err != nil {
		return execFailure(fmt.Sprintf("[projection.apply] exec [%s]", sqlStr), err, p.dialect.IsPermanent(err))
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

	// Aggregates are single-key by validation (composite primaryKey +
	// aggregate sources is rejected at parse) — one parent-key path. The
	// len guard keeps a directly-constructed config that skipped both
	// validation and the applyDefaults fill on the old error path (a
	// Permanent misconfiguration) instead of an index panic.
	if len(src.keyPaths) == 0 {
		return cluster.Permanent(fmt.Errorf("[projection.aggregate] no keyPath configured (topic %q)", src.topic))
	}
	parentKey, err := jsonpath.Get(src.keyPaths[0], jsonData)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[projection.aggregate] keyPath [%s]: %w", src.keyPaths[0], err))
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
		return cluster.Permanent(fmt.Errorf("[projection.aggregate] elementKey [%s]: %w", ag.elementKey, err))
	}
	element := make(map[string]any, len(ag.fields))
	for _, f := range ag.fields {
		v, err := jsonpath.Get(f.From, jsonData)
		if err != nil {
			return cluster.Permanent(fmt.Errorf("[projection.aggregate] element field %q from [%s]: %w", f.Field, f.From, err))
		}
		element[f.Field] = v
	}
	elementJSON, err := json.Marshal(element)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[projection.aggregate] marshal element: %w", err))
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
	if err := p.aggExec(ctx, tx, ag.materialize, ag.materializeSQL, repeatArg(pk, ag.parentBinds)...); err != nil {
		return err
	}
	// Re-parented: rebuild the old parent so its array drops the moved element.
	// Mirrors removeFromAggregate — an UPDATE that no-ops if the old parent has no
	// row, never a ghost. Skipped when the child is new or its parent is unchanged.
	if hadOldParent && oldParent != keyString(parentKey) {
		return p.aggExec(ctx, tx, ag.rebuild, ag.rebuildSQL, repeatArg(oldParent, ag.parentBinds)...)
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
	return p.aggExec(ctx, tx, ag.rebuild, ag.rebuildSQL, repeatArg(parentKey, ag.parentBinds)...)
}

// initForEach creates one forEach source's reconciliation sidecar and
// prepares its three statements. The sidecar reuses the aggregate sidecar
// shape (child_key PK, parent_key indexed; element columns unused) so it
// adds zero DDL surface; its name derives from the source topic, stable
// across config edits that reorder sources.
func (p *Projection) initForEach(si int, src ProjectionSource) (*forEachRuntime, error) {
	where := fmt.Sprintf("source %d (topic %q) forEach", si+1, src.Topic)
	sidecar := ForEachSidecarName(p.config.Table, src.Topic)

	ddl := p.dialect.CreateAggregateSidecarDDL(AggregateSpec{
		Table: p.config.Table, PrimaryKey: p.config.PrimaryKey[0], Sidecar: sidecar,
	})
	if _, err := p.db.ExecContext(p.initCtx, ddl); err != nil {
		return nil, fmt.Errorf("%s sidecar ddl [%s]: %w", where, ddl, err)
	}

	rt := &forEachRuntime{sidecar: sidecar}
	success := false
	defer func() {
		if !success {
			rt.closeStmts()
		}
	}()
	scConfig := sidecarConfig(sidecar)
	prepare := func(label, sqlString string) (*gosql.Stmt, error) {
		stmt, err := p.db.PrepareContext(p.initCtx, sqlString)
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
	rt.childrenSQL = p.dialect.CreateForEachChildrenSQL(sidecar)
	if rt.children, err = prepare("children", rt.childrenSQL); err != nil {
		return nil, err
	}
	success = true
	return rt, nil
}

// applyForEach folds one event through a forEach source: each element of
// the (deliberately multi-valued) forEach path folds as its own row via the
// source's rules, and the reconciliation sidecar records which rows this
// parent currently fans — rows for vanished elements are deleted, so a
// re-emitted parent converges absolutely (replay-safe, like every other
// projection write). An event whose forEach path is absent or not an array
// fans zero elements, which reconciles all of the parent's prior rows away.
func (p *Projection) applyForEach(ctx context.Context, tx *gosql.Tx, src *projectionSource, jsonData any, e *cluster.Entity) error {
	parentKey := string(e.Key)

	prior, err := p.forEachChildren(ctx, tx, src.fe, parentKey)
	if err != nil {
		return err
	}

	var elems []any
	if v, err := jsonpath.Get(src.forEach, jsonData); err == nil {
		if list, ok := v.([]any); ok {
			elems = list
		}
	}

	current := make(map[string]bool, len(elems))
	for i, el := range elems {
		matched, key, err := p.applyRowFold(ctx, tx, src, el, jsonData, nil)
		if err != nil {
			return fmt.Errorf("[projection.forEach] element %d: %w", i+1, err)
		}
		if !matched || key == "" || current[key] {
			continue
		}
		current[key] = true
		// The sidecar row: fanned row key, parent entity key; the aggregate
		// shape's element columns ride along unused ("" sort key, {} object).
		scValues := []any{key, parentKey, "", "{}"}
		if err := p.aggExec(ctx, tx, src.fe.upsertSidecar, src.fe.upsertSidecarSQL, p.dialect.BindArgs(scValues)...); err != nil {
			return err
		}
	}

	for _, child := range prior {
		if current[child] {
			continue
		}
		if err := p.forEachDeleteRow(ctx, tx, child); err != nil {
			return err
		}
		if err := p.aggExec(ctx, tx, src.fe.deleteSidecar, src.fe.deleteSidecarSQL, child); err != nil {
			return err
		}
	}
	return nil
}

// cascadeForEachDelete honors a parent tombstone for a forEach source
// (onDelete = "delete-rows"): every row the parent fanned out is deleted,
// found via the reconciliation sidecar (the tombstone carries only the
// parent entity key).
func (p *Projection) cascadeForEachDelete(ctx context.Context, tx *gosql.Tx, src *projectionSource, e *cluster.Entity) error {
	children, err := p.forEachChildren(ctx, tx, src.fe, string(e.Key))
	if err != nil {
		return err
	}
	for _, child := range children {
		if err := p.forEachDeleteRow(ctx, tx, child); err != nil {
			return err
		}
		if err := p.aggExec(ctx, tx, src.fe.deleteSidecar, src.fe.deleteSidecarSQL, child); err != nil {
			return err
		}
	}
	return nil
}

// forEachChildren lists the fanned row keys currently recorded for one
// parent in the reconciliation sidecar.
func (p *Projection) forEachChildren(ctx context.Context, tx *gosql.Tx, fe *forEachRuntime, parentKey string) ([]string, error) {
	rows, err := tx.StmtContext(ctx, fe.children).QueryContext(ctx, parentKey)
	if err != nil {
		return nil, execFailure(fmt.Sprintf("[projection.forEach] exec [%s]", fe.childrenSQL), err, p.dialect.IsPermanent(err))
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, execFailure(fmt.Sprintf("[projection.forEach] scan [%s]", fe.childrenSQL), err, p.dialect.IsPermanent(err))
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// forEachDeleteRow removes one fanned row by its key through the
// projection's shared delete statement.
func (p *Projection) forEachDeleteRow(ctx context.Context, tx *gosql.Tx, childKey string) error {
	if _, err := tx.StmtContext(ctx, p.delete.Stmt).ExecContext(ctx, childKey); err != nil {
		return execFailure(fmt.Sprintf("[projection.forEach] exec [%s]", p.delete.SQL), err, p.dialect.IsPermanent(err))
	}
	return nil
}

// foldStages folds one Actual's entities through the stage graph in one
// store transaction, routing each stage's deltas to the table sources
// consuming it (applied on the surrounding SQL transaction). The frontier
// advances atomically with the fold.
func (p *Projection) foldStages(ctx context.Context, tx *gosql.Tx, a *cluster.Actual) error {
	return p.stageStore.Update(func(stx *stagestore.Tx) error {
		p.stages.onDelta = func(stage string, outKey []byte, obj any, live bool) error {
			for _, src := range p.stageSinks[stage] {
				if err := p.applyStageDelta(ctx, tx, src, outKey, obj, live); err != nil {
					return err
				}
			}
			return nil
		}
		defer func() { p.stages.onDelta = nil }()

		dirty := dirtySet{}
		for _, e := range a.Entities {
			if !p.stages.ConsumesTopic(e.Type.ID) {
				continue
			}
			switch e.Variant() {
			case cluster.EntityVariantDelete:
				if err := p.stages.FoldTopicDelete(stx, e.Type.ID, e.Key, dirty); err != nil {
					return err
				}
			case cluster.EntityVariantRow:
				obj, err := decodeStageObject(e.Data)
				if err != nil {
					return cluster.Permanent(fmt.Errorf("[projection.stage] unmarshal entity data: %w", err))
				}
				if err := p.stages.FoldTopicUpsert(stx, e.Type.ID, e.Key, obj, e.Generation, dirty); err != nil {
					return err
				}
			case cluster.EntityVariantRefresh:
				// The epoch sweep: inputs and dimension rows this re-snapshot
				// did not re-assert retract, refolding their keys as explicit
				// deltas — downstream (including stage-fed table sources)
				// never needs sweep semantics of its own.
				if err := p.stages.SweepEpochs(stx, e.Type.ID, e.Generation, dirty); err != nil {
					return err
				}
			default:
				// Future variants fold nothing here; the source-side apply
				// dead-letters them loudly.
			}
		}
		if err := p.stages.Drain(stx, dirty); err != nil {
			return err
		}
		return stx.SetFrontier(a.Index)
	})
}

// applyStageDelta lands one stage delta on a table source: an upsert folds
// through the source's rules exactly as a topic entity would (the stage
// object is the payload), a retraction removes or ignores per onDelete.
func (p *Projection) applyStageDelta(ctx context.Context, tx *gosql.Tx, src *projectionSource, outKey []byte, obj any, live bool) error {
	if !live {
		if src.onDelete == onDeleteIgnore {
			return nil
		}
		if _, err := tx.StmtContext(ctx, p.delete.Stmt).ExecContext(ctx, string(outKey)); err != nil {
			return execFailure(fmt.Sprintf("[projection.stage] exec [%s]", p.delete.SQL), err, p.dialect.IsPermanent(err))
		}
		return nil
	}
	if !matchWhen(src.when, obj) {
		return nil
	}
	key := coerceForColumn(string(outKey), p.columnType(p.config.PrimaryKey[0]))
	_, _, err := p.applyRowFold(ctx, tx, src, obj, nil, []any{key})
	return err
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
			fmt.Sprintf("[projection.aggregate] exec [%s]", ag.lookupSQL), err, p.dialect.IsPermanent(err))
	}
}

// repeatArg binds one value to n placeholders — the aggregate materialize
// and rebuild statements repeat the parent key once per value column.
func repeatArg(v any, n int) []any {
	out := make([]any, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// aggExec runs one prepared aggregate statement, classifying a permanent error
// the same way the rule path does.
func (p *Projection) aggExec(ctx context.Context, tx *gosql.Tx, stmt *gosql.Stmt, sqlStr string, args ...any) error {
	if _, err := tx.StmtContext(ctx, stmt).ExecContext(ctx, args...); err != nil {
		return execFailure(fmt.Sprintf("[projection.aggregate] exec [%s]", sqlStr), err, p.dialect.IsPermanent(err))
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
			return cluster.Permanent(fmt.Errorf("[projection.lookup] field %q from [%s]: %w", f.Field, f.From, err))
		}
		fields[f.Field] = v
	}
	fieldsJSON, err := json.Marshal(fields)
	if err != nil {
		return cluster.Permanent(fmt.Errorf("[projection.lookup] marshal fields: %w", err))
	}

	// The dimension key is the entity's own Key — the value aggregate elements
	// reference in `on`, and the only key a (payload-less) delete can use.
	key := string(e.Key)
	dimValues := []any{key, string(fieldsJSON)}
	if err := p.aggExec(ctx, tx, lk.upsertDim, lk.upsertDimSQL, p.dialect.BindArgs(dimValues)...); err != nil {
		return err
	}
	if err := p.spineFanOut(ctx, tx, lk, key, fields); err != nil {
		return err
	}
	return p.fanOut(ctx, tx, lk, key)
}

// spineFanOut updates every enriched spine column fed from this dimension:
// value from the dimension event's extracted fields (nil fields = a
// dimension delete → NULL, matching what the join would now return), key
// coerced to each on column's declared type — the canonical join space. A
// dimension key that does not parse as the on column's type provably matches
// no row (the column cannot hold it), so it skips rather than errors.
func (p *Projection) spineFanOut(ctx context.Context, tx *gosql.Tx, lk *lookupRuntime, dimKey string, fields map[string]any) error {
	for _, dep := range lk.spineDeps {
		onKey, ok := coerceKeyForColumn(dimKey, dep.onColType)
		if !ok {
			zap.L().Debug("[projection.lookup] dimension key does not fit the on column's type; no row can reference it — skipping spine fan-out",
				zap.String("column", dep.column))
			continue
		}
		var val any
		if fields != nil {
			val = coerceForColumn(fields[dep.selectFld], dep.colType)
		}
		if _, err := tx.StmtContext(ctx, dep.update).ExecContext(ctx, val, onKey); err != nil {
			return execFailure(fmt.Sprintf("[projection.lookup] exec [%s]", dep.updateSQL), err, p.dialect.IsPermanent(err))
		}
	}
	return nil
}

// canonicalKeyString renders a coerced on-column value in the canonical join
// space's string form for the dimension-side comparison (dimension keys are
// text). Deterministic because the input is the TYPED value: int64 renders
// one way, text is itself. nil binds NULL — the subquery matches nothing and
// the enriched column lands NULL, the join-equivalence answer for a NULL FK.
func canonicalKeyString(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case int64:
		return strconv.FormatInt(x, 10)
	case string:
		return x
	default:
		return fmt.Sprintf("%v", x)
	}
}

// coerceKeyForColumn coerces a dimension entity key (text) into an on
// column's declared type for the fan-out bind. ok=false means the key cannot
// exist in that column (an int-family column and a non-numeric key).
func coerceKeyForColumn(key, sqlType string) (any, bool) {
	if columnIsNumericOrBool(sqlType) {
		n, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return nil, false
		}
		return n, true
	}
	return key, true
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
	if err := p.spineFanOut(ctx, tx, lk, key, nil); err != nil {
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
				fmt.Sprintf("[projection.lookup] exec [%s]", dep.affectedSQL), err, p.dialect.IsPermanent(err))
		}
		var parents []string
		for rows.Next() {
			var pk string
			if err := rows.Scan(&pk); err != nil {
				_ = rows.Close()
				return execFailure("[projection.lookup] scan affected parent", err, p.dialect.IsPermanent(err))
			}
			parents = append(parents, pk)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return execFailure("[projection.lookup] affected parents", err, p.dialect.IsPermanent(err))
		}
		_ = rows.Close()

		for _, pk := range parents {
			if err := p.aggExec(ctx, tx, dep.rebuild, dep.rebuildSQL, repeatArg(pk, dep.parentBinds)...); err != nil {
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
	if p.stageStore != nil {
		if cerr := p.stageStore.Close(); err == nil {
			err = cerr
		}
	}
	for _, list := range p.sources {
		for _, src := range list {
			for _, r := range src.rules {
				closeStmt(r.Stmt)
			}
			closeStmt(src.clear)
			if src.fe != nil {
				closeStmt(src.fe.upsertSidecar)
				closeStmt(src.fe.deleteSidecar)
				closeStmt(src.fe.children)
			}
			if src.agg != nil {
				closeStmt(src.agg.upsertSidecar)
				closeStmt(src.agg.deleteSidecar)
				closeStmt(src.agg.lookup)
				closeStmt(src.agg.materialize)
				closeStmt(src.agg.rebuild)
			}
			if src.lkp != nil {
				for _, dep := range src.lkp.spineDeps {
					closeStmt(dep.update)
				}
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

// closeStmts closes the aggregate runtime's prepared statements (nil-tolerant).
// initAggregate calls it on its own error path to avoid leaking the statements it
// prepared before the failure — a failed runtime is never stored on its source,
// so Projection.Close cannot reach it.
func (rt *aggregateRuntime) closeStmts() {
	for _, s := range []*gosql.Stmt{rt.upsertSidecar, rt.deleteSidecar, rt.lookup, rt.materialize, rt.rebuild} {
		if s != nil {
			_ = s.Close()
		}
	}
}

// closeStmts closes the lookup runtime's prepared statements (nil-tolerant). See
// aggregateRuntime.closeStmts — initLookup uses it the same way on its error path.
func (rt *lookupRuntime) closeStmts() {
	for _, s := range []*gosql.Stmt{rt.upsertDim, rt.deleteDim} {
		if s != nil {
			_ = s.Close()
		}
	}
}
