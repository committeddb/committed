// Package iceberg implements the Iceberg-on-S3 syncable: committed Actuals
// land in an Apache Iceberg table (S3 Tables, Athena, Redshift Spectrum —
// anything that reads Iceberg through a REST catalog) as a CURRENT-STATE
// table, not a fact log.
//
// Merge semantics are COPY-ON-WRITE, decided 2026-08-15 (equality deletes are
// unwritable in iceberg-go v0.6 and the Iceberg v4 spec discussion is moving
// away from them): each flush commits one atomic snapshot pair that first
// deletes every superseded row (a keyed upsert's prior version, a source
// DELETE's row, and — on a refresh boundary — every row whose generation
// predates the sweep epoch), then appends the batch's current rows. The
// library rewrites only the data files the delete filter touches (file stats
// prune the rest), so consumers always read plain data files: no
// merge-on-read cost, which is the right posture for a warehouse landing
// zone.
//
// The v1 table shape is a fixed envelope, one row per live entity:
//
//	key             string (required) — the entity key, the merge identity
//	payload         string            — the entity's JSON document, verbatim
//	committed_index long              — the raft index that wrote this version
//	generation      long              — the ingest refresh epoch (0 = unstamped)
//
// committed_index and generation are provenance/debugging columns (and the
// sweep predicate); projection to typed columns is a downstream concern (a
// CTAS/dbt model, or a loopback canonicalizing upstream).
//
// Exactly-once: the worker checkpoint advances ONLY on a successful flush
// (ShouldSnapshot is returned true at flush boundaries and false while
// buffering), and every commit stamps the flushed-through raft index into the
// snapshot summary (propertyCheckpoint). On restart the sink re-buffers from
// the last checkpoint and skips any flush whose range the table already
// carries; a flush that partially overlaps a committed range re-merges it,
// which is idempotent by key. Rows are buffered in memory between flushes —
// a restart loses only the buffer, never committed data, and the redelivery
// contract rebuilds it.
package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"

	// Registers the s3:// (and gcs/azblob) FileIO scheme — table data and
	// metadata live in object storage and load through this side effect.
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"

	"github.com/committeddb/committed/internal/cluster"
)

// propertyCheckpoint is the snapshot-summary property carrying the raft index
// a commit flushed through — the idempotent-re-commit marker. Namespaced so
// it can never collide with engine or user properties.
const propertyCheckpoint = "committed.checkpoint-index"

// RenderingVersion is the version of every rendering this sink writes into
// a table — the envelope schema, the row encoding, the sweep semantics. Its
// stamp is the TABLE property propertyRenderingVersion (the checkpoint is a
// snapshot-summary property; the rendering is a property of the table), so
// it moves, drops, and restores with the table. The worker reads it before
// serving and parks on a mismatch (db/rendering_stamp.go); this sink cannot
// converge in place, so the remedy is a fresh table (delete drops one
// committed created; the operator recreates one it did not). sql.SinkRenderingVersion
// is the SQL family's twin, versioned separately: the two render nothing in
// common.
const RenderingVersion uint64 = 1

const propertyRenderingVersion = "committed.rendering-version"

// propertyOwned marks a table or namespace committed CREATED (set with the
// create, never afterwards) — the ownership protocol: delete drops what
// committed created and leaves what it attached to; keepData hands a
// created table over by setting this to "false". A table without the
// property is one committed did not create.
const propertyOwned = "committed.owned"

// teardownTimeout bounds a Teardown's catalog calls (the destination that
// wedged the worker is the one being torn down). The engine bounds the
// whole call too, but the ctx is what cancels the HTTP round trips.
const teardownTimeout = 10 * time.Second

// RenderingVersion implements cluster.RenderingStamped.
func (s *Syncable) RenderingVersion() uint64 { return RenderingVersion }

// RenderingStamp implements cluster.RenderingStamped: the table property,
// read from the current metadata.
func (s *Syncable) RenderingStamp(ctx context.Context) (uint64, bool, error) {
	if err := s.tbl.Refresh(ctx); err != nil {
		return 0, false, fmt.Errorf("[iceberg] refresh table: %w", err)
	}
	v, ok := s.tbl.Properties()[propertyRenderingVersion]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("[iceberg] rendering stamp %q is not a version: %w", v, err)
	}
	return n, true, nil
}

// StampRendering implements cluster.RenderingStamped: a metadata-only
// commit setting the table property.
func (s *Syncable) StampRendering(ctx context.Context) error {
	tx := s.tbl.NewTransaction()
	if err := tx.SetProperties(iceberggo.Properties{propertyRenderingVersion: strconv.FormatUint(RenderingVersion, 10)}); err != nil {
		return fmt.Errorf("[iceberg] set rendering stamp: %w", err)
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("[iceberg] commit rendering stamp: %w", err)
	}
	s.tbl = newTbl
	return nil
}

var _ cluster.RenderingStamped = (*Syncable)(nil)

// Teardown implements cluster.Teardownable. Drop mode purges the table
// (catalog entry and data files) if committed created it, then the
// namespace if committed created that and nothing else lives in it; a
// table committed attached to is left alone. Keep mode hands a created
// table over: its property flips to not-owned and nothing is removed.
func (s *Syncable) Teardown(keep bool) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), teardownTimeout)
	defer cancel()
	owned, present, err := s.owned(ctx)
	if err != nil {
		return false, err
	}
	if !present {
		return false, nil // already gone: a second teardown is a no-op
	}
	if keep {
		if !owned {
			return false, nil
		}
		return false, s.disown(ctx)
	}
	if !owned {
		return false, nil
	}
	if err := s.catalog.PurgeTable(ctx, s.identifier()); err != nil {
		return false, fmt.Errorf("[iceberg] drop table: %w", err)
	}
	return true, s.dropNamespaceIfOwnedAndEmpty(ctx)
}

// owned reads the table's ownership from the current metadata; present is
// false when the table no longer exists.
func (s *Syncable) owned(ctx context.Context) (owned, present bool, err error) {
	if err := s.tbl.Refresh(ctx); err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("[iceberg] refresh table: %w", err)
	}
	return s.tbl.Properties()[propertyOwned] == "true", true, nil
}

// disown is the keepData handover: a metadata-only commit marking the
// table as not committed's, so no later delete drops it.
func (s *Syncable) disown(ctx context.Context) error {
	tx := s.tbl.NewTransaction()
	if err := tx.SetProperties(iceberggo.Properties{propertyOwned: "false"}); err != nil {
		return fmt.Errorf("[iceberg] disown table: %w", err)
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("[iceberg] commit disown: %w", err)
	}
	s.tbl = newTbl
	return nil
}

// dropNamespaceIfOwnedAndEmpty drops the namespace committed created once
// its last table is gone. The catalog does not distinguish "not empty"
// from other drop failures, so emptiness (tables and child namespaces) is
// checked first; anything the operator put there keeps the namespace.
func (s *Syncable) dropNamespaceIfOwnedAndEmpty(ctx context.Context) error {
	ns := table.Identifier{s.config.Namespace}
	props, err := s.catalog.LoadNamespaceProperties(ctx, ns)
	if err != nil {
		return fmt.Errorf("[iceberg] load namespace properties: %w", err)
	}
	if props[propertyOwned] != "true" {
		return nil
	}
	// One unpaginated page: the question is "anything here?", and the
	// page-size parameter is the one thing REST catalog servers disagree on
	// (tabulario's rejects it outright).
	ctx = s.catalog.SetPageSize(ctx, 0)
	for _, err := range s.catalog.ListTables(ctx, ns) {
		if err != nil {
			return fmt.Errorf("[iceberg] list tables: %w", err)
		}
		return nil // something else lives here
	}
	children, err := s.catalog.ListNamespaces(ctx, ns)
	if err != nil {
		return fmt.Errorf("[iceberg] list namespaces: %w", err)
	}
	if len(children) > 0 {
		return nil
	}
	if err := s.catalog.DropNamespace(ctx, ns); err != nil {
		return fmt.Errorf("[iceberg] drop namespace: %w", err)
	}
	return nil
}

// OwnsDestination implements cluster.Teardownable: the table property
// says committed created it, or there is no table yet.
func (s *Syncable) OwnsDestination(ctx context.Context) (bool, error) {
	owned, present, err := s.owned(ctx)
	if err != nil {
		return false, err
	}
	return !present || owned, nil
}

var _ cluster.Teardownable = (*Syncable)(nil)

const (
	defaultFlushRows     = 10000
	defaultFlushInterval = 60 * time.Second
)

// Config is the parsed [iceberg] section.
type Config struct {
	// Topic is the topic (type ID) this sink consumes.
	Topic string
	// CatalogURI is the Iceberg REST catalog endpoint. Credentials never ride
	// in the URI (rejected at parse) — authentication uses the standard AWS
	// credential chain / catalog token via environment.
	CatalogURI string
	// Namespace and Table identify the destination table in the catalog.
	Namespace string
	Table     string
	// Warehouse is the catalog warehouse location passed at connect (some
	// REST catalogs require it, e.g. "s3://bucket/warehouse").
	Warehouse string
	// FlushRows / FlushInterval bound the buffer: a flush commits when the
	// buffer holds this many entities, when this much time has passed since
	// the first buffered entity (checked on arrival — an idle topic flushes
	// on its next delivery), or when a refresh-boundary marker arrives.
	FlushRows     int
	FlushInterval time.Duration
	// Props are additional FileIO/catalog properties (s3.endpoint,
	// s3.region, s3.force-virtual-addressing …) — the minio/e2e and
	// private-endpoint knob.
	Props map[string]string
}

// bufferedRow is one key's pending state: the latest upsert seen for the key,
// or a tombstone (delete=true). Later entries for the same key overwrite
// earlier ones — log order collapses inside the buffer exactly as it would
// merge in the table.
type bufferedRow struct {
	payload    string
	index      uint64
	generation uint64
	delete     bool
}

// Syncable is the Iceberg sink.
type Syncable struct {
	config  *Config
	catalog *rest.Catalog
	tbl     *table.Table

	buffer        map[string]*bufferedRow
	sweepEpoch    uint64 // pending refresh-boundary sweep (max epoch seen)
	firstBuffered time.Time
	pendingIndex  uint64 // highest actual index in the buffer
}

// New connects the catalog and ensures the destination table exists with the
// envelope schema. Called by the parser at build time (off the raft apply
// path — builds run on the listener).
func New(ctx context.Context, config *Config) (*Syncable, error) {
	props := iceberggo.Properties{}
	for k, v := range config.Props {
		props[k] = v
	}
	if config.Warehouse != "" {
		props["warehouse"] = config.Warehouse
	}
	cat, err := rest.NewCatalog(ctx, "committed", config.CatalogURI, rest.WithAdditionalProps(props))
	if err != nil {
		return nil, fmt.Errorf("[iceberg] connect catalog: %w", err)
	}

	s := &Syncable{
		config:  config,
		catalog: cat,
		buffer:  map[string]*bufferedRow{},
	}
	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Syncable) identifier() table.Identifier {
	return table.Identifier{s.config.Namespace, s.config.Table}
}

// envelopeSchema is the fixed v1 table shape. Field IDs are part of the
// Iceberg schema contract and must never be renumbered.
func envelopeSchema() *iceberggo.Schema {
	return iceberggo.NewSchema(0,
		iceberggo.NestedField{ID: 1, Name: "key", Type: iceberggo.PrimitiveTypes.String, Required: true},
		iceberggo.NestedField{ID: 2, Name: "payload", Type: iceberggo.PrimitiveTypes.String, Required: false},
		iceberggo.NestedField{ID: 3, Name: "committed_index", Type: iceberggo.PrimitiveTypes.Int64, Required: true},
		iceberggo.NestedField{ID: 4, Name: "generation", Type: iceberggo.PrimitiveTypes.Int64, Required: true},
	)
}

func (s *Syncable) ensureTable(ctx context.Context) error {
	// Create-and-tolerate-exists rather than check-then-create: existence
	// probes (HEAD) are unevenly supported across REST catalog servers, and
	// create races resolve the same way regardless.
	// What committed creates it marks as its own (propertyOwned) in the same
	// call, so a namespace or table that already existed is never claimed.
	ns := table.Identifier{s.config.Namespace}
	if err := s.catalog.CreateNamespace(ctx, ns, iceberggo.Properties{propertyOwned: "true"}); err != nil &&
		!errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return fmt.Errorf("[iceberg] create namespace: %w", err)
	}

	tbl, err := s.catalog.LoadTable(ctx, s.identifier())
	if err == nil {
		s.tbl = tbl
		return nil
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		return fmt.Errorf("[iceberg] load table: %w", err)
	}
	tbl, err = s.catalog.CreateTable(ctx, s.identifier(), envelopeSchema(), catalog.WithProperties(iceberggo.Properties{
		propertyOwned:            "true",
		propertyRenderingVersion: strconv.FormatUint(RenderingVersion, 10), // created by this binary: rendered by it
	}))
	if err != nil {
		if errors.Is(err, catalog.ErrTableAlreadyExists) {
			// Lost a create race: load what the winner made.
			if tbl2, err2 := s.catalog.LoadTable(ctx, s.identifier()); err2 == nil {
				s.tbl = tbl2
				return nil
			}
		}
		return fmt.Errorf("[iceberg] create table: %w", err)
	}
	s.tbl = tbl
	return nil
}

func (s *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	matched := false
	force := false
	for _, e := range a.Entities {
		if e.Type == nil || e.Type.ID != s.config.Topic {
			continue // an entity from another topic in a mixed proposal — not ours
		}
		matched = true
		switch e.Variant() {
		case cluster.EntityVariantDelete:
			s.bufferPut(string(e.Key), &bufferedRow{delete: true, index: a.Index, generation: e.Generation})
		case cluster.EntityVariantRefresh:
			// The pass just closed: rows the re-enumeration could not re-emit
			// keep an older generation and must be swept. Force the flush so
			// the sweep commits with (never after) this marker's checkpoint.
			if e.Generation > s.sweepEpoch {
				s.sweepEpoch = e.Generation
			}
			force = true
		case cluster.EntityVariantRow:
			s.bufferPut(string(e.Key), &bufferedRow{payload: string(e.Data), index: a.Index, generation: e.Generation})
		default:
			return false, cluster.Permanent(fmt.Errorf(
				"[iceberg] entity variant %q is not supported by this binary; upgrade the node before syncing this topic", e.Variant()))
		}
	}
	if !matched {
		return false, nil
	}
	s.pendingIndex = a.Index

	if !force && !s.flushDue() {
		return false, nil
	}
	// Flush errors are TRANSIENT by classification: catalog/S3/commit
	// failures are access- or service-shaped, never entry-specific — the
	// worker wedges loudly and retries this Actual; the buffer is keyed, so
	// the retry's re-buffering is a no-op and the flush re-attempts.
	if err := s.flush(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Syncable) bufferPut(key string, row *bufferedRow) {
	if len(s.buffer) == 0 {
		s.firstBuffered = time.Now()
	}
	s.buffer[key] = row
}

func (s *Syncable) flushDue() bool {
	if len(s.buffer) == 0 {
		return false
	}
	return len(s.buffer) >= s.config.FlushRows ||
		time.Since(s.firstBuffered) >= s.config.FlushInterval
}

// flush commits the buffer as one atomic snapshot chain: delete every
// superseded row (buffered keys + the sweep predicate), append the buffer's
// live rows, stamp the checkpoint property. Idempotent against replays via
// the snapshot property — see the package comment for the interleavings.
func (s *Syncable) flush(ctx context.Context) error {
	if len(s.buffer) == 0 && s.sweepEpoch == 0 {
		return nil
	}

	// Refresh table state (another leader stint may have committed) and check
	// the idempotence marker: a replayed flush whose range the table already
	// carries clears the buffer without a new commit.
	if err := s.tbl.Refresh(ctx); err != nil {
		return fmt.Errorf("[iceberg] refresh table: %w", err)
	}
	if snap := s.tbl.CurrentSnapshot(); snap != nil && snap.Summary != nil {
		if v, ok := snap.Summary.Properties[propertyCheckpoint]; ok {
			if committed, err := strconv.ParseUint(v, 10, 64); err == nil && committed >= s.pendingIndex {
				s.clearBuffer()
				return nil
			}
		}
	}

	props := iceberggo.Properties{propertyCheckpoint: strconv.FormatUint(s.pendingIndex, 10)}

	keys := make([]string, 0, len(s.buffer))
	for k := range s.buffer {
		keys = append(keys, k)
	}
	var filter iceberggo.BooleanExpression = iceberggo.AlwaysFalse{}
	if len(keys) > 0 {
		filter = iceberggo.IsIn(iceberggo.Reference("key"), keys...)
	}
	if s.sweepEpoch > 0 {
		filter = iceberggo.NewOr(filter,
			iceberggo.LessThan(iceberggo.Reference("generation"), int64(s.sweepEpoch))) //nolint:gosec // G115: a refresh epoch is a small counter
	}

	tx := s.tbl.NewTransaction()
	if err := tx.Delete(ctx, filter, props); err != nil {
		return fmt.Errorf("[iceberg] delete superseded rows: %w", err)
	}
	rec, live, err := s.liveRecord()
	if err != nil {
		return err
	}
	if live > 0 {
		defer rec.Release()
		rdr, rerr := recordReader(rec)
		if rerr != nil {
			return rerr
		}
		defer rdr.Release()
		if err := tx.Append(ctx, rdr, props); err != nil {
			return fmt.Errorf("[iceberg] append rows: %w", err)
		}
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("[iceberg] commit: %w", err)
	}
	s.tbl = newTbl
	s.clearBuffer()
	return nil
}

func (s *Syncable) clearBuffer() {
	s.buffer = map[string]*bufferedRow{}
	s.sweepEpoch = 0
}

// Close drops the buffer (never committed data): the redelivery contract
// rebuilds it on the next start from the last checkpoint. Deliberately no
// flush here — a shutdown-path commit would race the worker's own
// checkpointing and gains nothing over the replay.
func (s *Syncable) Close() error {
	s.clearBuffer()
	return nil
}

// CanRematerialize is false in v1: the re-materialization verb's convergence
// contract needs a completion sweep for rows a changed projection no longer
// produces, and the sink has no per-replay epoch column to sweep on. The
// documented pattern for reshaping an Iceberg destination is blue-green: a
// second syncable into a new table, then a catalog RenameTable swap.
func (s *Syncable) CanRematerialize() bool { return false }

var _ cluster.Syncable = (*Syncable)(nil)
