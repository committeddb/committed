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
	ns := table.Identifier{s.config.Namespace}
	if err := s.catalog.CreateNamespace(ctx, ns, nil); err != nil &&
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
	tbl, err = s.catalog.CreateTable(ctx, s.identifier(), envelopeSchema())
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
