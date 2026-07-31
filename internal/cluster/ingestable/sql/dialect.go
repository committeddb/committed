package sql

import (
	"context"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

//go:generate protoc --go_out=paths=source_relative:. ./dialectpb/dialect.proto

// Dialect is the per-source implementation behind a SQL Ingestable (Postgres
// logical replication, MySQL binlog). Ingest streams source changes as
// Proposals.
//
// Like any Ingestable, a Dialect MUST translate a source DELETE into a delete
// entity (cluster.NewDeleteEntity) keyed by the row's primary key — never an
// upsert of the deleted row's pre-image. Emitting deletes is mandatory for a
// well-behaved ingestable: only a delete entity makes the downstream Syncable
// remove the record. See the cluster.Ingestable contract.
type Dialect interface {
	// Ingest streams source changes as Proposals. epochFloor is the
	// delete-surviving per-topic refresh-epoch highwater (see TopicEpochReader):
	// on a full snapshot with a cleared position it is the generation the sink
	// still carries, so the snapshot must stamp ABOVE it. 0 means the topic has
	// never been refreshed (a genuine first snapshot starts at epoch 1).
	Ingest(ctx context.Context, config *Config, pos cluster.Position, epochFloor uint64, pr chan<- *cluster.Proposal, po chan<- cluster.Position) error
	// Preflight validates that the source can be ingested safely, before any
	// worker starts. Today it is the replica-identity / binlog-row-image guard:
	// it connects to the source and verifies every watched table's DELETE change
	// record carries the configured primaryKey, so deletes can't be silently
	// dropped (see CheckKeyCoverage). A non-nil error fails the ingestable's
	// build — surfaced as a degraded config (loud, queryable), and no worker is
	// started — rather than letting it run and quietly lose deletes. It manages
	// its own (short) connection timeout.
	Preflight(config *Config) error
	// Status decodes pos into a point-in-time IngestableStatus — phase, per-table
	// snapshot progress, and the CDC cursor — and, where the dialect supports it,
	// queries the source for replication lag. Read-only and side-effect free; it
	// manages its own short connection timeout for any source query and tolerates
	// the empty position (a worker that has not checkpointed yet). A source-query
	// failure leaves Lag nil rather than failing the whole status.
	Status(ctx context.Context, config *Config, pos cluster.Position) (cluster.IngestableStatus, error)
	// SourceColumns returns, per watched table (keyed by the table name as
	// configured), the column names in source order AND the subset that are
	// generated/computed columns. The columns expand a MapAllColumns config into
	// explicit mappings and validate every mapping resolves; the generated set is
	// excluded from MapAllColumns and rejected if explicitly mapped, because
	// committed cannot replicate a generated column (the source's change stream
	// omits its value, so it is present on the snapshot but null on every later
	// CDC row). Read-only introspection with its own short connection timeout.
	SourceColumns(config *Config) (columns, generated map[string][]string, err error)
}

// TopicSpec routes one topic (its Type) to be fed from one-or-more source Tables,
// with its own Mappings and PrimaryKey. An ingestable holds one or more specs
// (Config.Topics): the flat single-topic config is one spec; the [[sql.topics]]
// form is N. All specs share the one connection / slot / publication / binlog
// reader and the one resume/dedup cursor.
type TopicSpec struct {
	Type           *cluster.Type
	Tables         []string
	Mappings       []Mapping
	MapAllColumns  bool
	ExcludeColumns []string
	PrimaryKey     []string
}

type Config struct {
	ConnectionString string

	// Topics is the per-table routing model — one entry per topic. Every
	// entity-building site resolves its table's spec via SpecForTable and stamps
	// that spec's Type / Mappings / PrimaryKey. The flat single-topic config
	// parses to a one-element Topics.
	Topics []TopicSpec
	// tableToSpec routes a source table name (lowercased) to its owning spec;
	// built by index() from Topics. A table may belong to at most one spec.
	tableToSpec map[string]*TopicSpec

	// The fields below mirror Topics[0] for the single-topic (flat) form and are
	// what the flat parser fills; multi-topic call sites read the per-table spec
	// instead. Tables is the derived union of every spec's tables.
	Type     *cluster.Type
	Mappings []Mapping
	// MapAllColumns infers a jsonName=column mapping for every source column
	// (across all watched Tables) so a whole-table mirror needs no per-column
	// [[sql.mappings]]. The column set is FROZEN at config-build time: the
	// parser introspects the source once and expands MapAllColumns into explicit
	// Mappings, so a column added to the source later does not silently enter
	// payloads until the config is re-POSTed. Any explicit Mappings override the
	// inferred mapping for that column (a rename); ExcludeColumns drops columns
	// from the inferred set. Once the parser has expanded it, Mappings is the
	// full explicit set and the runtime never sees MapAllColumns.
	MapAllColumns bool
	// ExcludeColumns are source columns to omit from the MapAllColumns set
	// (secrets, large blobs). Only meaningful with MapAllColumns. Column names
	// are matched as the source reports them.
	ExcludeColumns []string
	// PrimaryKey is the source table's primary-key column(s). A single column
	// keys each entity by its bare value; multiple columns (a composite PK, e.g.
	// IMDb principals' (tconst, ordering)) key by all of them so rows sharing a
	// leading column don't collide. See CompositeKey.
	PrimaryKey []string
	Tables     []string
	Options    map[string]string
}

// EnsureTopics backfills Topics from the flat singular fields (Type, Tables,
// Mappings, PrimaryKey, MapAllColumns, ExcludeColumns) when a Config was built
// without them. The parser always fills Topics, so this is a no-op on a parsed
// config; it lets a hand-constructed Config (tests, or any non-parser path) route
// through the same per-spec machinery, with the flat singular fields as the one
// spec. Idempotent — once Topics is non-empty it never rewrites it.
func (c *Config) EnsureTopics() {
	if len(c.Topics) > 0 {
		return
	}
	c.Topics = []TopicSpec{{
		Type:           c.Type,
		Tables:         c.Tables,
		Mappings:       c.Mappings,
		MapAllColumns:  c.MapAllColumns,
		ExcludeColumns: c.ExcludeColumns,
		PrimaryKey:     c.PrimaryKey,
	}}
}

// SpecForTable returns the TopicSpec routing the given source table (matched
// case-insensitively), or nil if the table is not watched by this config. The
// table→spec map is built lazily on first use: the ingest worker resolves specs
// from a single streaming goroutine (snapshot then stream, sequentially), so no
// lock is needed, and keeping it nil until first use leaves a freshly parsed
// Config comparable by value. Cross-spec table-routing conflicts are rejected at
// parse time (see the parser), so first-appearance wins here is never ambiguous.
func (c *Config) SpecForTable(table string) *TopicSpec {
	c.EnsureTopics()
	if c.tableToSpec == nil {
		c.tableToSpec = make(map[string]*TopicSpec, len(c.Tables))
		for i := range c.Topics {
			spec := &c.Topics[i]
			for _, t := range spec.Tables {
				key := strings.ToLower(t)
				if _, ok := c.tableToSpec[key]; !ok {
					c.tableToSpec[key] = spec
				}
			}
		}
	}
	return c.tableToSpec[strings.ToLower(table)]
}

// The mapstructure tags drive viper.UnmarshalKey when parsing the
// [[sql.mappings]] array-of-tables. Required because the Go field names
// differ from the TOML keys (JsonName→jsonName, SQLColumn→column), and
// they keep parsing independent of viper's key-case handling, which
// changed between viper versions.
type Mapping struct {
	JsonName  string `mapstructure:"jsonName"`
	SQLColumn string `mapstructure:"column"`
}
