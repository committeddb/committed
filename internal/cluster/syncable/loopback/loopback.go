// Package loopback implements the derived-topic syncable: a syncable whose
// destination is a topic in the SAME cluster. It consumes a source topic's
// Actuals, applies a per-entity jsonPath projection ONCE, and proposes the
// result to a target topic under the target's Type — so N downstream
// syncables consume the derived topic with dumb mappings instead of each
// repeating (and drifting) the same transform.
//
// The derived topic is a MATERIALIZATION, never a source of truth: it is
// rebuildable from the source forever (raw stays sacred), and its rows are
// exactly the cache of the source's current interpretation — the loopback
// sits behind the same interpretation (stamp ⊕ errata) and migration
// (always-current) wrappers as every other syncable, so errata and version
// migrations on the source are baked into what lands on the target.
//
// Faithful forwarding is the load-bearing property. For each source entity:
//
//   - a row forwards as an upsert with the SAME Key and the SAME Generation;
//   - a delete forwards as a delete tombstone with the same Key — so an RTBF
//     delete proposed to the source chases the derivation chain and the
//     scrubber erases the subject on the derived topic like any other;
//   - a refresh-boundary marker forwards at the same epoch — so an ingest
//     full-refresh of the source reconciles all the way through the chain
//     (a keyed sink downstream of the derived topic sweeps rows the
//     re-enumeration could not re-emit).
//
// Because generations and boundaries forward verbatim, the whole chain lives
// in ONE epoch space (the source's), and a log-order replay reproduces every
// downstream state exactly — which is what makes the re-materialization verb
// a plain replay here (see CanRematerialize).
//
// Transforms are stateless per-Actual and total: every source row produces
// exactly one output row with the same key. There is no filtering and no
// re-keying — that totality is what guarantees a replay converges a
// snapshot-kind target without a sweep, and key preservation is what keeps
// RTBF deletes translatable (a tombstone carries only the key).
package loopback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/PaesslerAG/jsonpath"

	"github.com/committeddb/committed/internal/cluster"
)

// Proposer is the loopback's write seam back into the cluster: db.DB
// implements it. Injected (dependency inversion) so this package never
// imports db.
type Proposer interface {
	Propose(ctx context.Context, p *cluster.Proposal) error
}

// Mapping projects one jsonPath of the source payload into one field of the
// derived payload. The mapstructure tags mirror the sql syncable's mappings
// (jsonPath is camelCase in TOML).
type Mapping struct {
	JsonPath string `mapstructure:"jsonPath"`
	Field    string `mapstructure:"field"`
}

// Config is the parsed [loopback] section.
type Config struct {
	// SourceTopic is the topic (type ID) this loopback consumes.
	SourceTopic string
	// TargetTopic is the topic (type ID) this loopback proposes into.
	TargetTopic string
	// Mappings project source jsonPaths into derived fields. Empty means
	// whole-payload passthrough (the derived topic re-types the source
	// bytes verbatim — the version-normalization shape when paired with
	// mode = "always-current").
	Mappings []Mapping
	// AcknowledgeAppendSemantics must be set to target a non-snapshot-kind
	// topic: a replay (crash recovery, ambiguous propose retry,
	// re-materialization) APPENDS to such a topic instead of converging,
	// so the duplicates are the operator's explicit trade.
	AcknowledgeAppendSemantics bool
}

// Syncable is the loopback sink. It resolves the target Type per Sync (so a
// target version bump stamps subsequent derived rows at the new version
// without a worker rebuild) and proposes one derived Proposal per matched
// source Actual.
type Syncable struct {
	proposer Proposer
	types    cluster.TypeResolver
	config   *Config
}

// New builds the sink. Callers go through SyncableParser.Parse, which
// validates the config first.
func New(proposer Proposer, types cluster.TypeResolver, config *Config) *Syncable {
	return &Syncable{proposer: proposer, types: types, config: config}
}

func (s *Syncable) Sync(ctx context.Context, a *cluster.Actual) (cluster.ShouldSnapshot, error) {
	matched := false
	for _, e := range a.Entities {
		if e.Type != nil && e.Type.ID == s.config.SourceTopic {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}

	// Resolve the target type per matched Actual. A missing target type is
	// config-shaped (it fails every entry identically), so per the
	// classification rule it stays TRANSIENT: the worker wedges loudly and
	// resumes when the operator declares the type, with nothing missing.
	t, err := s.types.ResolveType(cluster.LatestTypeRef(s.config.TargetTopic))
	if err != nil {
		return false, fmt.Errorf("[loopback] resolve target type %q: %w", s.config.TargetTopic, err)
	}
	if t == nil {
		return false, fmt.Errorf("[loopback] target type %q is not declared", s.config.TargetTopic)
	}

	out := make([]*cluster.Entity, 0, len(a.Entities))
	for _, e := range a.Entities {
		if e.Type == nil || e.Type.ID != s.config.SourceTopic {
			continue // an entity from another topic in a mixed proposal — not ours
		}
		switch e.Variant() {
		case cluster.EntityVariantDelete:
			de := cluster.NewDeleteEntity(t, e.Key)
			de.Generation = e.Generation
			out = append(out, de)
		case cluster.EntityVariantRefresh:
			out = append(out, cluster.NewRefreshBoundaryEntity(t, e.Generation))
		case cluster.EntityVariantRow:
			data, terr := Transform(e.Data, s.config.Mappings)
			if terr != nil {
				// Entry-specific: this payload will fail this projection
				// identically forever — dead-letter, don't wedge the topic.
				return false, cluster.Permanent(fmt.Errorf("[loopback] transform: %w", terr))
			}
			ne := cluster.NewUpsertEntity(t, e.Key, data)
			ne.Generation = e.Generation
			out = append(out, ne)
		default:
			return false, cluster.Permanent(fmt.Errorf(
				"[loopback] entity variant %q is not supported by this binary; upgrade the node before deriving this topic", e.Variant()))
		}
	}
	if len(out) == 0 {
		return false, nil
	}

	// Propose and wait for commit — the checkpoint must never advance past a
	// row that isn't durably in the target topic. A transient failure
	// (ErrProposalLost on a leader change) is retried by the worker; the
	// ambiguous case (committed but the response was lost) re-proposes, which
	// a snapshot-kind target converges by key and an acknowledged append-kind
	// target absorbs as its declared duplicate semantics.
	if err := s.proposer.Propose(ctx, &cluster.Proposal{Entities: out}); err != nil {
		return false, fmt.Errorf("[loopback] propose to %q: %w", s.config.TargetTopic, err)
	}
	return true, nil
}

func (s *Syncable) Close() error { return nil }

// CanRematerialize: a snapshot-kind target converges a plain replay by key —
// v1 transforms are total and key-preserving, so the replay re-emits every
// live key and forwarded boundaries re-reconcile source-vanished ones; no
// sweep is needed. Any other target kind would DOUBLE its entries under a
// replay, so the verb is refused (the acknowledgment covers normal-operation
// duplicates, not an operator-triggered wholesale re-append).
func (s *Syncable) CanRematerialize() bool {
	t, err := s.types.ResolveType(cluster.LatestTypeRef(s.config.TargetTopic))
	if err != nil || t == nil {
		return false
	}
	return t.EntityKind == cluster.EntityKindSnapshot
}

// BeginRematerialization is a no-op: the loopback needs no epoch marking —
// see CanRematerialize for why a plain replay already converges.
func (s *Syncable) BeginRematerialization(context.Context, uint64) error { return nil }

// CompleteRematerialization is a no-op: nothing to sweep.
func (s *Syncable) CompleteRematerialization(context.Context) error { return nil }

// Transform projects data through mappings. Empty mappings pass the payload
// through verbatim (never decoded — non-JSON payloads derive fine). With
// mappings, the payload decodes with number preservation (json.Number, so
// large integers and decimals re-marshal byte-exactly) and the output object
// marshals with sorted keys — a canonical, deterministic byte encoding, so a
// replay reproduces the derived topic's bytes exactly.
func Transform(data []byte, mappings []Mapping) ([]byte, error) {
	if len(mappings) == 0 {
		return data, nil
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	out := make(map[string]any, len(mappings))
	for _, m := range mappings {
		v, err := jsonpath.Get(m.JsonPath, doc)
		if err != nil {
			return nil, fmt.Errorf("jsonpath %q for field %q: %w", m.JsonPath, m.Field, err)
		}
		out[m.Field] = v
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("encode derived payload: %w", err)
	}
	return b, nil
}
