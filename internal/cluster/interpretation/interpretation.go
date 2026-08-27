// Package interpretation resolves the AUTHORITATIVE reading of a committed
// entity: its stamped type version ⊕ the errata fold. The stamp is a cache of
// the default reading in force at write time; errata are append-only,
// consensus-ordered statements that rebind readings for index ranges (see
// cluster.Erratum). The fold is deterministic — among matching errata, later
// in the log wins, and matching is always against the stamped version — so
// replaying data + errata history yields identical readings at every
// (data index, interpretation index) pair.
//
// The errata-free fast path costs one nil-map lookup: a topic with no errata
// sees no read-path overhead (bench-guarded).
package interpretation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/itchyny/gojq"

	"github.com/committeddb/committed/internal/cluster"
)

// predicateTimeout bounds one predicate evaluation, mirroring the migration
// runner's posture: an operator-supplied program must not hang a sync worker.
const predicateTimeout = 30 * time.Second

// compiled is one erratum with its predicate parsed once.
type compiled struct {
	cluster.Erratum
	// order is the erratum's raft index — the interpretation coordinate that
	// makes later-in-log-wins deterministic.
	order uint64
	// query is the parsed predicate; nil for a pure range binding.
	query *gojq.Query
	// tracker classifies this predicate's evaluation failures — entry-specific
	// (one payload the program can't evaluate) vs config-shaped (a predicate
	// wrong for every row it gates); see cluster.AmbiguityTracker. Allocated
	// only alongside query. It is the one mutable, internally-synchronized
	// field in the otherwise immutable snapshot; because the snapshot is
	// shared, consumers of the same topic pool evidence (which only
	// accelerates the loud, recoverable outcome), and every registry
	// recompile starts fresh — correct, since the registry just changed.
	tracker *cluster.AmbiguityTracker
}

// Registry is a compiled snapshot of the errata registry: build it from the
// applied records (in raft-index order), swap it atomically on change. Its
// semantic content — errata, ranges, predicates — is immutable and read
// lock-free; the only mutable state is each predicate candidate's ambiguity
// tracker (internally synchronized).
type Registry struct {
	// byType holds each type's errata in log order. A type with no errata is
	// simply absent — the fast path.
	byType map[string][]compiled
	// highwater is the highest erratum raft index in the snapshot: the
	// registry's interpretation index.
	highwater uint64
}

// EmptyRegistry is the zero registry every reader starts from.
var EmptyRegistry = &Registry{}

// NewRegistry compiles applied errata (any order; sorted internally by raft
// index) into an immutable snapshot. A predicate that fails to parse — which
// admission prevents — fails the build rather than silently dropping the
// erratum: a half-folded registry would read differently on different nodes.
func NewRegistry(applied []cluster.AppliedErratum) (*Registry, error) {
	r := &Registry{byType: make(map[string][]compiled, 8)}
	for _, a := range applied {
		c := compiled{Erratum: a.Erratum, order: a.Index}
		if a.Erratum.Predicate != "" {
			q, err := gojq.Parse(a.Erratum.Predicate)
			if err != nil {
				return nil, fmt.Errorf("erratum %q: predicate does not parse: %w", a.Erratum.ID, err)
			}
			c.query = q
			c.tracker = cluster.NewAmbiguityTracker()
		}
		r.byType[a.Erratum.TypeID] = append(r.byType[a.Erratum.TypeID], c)
		if a.Index > r.highwater {
			r.highwater = a.Index
		}
	}
	for _, cs := range r.byType {
		// Insertion sort by raft index: applied lists arrive nearly sorted.
		for i := 1; i < len(cs); i++ {
			for j := i; j > 0 && cs[j-1].order > cs[j].order; j-- {
				cs[j-1], cs[j] = cs[j], cs[j-1]
			}
		}
	}
	return r, nil
}

// Highwater is the highest erratum raft index folded into this snapshot — the
// registry's interpretation index (0 for an empty registry).
func (r *Registry) Highwater() uint64 {
	if r == nil {
		return 0
	}
	return r.highwater
}

// TypeHighwater is the highest erratum raft index affecting the given type
// (0 when the type has no errata) — the per-topic staleness comparator.
func (r *Registry) TypeHighwater(typeID string) uint64 {
	if r == nil {
		return 0
	}
	var hw uint64
	for _, c := range r.byType[typeID] {
		if c.order > hw {
			hw = c.order
		}
	}
	return hw
}

// EffectiveVersion resolves the authoritative reading of the entity at
// dataIndex: the stamped version unless a matching erratum rebinds it — among
// matching errata, later in the log wins. The payload is unmarshaled at most
// once, and only when a matching candidate carries a predicate. Errors only
// when a predicate cannot evaluate (a non-JSON payload, or a program the
// payload's shape breaks) — never a silent fall-through to a possibly-wrong
// reading — and the error comes back classified for egress by the erratum's
// own tracker: cluster.Permanent while the failure may be entry-specific,
// cluster.ErrConfigShaped (transient — the worker wedges) once a run of
// consecutive distinct rows with no clean evaluation establishes the
// predicate config-shaped. Rows an erratum's range never gates don't touch
// its tracker, so unrelated successes can't mask a predicate that fails
// every row it actually evaluates.
func (r *Registry) EffectiveVersion(ctx context.Context, typeID string, dataIndex uint64, stampedVersion int, payload []byte) (int, error) {
	if r == nil || r.byType == nil {
		return stampedVersion, nil
	}
	cs := r.byType[typeID]
	if cs == nil {
		return stampedVersion, nil // the errata-free fast path
	}

	effective := stampedVersion
	var doc any
	docReady := false
	for i := range cs {
		c := &cs[i]
		if !c.Matches(dataIndex, stampedVersion) {
			continue
		}
		if c.query != nil {
			if !docReady {
				dec := json.NewDecoder(bytes.NewReader(payload))
				dec.UseNumber()
				if err := dec.Decode(&doc); err != nil {
					// Classified through the forcing candidate's tracker: one
					// malformed row is entry-specific; a predicate erratum
					// admitted against a non-JSON topic fails every gated row.
					return 0, c.tracker.Classify(dataIndex, fmt.Errorf("erratum %q predicate: payload is not valid JSON: %w", c.ID, err))
				}
				docReady = true
			}
			match, err := evalPredicate(ctx, c.query, doc)
			if err != nil {
				return 0, c.tracker.Classify(dataIndex, fmt.Errorf("erratum %q predicate: %w", c.ID, err))
			}
			// A clean evaluation — match or not — proves the predicate can
			// read this data; reset its evidence.
			c.tracker.Succeeded()
			if !match {
				continue
			}
		}
		effective = c.RebindToVersion // later in the log wins: keep overwriting
	}
	return effective, nil
}

// evalPredicate runs a compiled predicate on the decoded payload; only a
// literal true is a match (jq's truthiness would make `.field` alone match
// any non-null value — too easy to write an accidental match-all).
func evalPredicate(ctx context.Context, q *gojq.Query, doc any) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, predicateTimeout)
	defer cancel()
	iter := q.RunWithContext(ctx, doc)
	v, ok := iter.Next()
	if !ok {
		return false, nil // no output = no match
	}
	if err, isErr := v.(error); isErr {
		return false, err
	}
	b, isBool := v.(bool)
	return isBool && b, nil
}
