package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
)

// The JSON shape census: the snapshot pass already streams every historical
// row through committed, so the ingest worker folds each row's payload SHAPE
// into a per-topic census — the bootstrap the type-drafting flow needs
// (inference is bootstrap-only; the runtime never infers, it reconciles via
// the validation tripwire). The fold's fast path is a fingerprint lookup: an
// already-seen shape costs one signature computation and a counter bump, no
// per-row merging, so large tables pay ~nothing beyond reading them.

// ingestableCensusType is the census record's internal system type. Ungated:
// an older node skips it — a stale census until upgrade is an observability
// gap, never a correctness one. Snapshot kind: one current census per
// ingestable, superseded records compactable.
var ingestableCensusType = registerSystemType(&Type{
	ID:         reservedSystemID(compatUngated, 2),
	Name:       "InternalIngestableCensus",
	Version:    1,
	EntityKind: EntityKindSnapshot,
}, AdmissionConfig)

func IsIngestableCensus(id string) bool {
	return id == ingestableCensusType.ID
}

// ShapeCensus is one distinct payload shape observed on a topic.
type ShapeCensus struct {
	// Shape is the signature's path list ("$.caption:string", ...).
	Shape []string `json:"shape"`
	// Count is how many rows carried exactly this shape.
	Count uint64 `json:"count"`
	// FirstRow/LastRow are 1-based row ordinals (within the snapshot pass at
	// this epoch) of the shape's first and latest sighting — the input the
	// errata authoring workflow needs to see interleaved shapes' ranges.
	FirstRow uint64 `json:"firstRow"`
	LastRow  uint64 `json:"lastRow"`
}

// PathValues is the bounded distinct-value set census keeps for one string
// path when value tracking is opted in (censusValues) — the input for enum
// drafting. Overflowed marks a path whose cardinality exceeded the limit;
// its Values stop accumulating (and the drafter proposes no enum for it).
type PathValues struct {
	Values     []string `json:"values"`
	Overflowed bool     `json:"overflowed"`
}

// TopicCensus is one topic's census at one refresh epoch. Shapes carry the
// durable state; the per-path view (paths → types, counts, first/last row) is
// DERIVED from them at render time (PathsView), which is what keeps the
// seen-shape fast path free of per-row merging.
type TopicCensus struct {
	Rows   uint64                  `json:"rows"`
	Shapes map[string]*ShapeCensus `json:"shapes"`
	// Values is present only when the ingestable opted into value tracking:
	// string path → its bounded distinct values.
	Values map[string]*PathValues `json:"values,omitempty"`
}

// PathCensus is the derived per-path view: every JSON type observed at the
// path, how many rows carry it, and the row-ordinal range it was seen in.
type PathCensus struct {
	Path     string   `json:"path"`
	Types    []string `json:"types"`
	Count    uint64   `json:"count"`
	FirstRow uint64   `json:"firstRow"`
	LastRow  uint64   `json:"lastRow"`
}

// IngestableCensus is one ingestable's census across its topics, at one
// refresh epoch.
type IngestableCensus struct {
	ID           string
	RefreshEpoch uint64
	Topics       map[string]*TopicCensus
}

// CensusOptions is the operator's census configuration, from the
// `[ingestable]` envelope (dialect-agnostic — the fold runs in the worker).
type CensusOptions struct {
	// Disabled turns the census off (`census = false`); it is ON by default —
	// the census only happens while a snapshot streams the table, and a
	// forgotten opt-in costs a full re-snapshot to get one later.
	Disabled bool
	// TrackValues (`censusValues = true`) opts into bounded distinct-value
	// tracking for string paths, the input for enum drafting. Opt-in because
	// it puts source VALUES into replicated census state (the PII posture:
	// types and paths only by default).
	TrackValues bool
	// ValueLimit (`censusValueLimit`) bounds the distinct values kept per
	// path; past it the path is marked overflowed and stops accumulating.
	// Defaults to DefaultCensusValueLimit.
	ValueLimit int
}

// DefaultCensusValueLimit bounds per-path distinct-value tracking (and is the
// drafter's enum-proposal threshold): low-cardinality means "at most this
// many distinct values seen".
const DefaultCensusValueLimit = 16

// Fold folds one snapshot-row payload into the topic's census. rows ordinals
// are per-topic. Returns an error only for a non-JSON payload (the caller
// logs and skips — the census must never disturb ingest).
func (tc *TopicCensus) Fold(data []byte, opts CensusOptions) error {
	var doc any
	if err := json.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("census fold: payload is not valid JSON: %w", err)
	}

	set := map[string]bool{}
	var values map[string]string
	if opts.TrackValues {
		values = map[string]string{}
	}
	walkCensus("$", doc, set, values)

	shape := make([]string, 0, len(set))
	for s := range set {
		shape = append(shape, s)
	}
	sort.Strings(shape)
	sum := sha256.Sum256([]byte(strings.Join(shape, "\n")))
	fp := hex.EncodeToString(sum[:16])

	tc.Rows++
	if s, ok := tc.Shapes[fp]; ok {
		// The fast path: an already-seen shape is a counter bump — no merge.
		s.Count++
		s.LastRow = tc.Rows
	} else {
		if tc.Shapes == nil {
			tc.Shapes = map[string]*ShapeCensus{}
		}
		tc.Shapes[fp] = &ShapeCensus{Shape: shape, Count: 1, FirstRow: tc.Rows, LastRow: tc.Rows}
	}

	// Value tracking cannot ride the fast path: a seen shape can still carry
	// a new value. Bounded per path, so the steady-state cost is a map probe.
	for path, v := range values {
		limit := opts.ValueLimit
		if limit <= 0 {
			limit = DefaultCensusValueLimit
		}
		if tc.Values == nil {
			tc.Values = map[string]*PathValues{}
		}
		pv := tc.Values[path]
		if pv == nil {
			pv = &PathValues{}
			tc.Values[path] = pv
		}
		if pv.Overflowed {
			continue
		}
		i := sort.SearchStrings(pv.Values, v)
		if i < len(pv.Values) && pv.Values[i] == v {
			continue
		}
		if len(pv.Values) >= limit {
			pv.Overflowed = true
			pv.Values = nil // past the bound the set is no longer meaningful — drop it
			continue
		}
		pv.Values = append(pv.Values, "")
		copy(pv.Values[i+1:], pv.Values[i:])
		pv.Values[i] = v
	}
	return nil
}

// walkCensus mirrors walkJSONShape and additionally captures string leaf
// values (into values, when non-nil) keyed by path.
func walkCensus(path string, v any, set map[string]bool, values map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		if len(t) == 0 {
			set[path+":object"] = true
			return
		}
		for k, child := range t {
			walkCensus(path+"."+k, child, set, values)
		}
	case []any:
		if len(t) == 0 {
			set[path+":array"] = true
			return
		}
		for _, child := range t {
			walkCensus(path+"[]", child, set, values)
		}
	case string:
		set[path+":string"] = true
		if values != nil {
			values[path] = t
		}
	case float64:
		set[path+":number"] = true
	case bool:
		set[path+":bool"] = true
	case nil:
		set[path+":null"] = true
	}
}

// PathsView derives the per-path census from the shapes: for each path, the
// union of observed types, the number of rows carrying it (the sum of its
// shapes' counts), and the row-ordinal range it was seen in. Sorted by path.
func (tc *TopicCensus) PathsView() []*PathCensus {
	byPath := map[string]*PathCensus{}
	for _, s := range tc.Shapes {
		for _, entry := range s.Shape {
			i := strings.LastIndex(entry, ":")
			if i < 0 {
				continue
			}
			path, typ := entry[:i], entry[i+1:]
			pc := byPath[path]
			if pc == nil {
				pc = &PathCensus{Path: path, FirstRow: s.FirstRow, LastRow: s.LastRow}
				byPath[path] = pc
			}
			if !contains(pc.Types, typ) {
				pc.Types = append(pc.Types, typ)
			}
			pc.Count += s.Count
			if s.FirstRow < pc.FirstRow {
				pc.FirstRow = s.FirstRow
			}
			if s.LastRow > pc.LastRow {
				pc.LastRow = s.LastRow
			}
		}
	}
	out := make([]*PathCensus, 0, len(byPath))
	for _, pc := range byPath {
		sort.Strings(pc.Types)
		out = append(out, pc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// Marshal encodes the census as its replicated record.
func (c *IngestableCensus) Marshal() ([]byte, error) {
	body, err := json.Marshal(c.Topics)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&clusterpb.LogIngestableCensus{
		ID:           c.ID,
		RefreshEpoch: c.RefreshEpoch,
		Census:       body,
	})
}

func (c *IngestableCensus) Unmarshal(bs []byte) error {
	lc := &clusterpb.LogIngestableCensus{}
	if err := proto.Unmarshal(bs, lc); err != nil {
		return err
	}
	c.ID = lc.ID
	c.RefreshEpoch = lc.RefreshEpoch
	c.Topics = map[string]*TopicCensus{}
	if len(lc.Census) == 0 {
		return nil
	}
	return json.Unmarshal(lc.Census, &c.Topics)
}

func NewIngestableCensusEntity(c *IngestableCensus) (*Entity, error) {
	bs, err := c.Marshal()
	if err != nil {
		return nil, err
	}
	return NewUpsertEntity(ingestableCensusType, []byte(c.ID), bs), nil
}

// DraftTypeSchema turns a topic's census into a draft JSON Schema for
// POST /type review — nested properties mirroring the observed paths,
// `additionalProperties: false` at every object level (so the tripwire later
// catches added fields), a type union where shapes disagreed, and — when
// value tracking was on — `enum` for string paths that stayed at or under
// the value limit (so the tripwire catches new enum values as schema
// violations). Inference is bootstrap-only: the draft is an operator review
// input, never auto-blessed.
func (tc *TopicCensus) DraftTypeSchema() ([]byte, error) {
	root := map[string]any{}
	for _, pc := range tc.PathsView() {
		insertDraftPath(root, strings.TrimPrefix(pc.Path, "$"), pc, tc.Values[pc.Path])
	}
	schema := draftNode(root)
	return json.MarshalIndent(schema, "", "  ")
}

// draftLeaf is a path's accumulated typing inside the draft tree.
type draftLeaf struct {
	types  []string
	values *PathValues
	// count is how many rows carried the path — the denominator for the
	// enum heuristic.
	count uint64
}

// insertDraftPath places one census path into the nested draft tree. The tree
// maps property name → either a nested map[string]any (an object), a
// *draftLeaf, or a draftArray wrapper.
type draftArray struct{ elem any }

func insertDraftPath(node map[string]any, rest string, pc *PathCensus, values *PathValues) {
	rest = strings.TrimPrefix(rest, ".")
	seg := rest
	tail := ""
	if i := strings.IndexByte(rest, '.'); i >= 0 {
		seg, tail = rest[:i], rest[i+1:]
	}
	arrays := 0
	for strings.HasSuffix(seg, "[]") {
		seg = strings.TrimSuffix(seg, "[]")
		arrays++
	}

	place := func(v any) any {
		for i := 0; i < arrays; i++ {
			v = &draftArray{elem: v}
		}
		return v
	}
	if tail == "" {
		leaf := &draftLeaf{types: pc.Types, values: values, count: pc.Count}
		merged := merge(node[seg], place(leaf))
		node[seg] = merged
		return
	}
	child := map[string]any{}
	insertDraftPath(child, tail, pc, values)
	node[seg] = merge(node[seg], place(child))
}

// merge combines two draft-tree values for one property (e.g. "$.a:object"
// from one shape and "$.a.b:string" from another, or an array wrapper with
// its sibling leaf). Maps merge recursively; a leaf beside a map keeps the
// map (the leaf's container type is implied); mismatched arrays merge their
// elements.
func merge(existing, incoming any) any {
	if existing == nil {
		return incoming
	}
	em, eok := existing.(map[string]any)
	im, iok := incoming.(map[string]any)
	if eok && iok {
		for k, v := range im {
			em[k] = merge(em[k], v)
		}
		return em
	}
	ea, eaok := existing.(*draftArray)
	ia, iaok := incoming.(*draftArray)
	if eaok && iaok {
		ea.elem = merge(ea.elem, ia.elem)
		return ea
	}
	el, elok := existing.(*draftLeaf)
	il, ilok := incoming.(*draftLeaf)
	if elok && ilok {
		for _, t := range il.types {
			if !contains(el.types, t) {
				el.types = append(el.types, t)
			}
		}
		sort.Strings(el.types)
		if el.values == nil {
			el.values = il.values
		}
		el.count += il.count
		return el
	}
	// Mixed kinds (a leaf recorded as ":object" beside real nested paths, a
	// bare ":array" beside "[]"-expanded elements): the structured side wins.
	if eok || eaok {
		return existing
	}
	return incoming
}

// draftNode renders a draft-tree node as a JSON Schema fragment.
func draftNode(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		props := map[string]any{}
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			props[k] = draftNode(t[k])
		}
		return map[string]any{
			"type":                 "object",
			"properties":           props,
			"additionalProperties": false,
		}
	case *draftArray:
		return map[string]any{"type": "array", "items": draftNode(t.elem)}
	case *draftLeaf:
		out := map[string]any{}
		types := make([]any, 0, len(t.types))
		for _, typ := range t.types {
			// The signature's bare container types mean "observed empty".
			switch typ {
			case "bool":
				typ = "boolean"
			case "object":
				return map[string]any{"type": "object", "additionalProperties": false}
			case "array":
				return map[string]any{"type": "array"}
			}
			types = append(types, typ)
		}
		if len(types) == 1 {
			out["type"] = types[0]
		} else {
			out["type"] = types
		}
		// The enum heuristic needs REPEAT evidence, not just low cardinality:
		// in a small census every free-text field is "low-cardinality", so an
		// enum is proposed only when distinct values < rows carrying the path
		// (some value repeated — the mark of a closed domain), under the
		// tracking bound, for pure-string paths.
		if t.values != nil && !t.values.Overflowed && len(t.values.Values) > 0 &&
			uint64(len(t.values.Values)) < t.count &&
			len(t.types) == 1 && t.types[0] == "string" {
			enum := make([]any, len(t.values.Values))
			for i, v := range t.values.Values {
				enum[i] = v
			}
			out["enum"] = enum
		}
		return out
	default:
		return map[string]any{}
	}
}
