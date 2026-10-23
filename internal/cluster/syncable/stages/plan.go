package stages

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// The plan is a projection's stages compiled to one spelling-free form:
// a list of query blocks, each one keyed fold with a fixed internal order
// (input → fan → joins → where → key → reduce → columns → having), which
// is SQL's own order for one SELECT. Blocks compose by naming earlier
// blocks as their input or join target.
//
// Spelling-free means that what the declared stages leave implicit, the
// plan states, and what two spellings say alike, the plan says once:
//
//   - every expression is compiled and printed canonically (Render);
//   - key types are resolved per key part, and a stage join carries the
//     key space it inherits from the stage it joins;
//   - merge aliases are explicit;
//   - the stages the engine writes itself (the re-key stage of a
//     field-addressed join, the lift stage of a topic merge side) are
//     blocks of their own;
//   - the clauses of one conjunction (a when, a join's where, a fold's
//     where) are sorted, since their order never changes a match.
//
// Forms that are not proven to mean the same stay distinct: latest's
// single-path order and its order-key list compare differently, the emit
// fold arms (sum = "…") are not unified with aggregate calls, a when arm
// is not rewritten as an expression, and paths keep their spelling.
//
// The plan's encoding is versioned, and its fingerprint (Fingerprint) is
// what a store's state is derived under. The compiler is a contract: the
// same stages compile to the same encoding in every release, and a release
// that must change the encoding bumps PlanVersion.

// PlanVersion is the version of the plan's encoding.
const PlanVersion = 1

// Plan is a projection's stages as query blocks, in manifest order with
// each engine-written block placed just before its first consumer.
type Plan struct {
	Version int     `json:"version"`
	Blocks  []Block `json:"blocks"`
}

// Block is one keyed fold.
type Block struct {
	Name string `json:"name"`
	// Synthetic names an engine-written block: SyntheticRekey (a
	// field-addressed join's target, keyed by that field) or
	// SyntheticLift (a topic merge side, keyed by its declared key). Both
	// pass their input through unchanged.
	Synthetic string `json:"synthetic,omitempty"`
	// From is the input: one topic or earlier block, or several read as
	// one stream (UNION ALL). Merge is the other input form: earlier
	// blocks combined by key (FULL OUTER JOIN USING the key).
	From  []string    `json:"from,omitempty"`
	Merge []MergeSide `json:"merge,omitempty"`
	// Fan fans each input into its array elements (CROSS JOIN LATERAL):
	// one step is forEach, several are the multi-arm fan, each applying to
	// the inputs its where matches. ElementKey is an element's identity
	// when it is not the key's first part.
	Fan        []FanStep   `json:"fan,omitempty"`
	ElementKey string      `json:"elementKey,omitempty"`
	Joins      []JoinStep  `json:"joins,omitempty"`
	Where      []Predicate `json:"where,omitempty"`
	// Key is the fold's key; nil for a merge (its sides' key) and a
	// window (each input keeps its own row).
	Key    *Key   `json:"key,omitempty"`
	Reduce Reduce `json:"reduce"`
	// Columns are the block's output; none for a synthetic block.
	Columns []Column `json:"columns,omitempty"`
	Having  string   `json:"having,omitempty"`
}

// The engine-written block kinds.
const (
	SyntheticRekey = "rekey"
	SyntheticLift  = "lift"
)

// MergeSide is one side of a merge: an earlier block and its alias.
type MergeSide struct {
	Block string `json:"block"`
	As    string `json:"as"`
}

// FanStep is one fan arm: the array path, and the inputs it applies to.
type FanStep struct {
	Path  string      `json:"path"`
	Where []Predicate `json:"where,omitempty"`
}

// JoinStep is one join, in the vocabulary of Join. Exactly one of Topic
// and Block is set. OnTypes has one entry per On path; for a join on an
// earlier block's key it, and Normalize, are that block's.
type JoinStep struct {
	Topic     string      `json:"topic,omitempty"`
	Block     string      `json:"block,omitempty"`
	On        []string    `json:"on"`
	OnTypes   []string    `json:"onTypes"`
	Normalize string      `json:"normalize,omitempty"`
	Field     string      `json:"field,omitempty"`
	Absent    bool        `json:"absent,omitempty"`
	Optional  bool        `json:"optional,omitempty"`
	Fan       bool        `json:"fan,omitempty"`
	As        string      `json:"as,omitempty"`
	Where     []Predicate `json:"where,omitempty"`
	OrderBy   []SortKey   `json:"orderBy,omitempty"`
	TieBy     []SortKey   `json:"tieBy,omitempty"`
	AsOf      string      `json:"asOf,omitempty"`
}

// Key is a fold key: its parts (paths as written, expressions rendered),
// each part's type, and the normalization applied to the rendered key.
type Key struct {
	Parts     []string `json:"parts,omitempty"`
	Types     []string `json:"types,omitempty"`
	Normalize string   `json:"normalize,omitempty"`
}

// The reduce kinds.
const (
	ReduceReshape   = "reshape"
	ReduceLatest    = "latest"
	ReduceAggregate = "aggregate"
	ReduceLiveSet   = "liveSet"
	ReduceWindow    = "window"
)

// Reduce is how a key's inputs fold to its output.
type Reduce struct {
	Kind string `json:"kind"`
	// latest's single-path form: the order and tie paths and their types.
	OrderPath string `json:"orderPath,omitempty"`
	OrderType string `json:"orderType,omitempty"`
	TiePath   string `json:"tiePath,omitempty"`
	TieType   string `json:"tieType,omitempty"`
	// latest's order-key form, and a window's order.
	OrderBy []SortKey `json:"orderBy,omitempty"`
	TieBy   []SortKey `json:"tieBy,omitempty"`
	// A window's partition (parts as in Key).
	PartitionBy []string `json:"partitionBy,omitempty"`
	// liveSet's delete evidence.
	DeleteWhen []Predicate `json:"deleteWhen,omitempty"`
}

// SortKey is one order key.
type SortKey struct {
	Expr       string `json:"expr"`
	Desc       bool   `json:"desc,omitempty"`
	NullsFirst bool   `json:"nullsFirst,omitempty"`
}

// Column is one output column: a path (From), an expression (Expr), or a
// fold arm (Fold over Arg, filtered by Where).
type Column struct {
	Name     string      `json:"name"`
	From     string      `json:"from,omitempty"`
	Expr     string      `json:"expr,omitempty"`
	Fold     string      `json:"fold,omitempty"`
	Arg      string      `json:"arg,omitempty"`
	Distinct bool        `json:"distinct,omitempty"`
	Where    []Predicate `json:"where,omitempty"`
}

// The predicate ops: a when clause's arms.
const (
	OpEquals      = "equals"
	OpNotEquals   = "notEquals"
	OpNull        = "null"
	OpNotNull     = "notNull"
	OpGreaterThan = "greaterThan"
	OpLessThan    = "lessThan"
	OpExpr        = "expr"
)

// Predicate is one when clause: Op against Path (with Value for a
// comparison), or an expression.
type Predicate struct {
	Op    string   `json:"op"`
	Path  string   `json:"path,omitempty"`
	Value *Literal `json:"value,omitempty"`
	Expr  string   `json:"expr,omitempty"`
}

// Literal is a comparison literal as the config decoded it. It encodes
// with its type, so literals that print alike but decode to different
// types (5 and 5.0) stay distinct.
type Literal struct{ v any }

// NewLiteral wraps a decoded comparison literal.
func NewLiteral(v any) *Literal { return &Literal{v} }

// Value returns the literal.
func (l *Literal) Value() any { return l.v }

// MarshalJSON implements json.Marshaler.
func (l *Literal) MarshalJSON() ([]byte, error) {
	var typ, text string
	switch v := l.v.(type) {
	case string:
		typ, text = "string", v
	case bool:
		typ, text = "bool", strconv.FormatBool(v)
	case int64:
		typ, text = "int", strconv.FormatInt(v, 10)
	case int:
		typ, text = "int", strconv.Itoa(v)
	case float64:
		typ, text = "float", strconv.FormatFloat(v, 'g', -1, 64)
	default:
		typ, text = fmt.Sprintf("%T", v), fmt.Sprintf("%v", v)
	}
	return json.Marshal(struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}{typ, text})
}

// Encode returns the plan's canonical encoding: compact JSON, with no
// HTML escaping (an expression's < and > stay as written).
func (p *Plan) Encode() []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(p); err != nil {
		// The plan is plain data; encoding cannot fail on it.
		panic(fmt.Sprintf("stages: encode plan: %v", err))
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// Fingerprint identifies the plan: the hash of its canonical encoding.
func (p *Plan) Fingerprint() string {
	sum := sha256.Sum256(p.Encode())
	return hex.EncodeToString(sum[:])
}

// sortPredicates orders a conjunction's clauses by their encoding.
func sortPredicates(ps []Predicate) {
	type keyed struct {
		key string
		p   Predicate
	}
	ks := make([]keyed, len(ps))
	for i := range ps {
		bs, _ := json.Marshal(&ps[i])
		ks[i] = keyed{string(bs), ps[i]}
	}
	sort.SliceStable(ks, func(a, b int) bool { return ks[a].key < ks[b].key })
	for i := range ks {
		ps[i] = ks[i].p
	}
}
