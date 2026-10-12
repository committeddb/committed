package stages

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/committeddb/committed/internal/cluster"
)

// Stage key/order comparison types (numeric vs lexical).
const (
	KeyTypeText   = "text"
	KeyTypeNumber = "number"
)

// ValidateWhen checks when-clause shapes: exactly one arm per clause
// (a scalar arm against a path, or a standalone expr), scalar literals
// only, no empty paths — and it compiles expr arms in place so matching
// never re-parses.
func ValidateWhen(clauses []WhenClause, where string) error {
	for k := range clauses {
		cl := &clauses[k]
		if cl.Expr != "" {
			if cl.Path != "" {
				return fmt.Errorf("%s: when entry: expr stands alone — the expression addresses its own paths; drop path", where)
			}
			if cl.Equals != nil || cl.Null || cl.NotNull || cl.NotEquals != nil || cl.GreaterThan != nil || cl.LessThan != nil {
				return fmt.Errorf("%s: when entry: exactly one arm — expr excludes the scalar arms (put the comparison inside the expression)", where)
			}
			compiled, err := Compile(cl.Expr)
			if err != nil {
				return fmt.Errorf("%s: when expr: %w", where, err)
			}
			cl.compiled = compiled
			continue
		}
		if cl.Path == "" {
			return fmt.Errorf("%s: when entry with empty path", where)
		}
		arms := 0
		for _, set := range []bool{cl.Equals != nil, cl.Null, cl.NotNull, cl.NotEquals != nil, cl.GreaterThan != nil, cl.LessThan != nil} {
			if set {
				arms++
			}
		}
		if arms != 1 {
			return fmt.Errorf("%s: when entry for %q: exactly one of equals, null, notNull, notEquals, greaterThan, lessThan, or expr is required", where, cl.Path)
		}
		for arm, lit := range map[string]any{"equals": cl.Equals, "notEquals": cl.NotEquals} {
			if lit != nil && !IsScalar(lit) {
				return fmt.Errorf("%s: when entry for %q: %s must be a scalar literal, got %T", where, cl.Path, arm, lit)
			}
		}
		for arm, lit := range map[string]any{"greaterThan": cl.GreaterThan, "lessThan": cl.LessThan} {
			if lit == nil {
				continue
			}
			if s, ok := lit.(string); ok {
				if _, isNum := numericDigits(s); isNum {
					return fmt.Errorf("%s: when entry for %q: %s = %q is a quoted number — write it unquoted to compare numerically (as text, \"10\" sorts before \"9\"); for a deliberate text comparison of digits use an expr arm", where, cl.Path, arm, s)
				}
				continue
			}
			if _, ok := ratFromScalar(lit); !ok {
				return fmt.Errorf("%s: when entry for %q: %s takes a number or text literal, got %T", where, cl.Path, arm, lit)
			}
		}
	}
	return nil
}

// Stage is one internal stage of a staged computation: a keyed
// refold from ONE input (a topic, or a PRIOR stage by name) into a private
// keyed object held in the syncable's stage store — never a topic, never a
// surface write (the terminal rule: only the table is outward-facing). Stages
// chain by name in manifest order; a table source consumes a stage's
// output via `from = "<stage name>"`.
type Stage struct {
	Name string `json:"name,omitempty"`
	From string `json:"from,omitempty"`
	// FromList is SQL's UNION ALL: the stage reads every listed topic or
	// prior stage as rows of ONE input stream (the TOML spelling is a list
	// `from`). Each input's identity is (source name, input key), so equal
	// keys from different sources stay separate rows, and source() names
	// the input's source. Two or more entries; a single source is From,
	// whose fingerprint a list never touches.
	FromList []string     `mapstructure:"fromList" json:"fromList,omitempty"`
	KeyPath  []string     `json:"keyPath,omitempty"`
	When     []WhenClause `json:"when,omitempty"`
	// Reduce: "" (reshape: one input object → one output object),
	// "aggregate" (fold arms over the key's retained inputs), or "latest"
	// (argmax: the key's output is the emit of the WINNING input by
	// OrderBy — a business field, never arrival order — with TieBy the
	// MANDATORY deterministic tiebreak; the field measured ties diverging
	// 37/276,286 without one). OrderByType/TieByType choose numeric vs
	// lexical comparison (text default — ISO dates order correctly as
	// text). The stage's `when` filters BEFORE the argmax by
	// construction (an unmatched input retracts from the retained set),
	// so an unapproved newer input never shadows an approved older one.
	Reduce      string `json:"reduce,omitempty"`
	OrderBy     string `json:"orderBy,omitempty"`
	OrderByType string `json:"orderByType,omitempty"`
	TieBy       string `json:"tieBy,omitempty"`
	TieByType   string `json:"tieByType,omitempty"`
	Joins       []Join `json:"joins,omitempty"`
	Emit        []Emit `json:"emit,omitempty"`
	// Having filters an aggregate stage's FOLDED keys — SQL's HAVING: an
	// expression over aggregate calls (`count(*) > 5`), evaluated after
	// the fold. A key whose having is not TRUE has no output: it retracts
	// downstream, and re-enters when the predicate flips back.
	Having string `mapstructure:"having" json:"having,omitempty"`
	// aggSlots are the aggregate calls of the emit exprs and having, in
	// fold order; havingCompiled is Having's AST. Populated by
	// ValidateShapes.
	aggSlots       []exprAgg
	havingCompiled Node
	// ForEach fans each input into N element-inputs (the deliberately
	// multi-valued path selects them): keyPath and emit/join paths resolve
	// against the ELEMENT, `$parent.` reaches the enclosing input, and a
	// re-emitted input reconciles (vanished elements retract; the input's
	// tombstone retracts them all). Elements feed the stage's reduce like
	// any input, so forEach + aggregate is fan-then-fold in one stage.
	ForEach string `json:"forEach,omitempty"`
	// DeleteWhen (reduce = "liveSet" only) classifies delete-shaped
	// events: a key is LIVE while it has qualifying inputs and ZERO
	// inputs matching DeleteWhen — created-minus-deleted as a set
	// difference, no ordering needed. A delete-shaped event is retained
	// as NEGATIVE evidence (it skips the when filter), so its own
	// retraction un-deletes the key. The live key emits from its
	// bytewise-largest non-delete input, like a reshape.
	DeleteWhen []WhenClause `json:"deleteWhen,omitempty"`
	// ElementKey is a fanned element's IDENTITY (element-scoped path) when
	// it differs from keyPath — the aggregate sidecar's ElementKey
	// precedent. keyPath is the REDUCE key (which output an element folds
	// into); ElementKey is which retained input it IS (what a re-delivery
	// replaces). Defaults to keyPath: fine for 1:1 fan (element id = row
	// id), required when a reduce folds multiple same-key elements (two
	// same-workarea amounts must both count).
	ElementKey string `json:"elementKey,omitempty"`
	// Normalize folds this stage's keyPath rendering into a canonical
	// form ("lower" — the cross-source GUID-case seam; see
	// NormalizeLower). Keys only, never emitted values. A consumer
	// addressing this stage (a stage-fed source, a stage join) sees the
	// normalized keys; declare the same normalize on any sibling source
	// sharing the key space.
	Normalize string `mapstructure:"normalize" json:"normalize,omitempty"`
	// Fan is the multi-arm forEach — SQL's UNION ALL of lateral fans:
	// each arm's when selects which INPUTS it applies to (the input is
	// the unit here, not the element), and every matching arm fans its
	// forEach path's elements. Element identity is namespaced by arm,
	// so equal element keys via different arms stay distinct inputs.
	// Exactly one of forEach or fan; two or more arms (one arm IS
	// forEach). The doubled-fan killer: created-elements and
	// added-elements fold through ONE stage instead of parallel fan
	// stages, parallel aggregates, and a union merge.
	Fan []FanArm `mapstructure:"fan" json:"fan,omitempty"`
	// identityEmit marks an engine-SYNTHESIZED identity reshape (see
	// expandSynthetic): it emits its input object verbatim. Never
	// declarable; unexported, so it is invisible to config decode and
	// the fingerprint.
	identityEmit bool
	// Merge combines PRIOR stages BY KEY (SQL's FULL OUTER JOIN
	// USING(key), aliased): for each key any listed upstream holds, the
	// fold unit is a tuple scoping each upstream's current output under
	// its alias ($.quoted.total). The key is live while any side is —
	// gate to left/inner with when notNull/null on an alias path (the
	// when unit rule: when filters the stage's fold unit). A merge
	// declares NO keyPath/keyType/normalize: its key space is inherited
	// from the merged stages, which admission requires to agree — the
	// merge sits entirely downstream of key rendering and adopts
	// upstream keys byte-verbatim. Same-key correlation belongs here;
	// foreign-key reference belongs to joins.
	Merge []MergeEntry `mapstructure:"merge" json:"merge,omitempty"`
	// KeyType declares each key part's comparison space, SQL's
	// declared-column-type model in this vocabulary: "text" (the
	// default — strings verbatim, typed numbers in canonical digits;
	// exactly the undeclared behavior) or "number", which additionally
	// COERCES string renderings — a producer that serializes 5 as
	// "5.0000" folds onto the same key as one that sends the number 5,
	// the cross-source hazard byte rendering alone cannot close. A
	// value that cannot render into its declared space (a non-numeric
	// string under "number") is non-membership, like a missing key
	// part. One entry per keyPath position; a single value broadcasts.
	// Joins addressing this stage inherit these types (like Normalize).
	KeyType []string `mapstructure:"keyType" json:"keyType,omitempty"`
	// PartitionBy, WindowOrderBy, and WindowTieBy declare a window stage's
	// window (reduce = "window") — SQL's named WINDOW w AS (PARTITION BY …
	// ORDER BY …). A window stage keeps ONE ROW PER INPUT, keyed by the
	// input's own key, and its emit exprs read each row's position in its
	// ordered partition through window calls (row_number(), lag(x),
	// sum(x) …). PartitionBy groups rows (paths or expressions, like
	// keyPath; none = one partition); WindowOrderBy is the business order
	// (expressions, each optionally asc|desc and nulls first|last), whose
	// equal values make rows PEERS (rank and the running aggregates treat
	// peers alike, as PostgreSQL does); WindowTieBy orders rows within a
	// peer group — required when a tie-sensitive function (row_number,
	// lag/lead, first_value/last_value) is used — and the input key breaks
	// any tie left after it. The TOML spelling is orderBy/tieBy; the
	// field names differ from latest's single-path OrderBy/TieBy so that
	// existing configs keep their fingerprints.
	//
	// WindowOrderBy/WindowTieBy are also latest's LIST form (orderBy/tieBy
	// written as a list, an expression, or with a direction): the winner
	// is the input with the greatest (order, tie) sort key in the value
	// order — so desc means the earliest wins, and, as in PostgreSQL's
	// DISTINCT ON … ORDER BY … DESC, a null order value is greatest unless
	// declared nulls first — with the input identity breaking an exact
	// tie. A single bare path keeps the single-path form and its
	// comparison (OrderBy/OrderByType), byte-identical.
	PartitionBy   []string `mapstructure:"partitionBy" json:"partitionBy,omitempty"`
	WindowOrderBy []string `mapstructure:"windowOrderBy" json:"windowOrderBy,omitempty"`
	WindowTieBy   []string `mapstructure:"windowTieBy" json:"windowTieBy,omitempty"`
	// winOrder/winTie are the compiled order keys and winSlots the window
	// calls of the emit exprs, in slot order. Populated by ValidateShapes.
	winOrder, winTie []winKey
	winSlots         []exprWin
}

// FanArm is one arm of a multi-arm fan (see Stage.Fan).
type FanArm struct {
	ForEach string       `mapstructure:"forEach" json:"forEach,omitempty"`
	When    []WhenClause `mapstructure:"when" json:"when,omitempty"`
}

// hasFan reports whether the stage fans (single-path forEach or
// multi-arm fan) — the guards that care about "fanned" care about both.
func (st *Stage) hasFan() bool {
	return st.ForEach != "" || len(st.Fan) > 0
}

// Sources lists the stage's input sources: its From, or every entry of
// its FromList (nil for a merge).
func (st *Stage) Sources() []string {
	if len(st.FromList) > 0 {
		return st.FromList
	}
	if st.From != "" {
		return []string{st.From}
	}
	return nil
}

// isUnion reports whether the stage reads several sources (FromList).
func (st *Stage) isUnion() bool { return len(st.FromList) > 0 }

// inputKey is an input's identity in the stage: the source's key
// verbatim for a single source; for a union, (source name, key) through
// the composite encoding, so equal keys from different sources stay
// distinct — and a window stage's rows, which key by this identity,
// decode as a two-part key (source, input key) at a table.
func (st *Stage) inputKey(src string, key []byte) []byte {
	if !st.isUnion() {
		return key
	}
	return []byte(OutKey([]string{src, string(key)}))
}

// inputSource recovers the source an input came from, given its
// identity in the stage (inputKey, framed by fanInKey for a fanned
// element). "" means a merge stage, which has no source.
func (st *Stage) inputSource(inKey []byte) string {
	if !st.isUnion() {
		return st.From
	}
	if st.hasFan() || st.fanJoin() >= 0 {
		// A fanned element's identity is fanInKey's frame: the parent
		// input's identity, length-prefixed, then the element key.
		n64, w := binary.Uvarint(inKey)
		if w <= 0 {
			return ""
		}
		rest := inKey[w:]
		n := int(n64) //nolint:gosec // G115: bounds-checked against len(rest) next line; a forged length fails there
		if n < 0 || n > len(rest) {
			return ""
		}
		inKey = rest[:n]
	}
	return st.identitySource(inKey)
}

// identitySource recovers the source of an input identity (inputKey's
// result, never fan-framed).
func (st *Stage) identitySource(inKey []byte) string {
	if !st.isUnion() {
		return st.From
	}
	parts, err := cluster.DecodeCompositeKey(string(inKey), 2)
	if err != nil {
		return ""
	}
	return parts[0]
}

// MergeEntry names one merged upstream and its alias in the tuple
// scope. As defaults to the stage name; admission requires the
// effective alias to be a path-safe identifier (add `as` for names
// jsonpath's dot syntax cannot address, e.g. dashed stage names).
type MergeEntry struct {
	Stage string `mapstructure:"stage" json:"stage,omitempty"`
	As    string `mapstructure:"as" json:"as,omitempty"`
	// Topic makes this side a TOPIC (exactly one of stage or topic)
	// with its own DECLARED key space — the lift stage, synthesized:
	// the engine folds the topic through a hidden identity reshape
	// ("<topic>[<keyPath>]" in stats) feeding the merge as a normal
	// side, so pw-quoted-class scaffolding is written by the machine.
	Topic     string   `mapstructure:"topic" json:"topic,omitempty"`
	KeyPath   []string `mapstructure:"keyPath" json:"keyPath,omitempty"`
	KeyType   []string `mapstructure:"keyType" json:"keyType,omitempty"`
	Normalize string   `mapstructure:"normalize" json:"normalize,omitempty"`
}

// Join is one filtering join of a stage: the stage's inputs
// participate only while the joined topic's row — addressed by the
// input's On value against the joined entity's KEY — exists and matches
// every Where clause. A dimension change refolds every dependent key
// (reverse-index fan-out); a dimension that has not arrived yet fails
// participation and heals when it lands. Joins FILTER (gap 6) — field
// resolution from joins is a later arm.
type Join struct {
	Topic string `mapstructure:"topic" json:"topic,omitempty"`
	// From joins against a PRIOR stage instead of a topic (exactly one of
	// Topic or From): the dimension rows are that stage's outputs, keyed
	// by its out key and maintained by the drain — so cross-stage
	// correlation is a join, not a second input. Manifest order applies:
	// the joined stage must be declared earlier.
	From string `mapstructure:"from" json:"from,omitempty"`
	// On locates the joined row's key in the input: one path for a
	// single-part key, several for a composite — rendered through the
	// same positional composite encoding stage keys and composite topic
	// entity keys use (values only; column names are never part of the
	// encoding), so a pair-keyed dimension is addressed symmetrically
	// whether it is a stage or a topic. For a stage join the arity must
	// match the joined stage's keyPath; a topic's key shape is the
	// producer's (decoupled by design), so arity there is the config
	// author's contract.
	On []string `mapstructure:"on" json:"on,omitempty"`
	// Absent inverts the join into an ANTI-join: the input participates
	// only while NO dimension row matches (none exists, or none passes
	// Where). The fan-out gives the hard half for free: when a matching
	// row ARRIVES, dependents refold and retract — and when it leaves or
	// stops matching, they heal back in. A missing On value is vacuously
	// absent (nothing to reference), so it participates.
	Absent bool         `mapstructure:"absent" json:"absent,omitempty"`
	Where  []WhenClause `mapstructure:"where" json:"where,omitempty"`
	// Field addresses a STAGE dimension by an EMITTED FIELD instead of
	// its key — SQL's join-on-any-column, the lp-bypid killer. The
	// engine synthesizes the re-key stage authors used to write by hand
	// (visible in stats as "<stage>[<field>]"; same runtime cost as the
	// manual stage). Non-unique field values resolve to the
	// bytewise-largest producer key's row — the reshape collision rule,
	// deterministic. Stage joins only; with field set, on takes exactly
	// one path, and normalize/onType ARE declarable (a non-key field is
	// not governed by the producer's key space).
	Field string `mapstructure:"field" json:"field,omitempty"`
	// As names the matched dimension row in the stage's REFOLD scope —
	// the reference-lookup half of joins (SQL: a joined table's columns;
	// a join that names its row can read it). Emit from/expr, aggregate
	// fold arms, and per-emit where address it as $.<as>.<field>; the
	// stage's when and keyPath see only the input (filter on the joined
	// row with the join's where). Lookups are refold-time state, never
	// retained copies: a dimension change refolds dependents, so pulled
	// values track their source. An alias shadows a same-named input
	// field (jsonpath has no ambiguity errors; the alias wins).
	As string `mapstructure:"as" json:"as,omitempty"`
	// Optional makes a NAMED join SQL's LEFT JOIN: a missing dimension
	// row (or one failing the join's where) scopes the alias as null
	// instead of gating membership. Requires As — an unnamed join only
	// filters, and an optional filter is no filter.
	Optional bool `mapstructure:"optional" json:"optional,omitempty"`
	// Normalize folds BOTH sides of this join's key comparison — the
	// input's on rendering AND (for a topic join) the topic's entity-key
	// rendering as it lands in this join's dimension rows — so a
	// lowercase reference matches an UPPERCASE-keyed CDC dimension.
	// Declarable on TOPIC joins only: a stage join INHERITS the joined
	// stage's normalize (BuildGraph resolves it), because its dimension
	// keys are that stage's outputs and the references must render in
	// that key space — declaring it separately could only agree or be a
	// silent mismatch (the field defect: an UPPERCASE reference against
	// lowered stage keys, an anti-join that suppressed nothing).
	Normalize string `mapstructure:"normalize" json:"normalize,omitempty"`
	// OnType declares the reference parts' comparison spaces (see
	// Stage.KeyType) for a TOPIC join — the topic's entity keys are the
	// producer's canonical renderings, so only the reference side needs
	// coercion. A stage join INHERITS the joined stage's KeyType
	// (BuildGraph resolves it) and rejects a declaration here.
	OnType []string `mapstructure:"onType" json:"onType,omitempty"`
	// Fan makes a named join MULTIPLY rows, as SQL's join does over a
	// non-unique column: the stage's fold unit becomes the PAIR (input,
	// matched row), one per row matching on and where, each its own
	// element with identity (input key, matched producer key) — a forEach
	// element's identity. keyPath, when, and emits see the input as $. and
	// the matched row as $.<as>. (fold time included: the matched row is
	// part of the fold unit). With optional, an input with no match is one
	// element with a null alias (LEFT JOIN). Requires as and field (a join
	// by key matches at most one row); one fan join per stage, and none
	// beside forEach/fan arms. A topic join may declare field when it
	// fans: every entity carrying the value matches.
	Fan bool `mapstructure:"fan" json:"fan,omitempty"`
	// OrderBy and TieBy choose WHICH matching row a join resolves to: the
	// greatest by orderBy, then tieBy (both window-style order keys —
	// expressions over the JOINED row, each optionally asc|desc and nulls
	// first|last), with the producer key as a hidden floor. SQL's LATERAL
	// (… ORDER BY … DESC LIMIT 1). Without orderBy a field join resolves
	// to the bytewise-largest producer key's row, as before. Requires
	// field (a join by key matches at most one row) and tieBy; a topic
	// join may declare field when it orders.
	OrderBy []string `mapstructure:"orderBy" json:"orderBy,omitempty"`
	TieBy   []string `mapstructure:"tieBy" json:"tieBy,omitempty"`
	// AsOf bounds an ordered join in business time — SQL's ASOF JOIN: only
	// rows whose (single, ascending) orderBy value is <= the input's asOf
	// value are eligible, so an order prices at the rate in effect at its
	// own time, and a backfilled or corrected version re-resolves exactly
	// the inputs in [its time, the next version). An expression over the
	// INPUT (fold-time scope: no aliases). A null asOf, or a null orderBy
	// value, is never eligible; comparing different families (a time
	// against a number) is a data error.
	AsOf string `mapstructure:"asOf" json:"asOf,omitempty"`

	// orderKeys/tieKeys/asOfExpr are the compiled OrderBy/TieBy/AsOf
	// (validateOrderedJoin).
	orderKeys, tieKeys []winKey
	asOfExpr           Node
}

// ordered reports whether a join resolves through its ordered index.
func (j *Join) ordered() bool { return len(j.OrderBy) > 0 }

// validateOrderedJoin holds an ordered join's rules (OrderBy/TieBy/AsOf)
// and compiles them in place.
func validateOrderedJoin(j *Join, jw string) error {
	switch {
	case len(j.OrderBy) == 0 && len(j.TieBy) > 0:
		return fmt.Errorf("%s: tieBy breaks orderBy's ties — declare orderBy", jw)
	case len(j.OrderBy) == 0 && j.AsOf != "":
		return fmt.Errorf("%s: asOf bounds the join's orderBy — declare orderBy (the version's business time) and tieBy", jw)
	case len(j.OrderBy) == 0:
		return nil
	case j.Field == "":
		return fmt.Errorf("%s: orderBy chooses among the rows matching a field — a join by key matches at most one row; declare field", jw)
	case len(j.TieBy) == 0:
		return fmt.Errorf("%s: an ordered join needs tieBy — two versions with equal orderBy values would otherwise resolve by key bytes; a version id is the usual choice", jw)
	case j.Fan:
		return fmt.Errorf("%s: fan keeps every match, so orderBy has nothing to choose — drop one", jw)
	}
	var err error
	if j.orderKeys, err = compileWinKeys(j.OrderBy, jw+": orderBy"); err != nil {
		return err
	}
	if j.tieKeys, err = compileWinKeys(j.TieBy, jw+": tieBy"); err != nil {
		return err
	}
	for _, k := range append(append([]winKey(nil), j.orderKeys...), j.tieKeys...) {
		if UsesSource(k.expr) || exprReferencesParent(k.expr) {
			return fmt.Errorf("%s: orderBy/tieBy read the joined row — source() and $parent have no meaning there", jw)
		}
	}
	if j.AsOf == "" {
		j.asOfExpr = nil
		return nil
	}
	switch {
	case len(j.orderKeys) != 1:
		return fmt.Errorf("%s: asOf compares against ONE orderBy key (the version's time) — got %d", jw, len(j.orderKeys))
	case j.orderKeys[0].desc:
		return fmt.Errorf("%s: asOf finds the latest version at or before the input's time, so its orderBy key ascends — drop desc (a forward as-of is not supported)", jw)
	case j.orderKeys[0].nullsFirst:
		return fmt.Errorf("%s: a null orderBy value is never eligible under asOf — drop nulls first", jw)
	}
	if j.asOfExpr, err = Compile(j.AsOf); err != nil {
		return fmt.Errorf("%s: asOf: %w", jw, err)
	}
	if UsesSource(j.asOfExpr) || exprReferencesParent(j.asOfExpr) {
		return fmt.Errorf("%s: asOf reads the input — source() and $parent are not supported there", jw)
	}
	if a, b := castFamily(j.asOfExpr), castFamily(j.orderKeys[0].expr); a != "" && b != "" && a != b {
		return fmt.Errorf("%s: asOf is a %s but orderBy is a %s — the bound compares within one family; cast both alike", jw, a, b)
	}
	return nil
}

// castFamily names the comparison family a cast declares ("" when the
// expression's type is not declared by a cast).
func castFamily(n Node) string {
	c, ok := n.(exprCast)
	if !ok {
		return ""
	}
	switch c.typ {
	case "numeric", "integer", "bigint":
		return "number"
	case "timestamptz", "timestamp", "date":
		return "date/time"
	case "text":
		return "text"
	case "interval":
		return "interval"
	case "boolean":
		return "boolean"
	}
	return ""
}

// fanJoin returns the index of the stage's fan join, or -1.
func (st *Stage) fanJoin() int {
	for i := range st.Joins {
		if st.Joins[i].Fan {
			return i
		}
	}
	return -1
}

// validateFanJoin holds a fan join's own rules (see Join.Fan).
func validateFanJoin(st *Stage, j *Join, jw string) error {
	switch {
	case j.As == "":
		return fmt.Errorf("%s: fan multiplies rows by the matched rows — name them with as", jw)
	case j.Absent:
		return fmt.Errorf("%s: fan with absent: an anti-join has no row to fan", jw)
	case j.Field == "":
		return fmt.Errorf("%s: fan needs field — a join by key matches at most one row (topic entity keys and stage keys are unique); fan joins on a non-unique field", jw)
	case st.hasFan():
		return fmt.Errorf("%s: fan beside forEach/fan arms — one fan per fold unit; fan the payload in a prior stage", jw)
	case st.Reduce == "window":
		return fmt.Errorf("%s: fan on a window stage — fan in a prior stage keyed by the pair, and window its outputs", jw)
	case st.Reduce == "liveSet":
		return fmt.Errorf("%s: fan on a liveSet: delete evidence would be per pair, and a delete-shaped input with no match would be silently lost — fan in a prior stage and liveSet its outputs", jw)
	}
	return nil
}

// target returns the join's dimension source name (topic or stage).
func (j *Join) target() string {
	if j.From != "" {
		return j.From
	}
	return j.Topic
}

// Emit is one field of a stage's emitted object. A reshape stage's
// fields carry exactly one of From (a jsonpath into the input) or Expr
// (the closed expression language); an aggregate stage's fields carry
// exactly one fold arm — Sum/Min/Max (an expression folded over the key's
// retained inputs; Sum is numeric, Min/Max order ANY scalar — text
// lexically, SQL's MIN/MAX over date strings), Count (rows), or Collect
// (the values as a sorted array). Array-of-tables so field names
// survive byte-exact.
type Emit struct {
	Field string `mapstructure:"field"`
	From  string `mapstructure:"from"`
	Expr  string `mapstructure:"expr"`
	Sum   string `mapstructure:"sum"`
	Min   string `mapstructure:"min"`
	Max   string `mapstructure:"max"`
	Count bool   `mapstructure:"count"`
	// Collect folds an expression's values into an ARRAY — SQL's
	// array_agg, with the determinism SQL doesn't promise: values
	// always sort (numbers numerically, then strings lexically, then
	// bools), so equal folds are byte-equal. Nulls skip like every
	// fold arm; a member key whose values all skip emits an empty
	// array (membership belongs to the key, not the field).
	Collect string `mapstructure:"collect" json:"collect,omitempty"`
	// Distinct dedupes Collect's values (array_agg(DISTINCT ...)).
	Distinct bool `mapstructure:"distinct" json:"distinct,omitempty"`
	// Where filters THIS fold arm's inputs — SQL's FILTER (WHERE ...):
	// the emit folds only inputs matching its clauses, while key
	// membership stays the stage's (a filtered-to-empty field renders
	// zero/empty, the row remains). Fold arms only; a reshape emit's
	// condition is the stage's when.
	Where []WhenClause `mapstructure:"where" json:"where,omitempty"`

	// compiled holds the admission-checked AST of whichever expression arm
	// is set (Expr/Sum/Min/Max/Collect), populated by validateProjectionConfig.
	compiled Node
}

// ValidateShapes holds the stage shape rules — the sql gate calls it,
// and the engine tests exercise it directly so nothing regresses
// behind wiring.
func ValidateShapes(stages []Stage) error {
	names := make(map[string]bool, len(stages))
	emitSets := map[string]map[string]bool{}
	for i := range stages {
		st := &stages[i]
		where := fmt.Sprintf("stage %d (%q)", i+1, st.Name)
		if st.Name == "" {
			return fmt.Errorf("stage %d: name is required (stages chain by name)", i+1)
		}
		if names[st.Name] {
			return fmt.Errorf("%s: a second stage named %q", where, st.Name)
		}
		if strings.ContainsAny(st.Name, "[]") {
			return fmt.Errorf("%s: brackets in stage names are reserved for the engine's synthesized re-key stages (\"<stage>[<field>]\")", where)
		}
		if st.From == "" && len(st.FromList) == 0 && len(st.Merge) == 0 {
			return fmt.Errorf("%s: from is required — a topic id, or a prior stage's name (or a list of them, or merge, to combine prior stages by key)", where)
		}
		if st.From != "" && len(st.FromList) > 0 {
			return fmt.Errorf("%s: from is one source or a list of sources, not both", where)
		}
		if len(st.Sources()) > 0 && len(st.Merge) > 0 {
			return fmt.Errorf("%s: exactly one of from or merge (a stage consumes one input stream, or combines prior stages by key)", where)
		}
		if len(st.FromList) == 1 {
			return fmt.Errorf("%s: a from list takes two or more sources — one source is from = %q", where, st.FromList[0])
		}
		seenSrc := make(map[string]bool, len(st.FromList))
		for _, src := range st.Sources() {
			if src == "" {
				return fmt.Errorf("%s: from lists an empty source name", where)
			}
			if seenSrc[src] {
				return fmt.Errorf("%s: from lists %q twice — a source's rows are already all in the union; listing it again would count each row twice", where, src)
			}
			seenSrc[src] = true
			if src == st.Name || (!names[src] && IndexOf(stages, src) >= 0) {
				return fmt.Errorf("%s: from %q references a stage at or after this one — stages chain in manifest order (move the producer above its consumer)", where, src)
			}
		}
		names[st.Name] = true
		if len(st.Merge) > 0 {
			if err := validateMergeStage(stages, st, names, where); err != nil {
				return err
			}
		}
		if len(st.KeyPath) == 0 && len(st.Merge) == 0 && st.Reduce != "window" {
			return fmt.Errorf("%s: keyPath is required (one path per key part; several = a composite key)", where)
		}
		if st.Reduce != "window" && len(st.PartitionBy) > 0 {
			return fmt.Errorf("%s: partitionBy declares a window — it is only for reduce = \"window\"", where)
		}
		if st.Reduce != "window" && st.Reduce != "latest" && (len(st.WindowOrderBy) > 0 || len(st.WindowTieBy) > 0) {
			return fmt.Errorf("%s: orderBy/tieBy order a window's rows or choose latest's winner — they are only for reduce = \"window\" or \"latest\"", where)
		}
		for _, kp := range st.KeyPath {
			if err := validateKeyPart(kp, where+": keyPath"); err != nil {
				return err
			}
		}
		if st.ForEach != "" && len(st.Fan) > 0 {
			return fmt.Errorf("%s: exactly one of forEach or fan (fan IS the multi-path forEach)", where)
		}
		if len(st.Fan) == 1 {
			return fmt.Errorf("%s: fan takes two or more arms — a single path is just forEach", where)
		}
		for ai := range st.Fan {
			arm := &st.Fan[ai]
			aw := fmt.Sprintf("%s fan arm %d", where, ai+1)
			if arm.ForEach == "" {
				return fmt.Errorf("%s: forEach is required (the arm's element path)", aw)
			}
			if !MultiValuedPath(arm.ForEach) {
				return fmt.Errorf("%s: forEach [%s] is single-valued — a fan arm fans an array into element-inputs", aw, arm.ForEach)
			}
			if err := ValidateWhen(arm.When, aw+" when"); err != nil {
				return err
			}
			// The arm's when selects INPUTS — there is no enclosing scope.
			if err := RejectParentPaths(arm.When, aw+" when"); err != nil {
				return err
			}
		}
		if st.hasFan() && len(st.KeyPath) > 1 && st.ElementKey == "" {
			return fmt.Errorf("%s: a forEach stage with a composite keyPath needs elementKey (the element's own identity)", where)
		}
		if err := ValidateWhen(st.When, where); err != nil {
			return err
		}
		if !st.hasFan() {
			if err := RejectParentPaths(st.When, where+" when"); err != nil {
				return err
			}
			if err := RejectParentPaths(st.DeleteWhen, where+" deleteWhen"); err != nil {
				return err
			}
		}
		if st.Reduce == "liveSet" && st.hasFan() {
			return fmt.Errorf("%s: forEach with reduce = \"liveSet\": a fanned liveSet's delete evidence is per-element, and a delete-shaped event carrying no elements would be silently lost — fan in a prior stage and liveSet its outputs downstream", where)
		}
		if len(st.DeleteWhen) > 0 {
			if st.Reduce != "liveSet" {
				return fmt.Errorf("%s: deleteWhen is only for reduce = \"liveSet\"", where)
			}
			if err := ValidateWhen(st.DeleteWhen, where+" deleteWhen"); err != nil {
				return err
			}
		}
		switch st.Reduce {
		case "window":
			if err := validateWindowStage(st, where); err != nil {
				return err
			}
		case "", "aggregate":
			if st.OrderBy != "" || st.TieBy != "" {
				return fmt.Errorf("%s: orderBy/tieBy are only for reduce = \"latest\"", where)
			}
		case "liveSet":
			if len(st.DeleteWhen) == 0 {
				return fmt.Errorf("%s: reduce = \"liveSet\" needs deleteWhen — the clauses that mark an event delete-shaped", where)
			}
			if st.OrderBy != "" || st.TieBy != "" {
				return fmt.Errorf("%s: orderBy/tieBy are only for reduce = \"latest\"", where)
			}
		case "latest":
			if len(st.WindowOrderBy) > 0 || len(st.WindowTieBy) > 0 {
				if err := validateLatestOrderList(st, where); err != nil {
					return err
				}
				break
			}
			if st.OrderBy == "" {
				return fmt.Errorf("%s: reduce = \"latest\" needs orderBy — a BUSINESS field; arrival order diverges under backfill", where)
			}
			if st.TieBy == "" {
				return fmt.Errorf("%s: reduce = \"latest\" needs tieBy — ties are real in the field (37 of 276,286 measured) and an unbroken tie folds nondeterministically; an id column is the usual choice", where)
			}
			for _, p := range []string{st.OrderBy, st.TieBy} {
				if err := RejectMultiValued(p, where); err != nil {
					return err
				}
			}
			for _, ot := range []string{st.OrderByType, st.TieByType} {
				switch ot {
				case "", KeyTypeText, KeyTypeNumber:
				default:
					return fmt.Errorf("%s: orderByType/tieByType %q is invalid (want %q or %q)", where, ot, KeyTypeText, KeyTypeNumber)
				}
			}
		default:
			return fmt.Errorf("%s: reduce %q is invalid (want \"latest\", \"aggregate\", \"liveSet\", \"window\", or omit for a reshape stage)", where, st.Reduce)
		}
		if st.ForEach != "" && !MultiValuedPath(st.ForEach) {
			return fmt.Errorf("%s: forEach path [%s] is single-valued — forEach fans an array into element-inputs; drop it for one input per entity", where, st.ForEach)
		}
		if st.ElementKey != "" {
			if !st.hasFan() {
				return fmt.Errorf("%s: elementKey is only for forEach stages (it names a fanned element's identity)", where)
			}
			if err := RejectMultiValued(st.ElementKey, where+": elementKey"); err != nil {
				return err
			}
		}
		if st.ForEach != "" && st.ElementKey == "" && st.Reduce != "" {
			return fmt.Errorf("%s: a forEach stage with reduce = %q needs elementKey — the element's own identity — or two same-key elements would collapse into one retained input", where, st.Reduce)
		}
		if !ValidNormalize(st.Normalize) {
			return fmt.Errorf("%s: normalize %q is not supported (want %q)", where, st.Normalize, NormalizeLower)
		}
		if err := validateKeyTypes(st.KeyType, len(st.KeyPath), where+" keyType"); err != nil {
			return err
		}
		joinAliases := make(map[string]bool, len(st.Joins))
		// foldTimeAliases are the aliases fold-time positions may not
		// address: every named join's but a fan join's, whose matched row
		// is part of the fold unit itself.
		foldTimeAliases := map[string]bool{}
		fieldJoinSpaces := map[string]string{}
		fans := 0
		for ji, j := range st.Joins {
			jw := fmt.Sprintf("%s join %d (%q)", where, ji+1, j.target())
			if err := validateOrderedJoin(&st.Joins[ji], jw); err != nil {
				return err
			}
			if j.Fan {
				if err := validateFanJoin(st, &j, jw); err != nil {
					return err
				}
				if fans++; fans > 1 {
					return fmt.Errorf("%s: a second fan join — two would multiply into a cross product of matches; fan in one stage and join the next downstream", jw)
				}
			}
			if !ValidNormalize(j.Normalize) {
				return fmt.Errorf("%s: normalize %q is not supported (want %q)", jw, j.Normalize, NormalizeLower)
			}
			if (j.Topic == "") == (j.From == "") {
				return fmt.Errorf("%s: exactly one of topic (a topic's rows by entity key) or from (a PRIOR stage's outputs by its key) is required", jw)
			}
			if j.Topic != "" && IndexOf(stages, j.Topic) >= 0 {
				return fmt.Errorf("%s: %q is a stage — join it with from = %q (topic joins address topics)", jw, j.Topic, j.Topic)
			}
			if j.Field != "" {
				if j.From == "" && !j.Fan && !j.ordered() {
					return fmt.Errorf("%s: field addresses a STAGE's emitted field — a topic join addresses the entity key; drop field, or fold the topic through a stage first", jw)
				}
				if len(j.On) != 1 {
					return fmt.Errorf("%s: with field, on takes exactly one path (the reference to that one emitted field)", jw)
				}
				if err := RejectMultiValued(j.Field, jw+": field"); err != nil {
					return err
				}
				space := j.Normalize + "\x00" + strings.Join(j.OnType, ",")
				if prior, ok := fieldJoinSpaces[j.From+"\x00"+j.Field]; ok && prior != space && !j.Fan && !j.ordered() {
					return fmt.Errorf("%s: two field joins address %s[%s] with different normalize/onType — the synthesized re-key stage has ONE key space; declare them identically", jw, j.From, j.Field)
				}
				if !j.Fan && !j.ordered() {
					fieldJoinSpaces[j.From+"\x00"+j.Field] = space
				}
			}
			if j.From != "" {
				fi := IndexOf(stages, j.From)
				if fi < 0 {
					return fmt.Errorf("%s: from %q names no declared stage", jw, j.From)
				}
				if fi >= i {
					return fmt.Errorf("%s: from %q references a stage at or after this one — stages join in manifest order (move the producer above its consumer)", jw, j.From)
				}
				if j.Field == "" && j.Normalize != "" {
					return fmt.Errorf("%s: normalize is not declarable on a stage join — references render in the joined stage's key space, so the join inherits stage %q's normalize automatically; declare it on that stage", jw, j.From)
				}
			}
			if len(j.On) == 0 {
				return fmt.Errorf("%s: on is required — the input field(s) holding the joined entity's key", jw)
			}
			for _, onPath := range j.On {
				if err := RejectMultiValued(onPath, jw+": on"); err != nil {
					return err
				}
			}
			if j.From != "" && j.Field == "" {
				if fi := IndexOf(stages, j.From); fi >= 0 {
					if arity, _, _ := ResolvedKeySpace(stages, fi); arity != KeyArityProducer && len(j.On) != arity {
						return fmt.Errorf("%s: on has %d path(s) but stage %q keys by %d path(s) — a stage join's on addresses the joined stage's key, so the arities must match, in the same order", jw, len(j.On), j.From, arity)
					}
				}
			}
			if err := ValidateWhen(j.Where, jw); err != nil {
				return err
			}
			if err := RejectParentPaths(j.Where, jw+" where"); err != nil {
				return err
			}
			if err := RejectSourceInWhens(j.Where, jw+" where", "a join's where filters the joined row, which has no source; test the input's source in the stage's when"); err != nil {
				return err
			}
			if j.As != "" {
				if j.Absent {
					return fmt.Errorf("%s: as names the joined row — an absent (anti-)join has no row to name", jw)
				}
				if !pathSafeIdent(j.As) {
					return fmt.Errorf("%s: as %q is not addressable by a jsonpath dot segment (letters, digits, underscore)", jw, j.As)
				}
				if j.As == "parent" {
					return fmt.Errorf("%s: as \"parent\" confusingly shadows the $parent scope — pick another alias", jw)
				}
				if joinAliases[j.As] {
					return fmt.Errorf("%s: a second join aliased %q — aliases are unique per stage", jw, j.As)
				}
				joinAliases[j.As] = true
				if !j.Fan {
					foldTimeAliases[j.As] = true
				}
			}
			if j.Optional && j.As == "" {
				return fmt.Errorf("%s: optional is only meaningful with as — an unnamed join only filters, and an optional filter is no filter", jw)
			}
			if j.From != "" && j.Field == "" && len(j.OnType) > 0 {
				return fmt.Errorf("%s: onType is not declarable on a stage join — references render in the joined stage's key space, so the join inherits stage %q's keyType automatically; declare it on that stage", jw, j.From)
			}
			if err := validateKeyTypes(j.OnType, len(j.On), jw+" onType"); err != nil {
				return err
			}
		}
		if len(foldTimeAliases) > 0 {
			if err := rejectFoldTimeAliasRefs(st, foldTimeAliases, where); err != nil {
				return err
			}
		}
		if err := rejectUnemittedFieldPaths(stages, st, emitSets, where); err != nil {
			return err
		}
		fields := make(map[string]bool, len(st.Emit))
		for k := range st.Emit {
			fields[st.Emit[k].Field] = true
		}
		emitSets[st.Name] = fields
		if len(st.Emit) == 0 {
			return fmt.Errorf("%s: emit needs at least one field", where)
		}
		emitted := make(map[string]bool, len(st.Emit))
		st.aggSlots, st.havingCompiled, st.winSlots = nil, nil, nil
		for k := range st.Emit {
			e := &st.Emit[k]
			ew := fmt.Sprintf("%s emit %q", where, e.Field)
			if e.Field == "" {
				return fmt.Errorf("%s: emit entry with empty field", where)
			}
			if emitted[e.Field] {
				return fmt.Errorf("%s: declared twice", ew)
			}
			emitted[e.Field] = true
			folds := 0
			for _, set := range []bool{e.Sum != "", e.Min != "", e.Max != "", e.Count, e.Collect != ""} {
				if set {
					folds++
				}
			}
			plain := 0
			for _, set := range []bool{e.From != "", e.Expr != ""} {
				if set {
					plain++
				}
			}
			if st.Reduce == "aggregate" { //nolint:gocritic // if-else reads clearer than switch here
				if folds+plain != 1 || e.From != "" {
					return fmt.Errorf("%s: an aggregate stage's emit carries exactly one of sum, min, max, count, collect, or an expr over aggregate calls (round(sum($.a) / count(*), 2))", ew)
				}
				if e.Expr != "" && len(e.Where) > 0 {
					return fmt.Errorf("%s: where filters a single fold arm — inside an expr, filter each aggregate call: sum($.x) filter (where …)", ew)
				}
			} else {
				if plain != 1 || folds != 0 {
					kind := "reshape"
					if st.Reduce != "" {
						kind = st.Reduce
					}
					return fmt.Errorf("%s: a %s stage's emit carries exactly one of from or expr", ew, kind)
				}
			}
			if e.Distinct && e.Collect == "" {
				return fmt.Errorf("%s: distinct applies to collect (the other fold arms are already per-value)", ew)
			}
			if len(e.Where) > 0 {
				if st.Reduce != "aggregate" {
					return fmt.Errorf("%s: where filters a fold arm's inputs (SQL's FILTER) — on a reshape emit, the condition is the stage's when", ew)
				}
				if err := ValidateWhen(e.Where, ew+" where"); err != nil {
					return err
				}
				if !st.hasFan() {
					if err := RejectParentPaths(e.Where, ew+" where"); err != nil {
						return err
					}
				}
			}
			if e.From != "" {
				if err := RejectMultiValued(e.From, ew); err != nil {
					return err
				}
			}
			if st.Reduce == "aggregate" && e.Expr != "" {
				compiled, err := compileAggregate(e.Expr, &st.aggSlots)
				if err != nil {
					return fmt.Errorf("%s: %w", ew, err)
				}
				e.compiled = compiled
				continue
			}
			if st.Reduce == "window" && e.Expr != "" {
				compiled, err := compileWindowExpr(e.Expr, &st.winSlots)
				if err != nil {
					return fmt.Errorf("%s: %w", ew, err)
				}
				e.compiled = compiled
				continue
			}
			for _, src := range []string{e.Expr, e.Sum, e.Min, e.Max, e.Collect} {
				if src == "" {
					continue
				}
				compiled, err := Compile(src)
				if err != nil {
					return fmt.Errorf("%s: %w", ew, err)
				}
				e.compiled = compiled
			}
		}
		if st.Reduce == "window" {
			if err := validateWindowFrames(st, where); err != nil {
				return err
			}
		}
		if st.Reduce == "window" && len(st.WindowTieBy) == 0 {
			for _, w := range st.winSlots {
				if tieSensitiveFns[w.fn] {
					return fmt.Errorf("%s: %s(…) depends on the order WITHIN ties (rows with equal orderBy values) — declare tieBy (an id or sequence column is the usual choice); rank, dense_rank, and the running aggregates treat ties as peers and need none", where, w.fn)
				}
			}
		}
		if st.Having != "" {
			if st.Reduce != "aggregate" {
				return fmt.Errorf("%s: having filters an aggregate stage's folded keys (SQL's HAVING) — on other stages the condition is the stage's when", where)
			}
			compiled, err := compileAggregate(st.Having, &st.aggSlots)
			if err != nil {
				return fmt.Errorf("%s having: %w", where, err)
			}
			st.havingCompiled = compiled
		}
		if len(st.Merge) > 0 && stageUsesSource(st) {
			return fmt.Errorf("%s: source() names an input's from source — a merge has no from (its sides are aliased: test $.<alias> is not null)", where)
		}
	}
	return nil
}

// validateLatestOrderList checks a latest stage's list form — orderBy and
// tieBy as window-style order keys (expressions, each optionally
// asc|desc and nulls first|last) — and compiles them in place.
func validateLatestOrderList(st *Stage, where string) error {
	switch {
	case st.OrderBy != "" || st.TieBy != "":
		return fmt.Errorf("%s: orderBy and tieBy are both single paths or both order-key lists", where)
	case st.OrderByType != "" || st.TieByType != "":
		return fmt.Errorf("%s: orderByType/tieByType are for single-path orderBy/tieBy; order keys compare in the value order — cast instead ($.ts::numeric, $.at::timestamptz)", where)
	case len(st.WindowOrderBy) == 0:
		return fmt.Errorf("%s: reduce = \"latest\" needs orderBy — a BUSINESS field; arrival order diverges under backfill", where)
	case len(st.WindowTieBy) == 0:
		return fmt.Errorf("%s: reduce = \"latest\" needs tieBy — ties are real in the field (37 of 276,286 measured) and an unbroken tie folds nondeterministically; an id column is the usual choice", where)
	}
	var err error
	if st.winOrder, err = compileWinKeys(st.WindowOrderBy, where+": orderBy"); err != nil {
		return err
	}
	st.winTie, err = compileWinKeys(st.WindowTieBy, where+": tieBy")
	return err
}

// validateMergeStage checks the merge form: at least two prior stages,
// unique path-safe aliases, unanimous key spaces, and none of the
// fields a merge cannot carry — its key space is inherited (never
// declared) and its fold unit is the tuple (reduce/fan/join arms are
// meaningless and rejected rather than ignored).
func validateMergeStage(stages []Stage, st *Stage, names map[string]bool, where string) error {
	switch {
	case len(st.KeyPath) > 0:
		return fmt.Errorf("%s: keyPath is not declarable on a merge — its key space is inherited from the merged stages", where)
	case len(st.KeyType) > 0:
		return fmt.Errorf("%s: keyType is not declarable on a merge — its key space is inherited from the merged stages", where)
	case st.Normalize != "":
		return fmt.Errorf("%s: normalize is not declarable on a merge — its key space is inherited from the merged stages", where)
	case st.Reduce != "":
		return fmt.Errorf("%s: reduce is not applicable to a merge (its retained set is the per-upstream tuple by construction); fold before or after it", where)
	case st.ForEach != "" || st.ElementKey != "":
		return fmt.Errorf("%s: forEach is not applicable to a merge; fan in a prior stage", where)
	case len(st.DeleteWhen) > 0:
		return fmt.Errorf("%s: deleteWhen is only for reduce = %q", where, "liveSet")
	case st.OrderBy != "" || st.TieBy != "":
		return fmt.Errorf("%s: orderBy/tieBy are only for reduce = %q", where, "latest")
	case len(st.Joins) > 0:
		return fmt.Errorf("%s: joins are not applicable to a merge stage; join in a prior stage, or merge the join's producer (same-key correlation belongs to merge, foreign-key reference to joins)", where)
	case len(st.Merge) < 2:
		return fmt.Errorf("%s: merge combines at least two prior stages", where)
	}
	seenStage := map[string]bool{}
	seenAlias := map[string]bool{}
	for ei, e := range st.Merge {
		ew := fmt.Sprintf("%s merge entry %d", where, ei+1)
		if (e.Stage == "") == (e.Topic == "") {
			return fmt.Errorf("%s: exactly one of stage or topic is required", ew)
		}
		if e.Topic != "" {
			if len(e.KeyPath) == 0 {
				return fmt.Errorf("%s: a topic merge side declares its own keyPath (a topic has no stage key space to inherit)", ew)
			}
			if !ValidNormalize(e.Normalize) {
				return fmt.Errorf("%s: normalize %q is not supported (want %q)", ew, e.Normalize, NormalizeLower)
			}
			if err := validateKeyTypes(e.KeyType, len(e.KeyPath), ew+" keyType"); err != nil {
				return err
			}
			for _, kp := range e.KeyPath {
				if err := validateKeyPart(kp, ew+": keyPath"); err != nil {
					return err
				}
			}
		} else {
			if len(e.KeyPath) > 0 || len(e.KeyType) > 0 || e.Normalize != "" {
				return fmt.Errorf("%s: keyPath/keyType/normalize are only for TOPIC sides — a stage side's key space is the stage's own", ew)
			}
			if !names[e.Stage] {
				if IndexOf(stages, e.Stage) >= 0 {
					return fmt.Errorf("%s: %q references a stage at or after this one — stages merge in manifest order (move the producer above its consumer)", ew, e.Stage)
				}
				return fmt.Errorf("%s: %q names no declared stage (merge combines STAGES; or declare a topic side: { topic, keyPath, as })", ew, e.Stage)
			}
		}
		side := e.Stage
		if side == "" {
			side = e.Topic + "[" + strings.Join(e.KeyPath, ",") + "]"
		}
		if seenStage[side] {
			return fmt.Errorf("%s: side %q merged twice", ew, side)
		}
		seenStage[side] = true
		alias := e.As
		if alias == "" {
			alias = e.Stage
			if alias == "" {
				alias = e.Topic
			}
		}
		if !pathSafeIdent(alias) {
			return fmt.Errorf("%s: alias %q is not addressable by a jsonpath dot segment — add as = %q (letters, digits, underscore)", ew, alias, "<identifier>")
		}
		if seenAlias[alias] {
			return fmt.Errorf("%s: a second merged side aliased %q", ew, alias)
		}
		seenAlias[alias] = true
	}
	// Unanimous key spaces: merged sides pair by RENDERED BYTES, so a
	// rendering disagreement (arity, keyType, normalize) would pair
	// nothing — silently, since an unpaired side is legal full-outer
	// absence. Loud here instead.
	baseArity, baseKT, baseNorm := mergeSideSpace(stages, &st.Merge[0])
	baseName := mergeSideName(&st.Merge[0])
	for ei := range st.Merge[1:] {
		e := &st.Merge[ei+1]
		arity, kt, norm := mergeSideSpace(stages, e)
		if arity != baseArity && arity != KeyArityProducer && baseArity != KeyArityProducer {
			return fmt.Errorf("%s: merged sides disagree on key arity (%q keys by %d part(s), %q by %d) — merged sides pair by key, so their key spaces must agree", where, baseName, baseArity, mergeSideName(e), arity)
		}
		if !equalKeyTypes(baseKT, kt, baseArity) {
			return fmt.Errorf("%s: merged sides disagree on keyType (%q vs %q) — a rendering disagreement pairs NOTHING, silently; declare matching keyType on both", where, baseName, mergeSideName(e))
		}
		if norm != baseNorm {
			return fmt.Errorf("%s: merged sides disagree on normalize (%q declares %q, %q declares %q) — a rendering disagreement pairs NOTHING, silently; declare matching normalize on both", where, baseName, baseNorm, mergeSideName(e), norm)
		}
	}
	return nil
}

// mergeSideSpace resolves one merge side's key space: a stage side
// through ResolvedKeySpace, a topic side from its declaration.
func mergeSideSpace(stages []Stage, e *MergeEntry) (int, []string, string) {
	if e.Topic != "" {
		return len(e.KeyPath), e.KeyType, e.Normalize
	}
	return ResolvedKeySpace(stages, IndexOf(stages, e.Stage))
}

func mergeSideName(e *MergeEntry) string {
	if e.Topic != "" {
		return e.Topic
	}
	return e.Stage
}

// rejectUnemittedFieldPaths is the declared-shape guard: the language
// has TWO object kinds — dynamic-shape topic payloads, where
// missing-field-is-null is correct SQL semantics, and DECLARED-shape
// stage emits, where consistency is provable at admission. A consumer
// path into a stage-produced object whose first field segment the
// producer never emits can never resolve; it must reject loudly here,
// not scope null silently (the field bug: $.s10w.cdate against a
// producer emitting only {job} — null on 5,983 rows, found by replay
// diff instead of this message). Only the FIRST segment is checkable
// (emitted values may be copied subtrees); bare alias paths (presence
// tests) claim no field; topic sides/joins and topic lifts stay
// dynamic; $parent is scope, not shape.
func rejectUnemittedFieldPaths(stages []Stage, st *Stage, emitSets map[string]map[string]bool, where string) error {
	// aliasSets: refold-scope aliases → the producing stage's emit set
	// (nil = dynamic/unknown, unchecked). Field-addressed joins resolve
	// through their synthetic to the SOURCE stage's set.
	type aliasSrc struct {
		set      map[string]bool
		producer string
	}
	aliasSets := map[string]aliasSrc{}
	for ei := range st.Merge {
		e := &st.Merge[ei]
		alias := e.As
		if alias == "" {
			alias = e.Stage
			if alias == "" {
				alias = e.Topic
			}
		}
		if e.Stage != "" {
			aliasSets[alias] = aliasSrc{emitSets[e.Stage], e.Stage}
		} else {
			aliasSets[alias] = aliasSrc{nil, ""} // topic side: dynamic payload
		}
	}
	for ji := range st.Joins {
		j := &st.Joins[ji]
		if j.As == "" {
			continue
		}
		if j.From != "" {
			aliasSets[j.As] = aliasSrc{emitSets[j.From], j.From}
		} else {
			aliasSets[j.As] = aliasSrc{nil, ""} // topic join: dynamic payload
		}
	}
	// inputSet: for a stage-fed, unfanned stage, input-scope paths
	// address the upstream's emit set. Fanned stages' input paths are
	// element-scope (dynamic); merges' input is the tuple (aliases).
	// A union's input is any of its sources' rows, so a path is
	// unemitted only when NO source can carry it — and any topic source
	// makes the shape dynamic.
	var inputSet map[string]bool
	inputFrom := ""
	if len(st.Sources()) > 0 && !st.hasFan() && len(st.Merge) == 0 {
		union := map[string]bool{}
		for _, src := range st.Sources() {
			s, ok := emitSets[src]
			if !ok {
				union = nil
				break
			}
			for f := range s {
				union[f] = true
			}
		}
		if union != nil {
			inputSet = union
			inputFrom = strings.Join(st.Sources(), `", "`)
		}
	}
	check := func(path, pos string) error {
		first, rest := pathFirstSegment(path)
		if first == "" {
			return nil
		}
		if src, isAlias := aliasSets[first]; isAlias {
			if src.set == nil || rest == "" {
				return nil // dynamic side, or a bare-alias presence test
			}
			field, _ := segmentHead(rest)
			if field != "" && !src.set[field] {
				return unemittedErr(where, pos, path, src.producer, field, src.set)
			}
			return nil
		}
		if inputSet != nil && !inputSet[first] {
			return unemittedErr(where, pos, path, inputFrom, first, inputSet)
		}
		return nil
	}
	checkWhens := func(cls []WhenClause, pos string) error {
		for k := range cls {
			c := &cls[k]
			if c.Expr != "" {
				n := c.compiled
				if n == nil {
					if n2, err := Compile(c.Expr); err == nil {
						n = n2
					}
				}
				var werr error
				walkExprPaths(n, func(p string) {
					if werr == nil {
						werr = check(p, pos)
					}
				})
				if werr != nil {
					return werr
				}
				continue
			}
			if err := check(c.Path, pos); err != nil {
				return err
			}
		}
		return nil
	}
	for _, kp := range st.KeyPath {
		var kerr error
		walkKeyPaths(kp, func(p string) {
			if kerr == nil {
				kerr = check(p, "keyPath")
			}
		})
		if kerr != nil {
			return kerr
		}
	}
	var werr error
	windowKeyPathRefs(st, func(p, pos string) {
		if werr == nil {
			werr = check(p, pos)
		}
	})
	if werr != nil {
		return werr
	}
	if err := checkWhens(st.When, "when"); err != nil {
		return err
	}
	if err := checkWhens(st.DeleteWhen, "deleteWhen"); err != nil {
		return err
	}
	for ji := range st.Joins {
		j := &st.Joins[ji]
		for _, on := range j.On {
			if err := check(on, "join on"); err != nil {
				return err
			}
		}
		// A join's where filters the DIMENSION ROW: stage dims have a
		// declared shape (their emit set), topic dims are dynamic.
		if j.From != "" && j.Field == "" {
			if dimSet, ok := emitSets[j.From]; ok && dimSet != nil {
				for k := range j.Where {
					first, _ := pathFirstSegment(j.Where[k].Path)
					if first != "" && !dimSet[first] {
						return unemittedErr(where, "join where", j.Where[k].Path, j.From, first, dimSet)
					}
				}
			}
		}
	}
	for k := range st.Emit {
		e := &st.Emit[k]
		pos := "emit " + e.Field
		if e.From != "" {
			if err := check(e.From, pos); err != nil {
				return err
			}
		}
		compiled := e.compiled
		if compiled == nil && e.Expr != "" {
			compiled = compileForPathCheck(st, e.Expr)
		}
		if compiled != nil {
			var werr error
			walkExprPaths(compiled, func(p string) {
				if werr == nil {
					werr = check(p, pos)
				}
			})
			if werr != nil {
				return werr
			}
		}
		if err := checkWhens(e.Where, pos+" where"); err != nil {
			return err
		}
	}
	if st.Having != "" {
		if compiled := compileForPathCheck(st, st.Having); compiled != nil {
			var werr error
			walkExprPaths(compiled, func(p string) {
				if werr == nil {
					werr = check(p, "having")
				}
			})
			if werr != nil {
				return werr
			}
		}
	}
	return nil
}

func unemittedErr(where, pos, path, producer, field string, set map[string]bool) error {
	names := make([]string, 0, len(set))
	for f := range set {
		names = append(names, f)
	}
	sort.Strings(names)
	who := fmt.Sprintf("stage %q never emits", producer)
	if strings.Contains(producer, `", "`) {
		who = fmt.Sprintf(`none of the union's stages "%s" emits`, producer)
	}
	return fmt.Errorf("%s: %s path [%s] reads field %q which %s — a stage's emitted fields {%s} are the whole surface a consumer can address (a missing field here is a config inconsistency, not a null)", where, pos, path, field, who, strings.Join(names, ", "))
}

// pathFirstSegment splits "$.a.b[0].c" into ("a", "b[0].c"); non-$.
// and $parent paths return "" (dynamic scope, unchecked).
func pathFirstSegment(path string) (string, string) {
	rest, ok := strings.CutPrefix(path, "$.")
	if !ok {
		return "", ""
	}
	return segmentHead(rest)
}

func segmentHead(s string) (string, string) {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return s[:i], s[i+1:]
		}
		if s[i] == '[' {
			return s[:i], s[i:]
		}
	}
	return s, ""
}

// rejectFoldTimeAliasRefs guards the grammar's newest seam: the
// join-lookup scope exists at REFOLD (emits, fold arms, per-emit
// where), so a FOLD-TIME position — when, deleteWhen, keyPath, forEach,
// any join's on — addressing a declared alias would resolve nothing and
// silently never match. Loud here instead.
func rejectFoldTimeAliasRefs(st *Stage, aliases map[string]bool, where string) error {
	checkPath := func(p, pos string) error {
		if a := aliasPrefix(p, aliases); a != "" {
			return fmt.Errorf("%s: %s [%s] addresses join alias %q — the lookup scope exists at REFOLD (emits, fold arms, per-emit where); at fold time this path resolves nothing and would silently never match. Filter the joined row with the join's where; to key or fan by a looked-up value, emit it here and use the next stage", where, pos, p, a)
		}
		return nil
	}
	checkWhens := func(cls []WhenClause, pos string) error {
		for _, c := range cls {
			if c.Expr != "" {
				n := c.compiled
				if n == nil {
					if n2, err := Compile(c.Expr); err == nil {
						n = n2
					}
				}
				var aerr error
				walkExprPaths(n, func(p string) {
					if aerr == nil {
						aerr = checkPath(p, pos)
					}
				})
				if aerr != nil {
					return aerr
				}
				continue
			}
			if err := checkPath(c.Path, pos); err != nil {
				return err
			}
		}
		return nil
	}
	if err := checkWhens(st.When, "when path"); err != nil {
		return err
	}
	if err := checkWhens(st.DeleteWhen, "deleteWhen path"); err != nil {
		return err
	}
	for _, kp := range st.KeyPath {
		var kerr error
		walkKeyPaths(kp, func(p string) {
			if kerr == nil {
				kerr = checkPath(p, "keyPath")
			}
		})
		if kerr != nil {
			return kerr
		}
	}
	var werr error
	windowKeyPathRefs(st, func(p, pos string) {
		if werr == nil {
			werr = checkPath(p, pos)
		}
	})
	if werr != nil {
		return werr
	}
	if st.ForEach != "" {
		if err := checkPath(st.ForEach, "forEach"); err != nil {
			return err
		}
	}
	for ai := range st.Fan {
		if err := checkPath(st.Fan[ai].ForEach, "fan forEach"); err != nil {
			return err
		}
		if err := checkWhens(st.Fan[ai].When, "fan when path"); err != nil {
			return err
		}
	}
	for ji := range st.Joins {
		for _, on := range st.Joins[ji].On {
			if err := checkPath(on, "join on"); err != nil {
				return err
			}
		}
		if n := st.Joins[ji].asOfExpr; n != nil {
			var aerr error
			walkExprPaths(n, func(p string) {
				if aerr == nil {
					aerr = checkPath(p, "join asOf")
				}
			})
			if aerr != nil {
				return aerr
			}
		}
	}
	return nil
}

// aliasPrefix reports which declared alias (if any) a jsonpath
// addresses: $.<alias> followed by '.', '[', or end of path.
func aliasPrefix(path string, aliases map[string]bool) string {
	rest, ok := strings.CutPrefix(path, "$.")
	if !ok {
		return ""
	}
	for i := 0; i < len(rest); i++ {
		if rest[i] == '.' || rest[i] == '[' {
			rest = rest[:i]
			break
		}
	}
	if aliases[rest] {
		return rest
	}
	return ""
}

// pathSafeIdent reports whether s is addressable as a jsonpath dot
// segment ([A-Za-z_][A-Za-z0-9_]*).
func pathSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// ResolvedKeySpace reports a stage's effective key space — arity,
// keyType, normalize — resolving THROUGH merges (a merge inherits its
// entries' unanimous space; recursion is well-founded by manifest
// order). Every consumer of a stage's keys (stage-fed table arity
// checks, join inheritance, merge agreement) resolves through here.
func ResolvedKeySpace(stages []Stage, i int) (arity int, keyType []string, normalize string) {
	st := &stages[i]
	if len(st.Merge) > 0 {
		return mergeSideSpace(stages, &st.Merge[0])
	}
	if st.Reduce == "window" {
		// A window stage's rows key by the INPUT's key: a prior stage's
		// key space, inherited — or a topic's entity keys, whose shape is
		// the producer's (KeyArityProducer).
		// A union's rows key by (source, input key): two parts, but the
		// input key's own shape is each source's, so arity is unchecked.
		if fi := IndexOf(stages, st.From); fi >= 0 && fi < i {
			return ResolvedKeySpace(stages, fi)
		}
		return KeyArityProducer, nil, ""
	}
	return len(st.KeyPath), st.KeyType, st.Normalize
}

// KeyArityProducer is the resolved arity of a key space the PRODUCER
// shapes — a window stage over a topic keys its rows by the topic's
// entity keys, single or composite as the producer writes them. Arity
// checks skip it: as with a topic join's on, the arity is the config
// author's contract (a composite entity key decodes by the consumer's
// declared column count).
const KeyArityProducer = -1

// ProbeKey renders a caller's key parts in stage i's RESOLVED key
// space — typed per position, normalized, composed through the
// composite encoding — so an introspection probe can only miss when the
// key is genuinely absent, never because the caller misreproduced the
// stored bytes. Resolution goes through ResolvedKeySpace, so probing a
// MERGE stage renders in its inherited arity/keyType/normalize rather
// than the merge's own empty declarations. With a keyType declared, the
// last caller obligation (canonical digits) disappears: "5.0000" probes
// as 5. Returns ok=false (no error) when a part cannot render into its
// declared comparison space — genuine non-membership; an arity mismatch
// errors loudly.
func ProbeKey(sts []Stage, i int, keyParts []string) (string, bool, error) {
	arity, kts, norm := ResolvedKeySpace(sts, i)
	if arity != KeyArityProducer && len(keyParts) != arity {
		return "", false, fmt.Errorf("stage %q keys by %d part(s) but the probe carries %d — pass one probeKey per keyPath position, in order", sts[i].Name, arity, len(keyParts))
	}
	parts := make([]string, len(keyParts))
	for j, kp := range keyParts {
		part, ok := TypedKeyPart(KeyTypeAt(kts, j), kp)
		if !ok {
			return "", false, nil
		}
		parts[j] = NormalizeKeyPart(norm, part)
	}
	return OutKey(parts), true, nil
}

// equalKeyTypes compares two keyType declarations position-by-position
// (broadcast and default resolve per KeyTypeAt).
func equalKeyTypes(a, b []string, arity int) bool {
	for i := 0; i < arity; i++ {
		if KeyTypeAt(a, i) != KeyTypeAt(b, i) {
			return false
		}
	}
	return true
}

// validateKeyTypes checks a keyType/onType declaration: values from the
// text/number space, arity one (broadcast) or exactly the key's arity.
func validateKeyTypes(kts []string, arity int, where string) error {
	for _, kt := range kts {
		switch kt {
		case KeyTypeText, KeyTypeNumber:
		default:
			return fmt.Errorf("%s: %q is invalid (want %q or %q)", where, kt, KeyTypeText, KeyTypeNumber)
		}
	}
	if len(kts) > 1 && len(kts) != arity {
		return fmt.Errorf("%s: %d type(s) for %d key part(s) — declare one per position (or a single type to apply to all)", where, len(kts), arity)
	}
	return nil
}

// KeyTypeAt resolves the declared comparison space for one key position:
// positional when a full list is declared, broadcast when a single value
// is, text when nothing is.
func KeyTypeAt(kts []string, i int) string {
	switch {
	case len(kts) == 0:
		return KeyTypeText
	case len(kts) == 1:
		return kts[0]
	default:
		return kts[i]
	}
}

// Fingerprint identifies the stage definitions a store's state was
// derived under: any change to them invalidates the store
// (stagestore.Open resets on mismatch and the syncable re-derives from
// the log). The marshal is DECLARED CONTENT ONLY (omitempty everywhere;
// WhenClause marshals its set arms explicitly), so adding vocabulary
// fields in a new binary does NOT alter the fingerprints of configs
// that don't use them — the field lesson: a spurious reset is not "one
// harmless rebuild", it is a silent ~30-minute (hours at scale)
// re-derivation with live-tail latency degraded the whole way. The
// reference contract test pins this stability; a DELIBERATE semantic
// change that must reset unchanged configs gets an upgrade-notes
// callout, never an accident.
func Fingerprint(stages []Stage) string {
	bs, err := json.Marshal(stages)
	if err != nil {
		// Stages are plain data; Marshal cannot fail on them. Guard anyway:
		// a non-marshalable future field must not silently reuse stale state.
		// The shape is stored with the syncable's schema, so like every
		// persisted string its error text goes through the redaction choke
		// point.
		msg, _ := cluster.RedactedMessage(err)
		return "unmarshalable:" + msg
	}
	sum := sha256.Sum256(bs)
	return hex.EncodeToString(sum[:])
}

// IndexOf returns the index of the stage with this name, or -1.
func IndexOf(stages []Stage, name string) int {
	for i := range stages {
		if stages[i].Name == name {
			return i
		}
	}
	return -1
}
