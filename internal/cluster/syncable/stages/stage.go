package stages

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
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
			if _, ok := ratFromScalar(lit); !ok {
				return fmt.Errorf("%s: when entry for %q: %s takes a numeric literal (values compare numerically, SQL-style), got %T", where, cl.Path, arm, lit)
			}
		}
	}
	return nil
}

// Stage is one internal stage of a staged computation: a keyed
// refold from ONE input (a topic, or a PRIOR stage by name) into a private
// keyed object held in the syncable's stage store — never a topic, never a
// sink write (the terminal rule: only the table is outward-facing). Stages
// chain by name in manifest order; a table source consumes a stage's
// output via `from = "<stage name>"`.
type Stage struct {
	Name    string       `json:"name,omitempty"`
	From    string       `json:"from,omitempty"`
	KeyPath []string     `json:"keyPath,omitempty"`
	When    []WhenClause `json:"when,omitempty"`
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
}

// MergeEntry names one merged upstream and its alias in the tuple
// scope. As defaults to the stage name; admission requires the
// effective alias to be a path-safe identifier (add `as` for names
// jsonpath's dot syntax cannot address, e.g. dashed stage names).
type MergeEntry struct {
	Stage string `mapstructure:"stage" json:"stage,omitempty"`
	As    string `mapstructure:"as" json:"as,omitempty"`
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
	for i := range stages {
		st := &stages[i]
		where := fmt.Sprintf("stage %d (%q)", i+1, st.Name)
		if st.Name == "" {
			return fmt.Errorf("stage %d: name is required (stages chain by name)", i+1)
		}
		if names[st.Name] {
			return fmt.Errorf("%s: a second stage named %q", where, st.Name)
		}
		if st.From == "" && len(st.Merge) == 0 {
			return fmt.Errorf("%s: from is required — a topic id, or a prior stage's name (or merge, to combine prior stages by key)", where)
		}
		if st.From != "" && len(st.Merge) > 0 {
			return fmt.Errorf("%s: exactly one of from or merge (a stage consumes one input stream, or combines prior stages by key)", where)
		}
		if st.From != "" && (st.From == st.Name || (!names[st.From] && IndexOf(stages, st.From) >= 0)) {
			return fmt.Errorf("%s: from %q references a stage at or after this one — stages chain in manifest order (move the producer above its consumer)", where, st.From)
		}
		names[st.Name] = true
		if len(st.Merge) > 0 {
			if err := validateMergeStage(stages, st, names, where); err != nil {
				return err
			}
		}
		if len(st.KeyPath) == 0 && len(st.Merge) == 0 {
			return fmt.Errorf("%s: keyPath is required (one path per key part; several = a composite key)", where)
		}
		for _, kp := range st.KeyPath {
			if err := RejectMultiValued(kp, where+": keyPath"); err != nil {
				return err
			}
		}
		if st.ForEach != "" && len(st.KeyPath) > 1 && st.ElementKey == "" {
			return fmt.Errorf("%s: a forEach stage with a composite keyPath needs elementKey (the element's own identity)", where)
		}
		if err := ValidateWhen(st.When, where); err != nil {
			return err
		}
		if st.ForEach == "" {
			if err := RejectParentPaths(st.When, where+" when"); err != nil {
				return err
			}
			if err := RejectParentPaths(st.DeleteWhen, where+" deleteWhen"); err != nil {
				return err
			}
		}
		if st.Reduce == "liveSet" && st.ForEach != "" {
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
			return fmt.Errorf("%s: reduce %q is invalid (want \"latest\", \"aggregate\", \"liveSet\", or omit for a reshape stage)", where, st.Reduce)
		}
		if st.ForEach != "" && !MultiValuedPath(st.ForEach) {
			return fmt.Errorf("%s: forEach path [%s] is single-valued — forEach fans an array into element-inputs; drop it for one input per entity", where, st.ForEach)
		}
		if st.ElementKey != "" {
			if st.ForEach == "" {
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
		for ji, j := range st.Joins {
			jw := fmt.Sprintf("%s join %d (%q)", where, ji+1, j.target())
			if !ValidNormalize(j.Normalize) {
				return fmt.Errorf("%s: normalize %q is not supported (want %q)", jw, j.Normalize, NormalizeLower)
			}
			if (j.Topic == "") == (j.From == "") {
				return fmt.Errorf("%s: exactly one of topic (a topic's rows by entity key) or from (a PRIOR stage's outputs by its key) is required", jw)
			}
			if j.Topic != "" && IndexOf(stages, j.Topic) >= 0 {
				return fmt.Errorf("%s: %q is a stage — join it with from = %q (topic joins address topics)", jw, j.Topic, j.Topic)
			}
			if j.From != "" {
				fi := IndexOf(stages, j.From)
				if fi < 0 {
					return fmt.Errorf("%s: from %q names no declared stage", jw, j.From)
				}
				if fi >= i {
					return fmt.Errorf("%s: from %q references a stage at or after this one — stages join in manifest order (move the producer above its consumer)", jw, j.From)
				}
				if j.Normalize != "" {
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
			if j.From != "" {
				if fi := IndexOf(stages, j.From); fi >= 0 {
					if arity, _, _ := ResolvedKeySpace(stages, fi); len(j.On) != arity {
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
			}
			if j.Optional && j.As == "" {
				return fmt.Errorf("%s: optional is only meaningful with as — an unnamed join only filters, and an optional filter is no filter", jw)
			}
			if j.From != "" && len(j.OnType) > 0 {
				return fmt.Errorf("%s: onType is not declarable on a stage join — references render in the joined stage's key space, so the join inherits stage %q's keyType automatically; declare it on that stage", jw, j.From)
			}
			if err := validateKeyTypes(j.OnType, len(j.On), jw+" onType"); err != nil {
				return err
			}
		}
		if len(joinAliases) > 0 {
			if err := rejectFoldTimeAliasRefs(st, joinAliases, where); err != nil {
				return err
			}
		}
		if len(st.Emit) == 0 {
			return fmt.Errorf("%s: emit needs at least one field", where)
		}
		emitted := make(map[string]bool, len(st.Emit))
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
				if folds != 1 || plain != 0 {
					return fmt.Errorf("%s: an aggregate stage's emit carries exactly one of sum, min, max, count, or collect", ew)
				}
			} else {
				if plain != 1 || folds != 0 {
					return fmt.Errorf("%s: a reshape stage's emit carries exactly one of from or expr", ew)
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
				if st.ForEach == "" {
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
	}
	return nil
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
		if e.Stage == "" {
			return fmt.Errorf("%s: stage is required", ew)
		}
		if !names[e.Stage] {
			if IndexOf(stages, e.Stage) >= 0 {
				return fmt.Errorf("%s: %q references a stage at or after this one — stages merge in manifest order (move the producer above its consumer)", ew, e.Stage)
			}
			return fmt.Errorf("%s: %q names no declared stage (merge combines STAGES; fold a topic through a stage first)", ew, e.Stage)
		}
		if seenStage[e.Stage] {
			return fmt.Errorf("%s: stage %q merged twice", ew, e.Stage)
		}
		seenStage[e.Stage] = true
		alias := e.As
		if alias == "" {
			alias = e.Stage
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
	baseArity, baseKT, baseNorm := ResolvedKeySpace(stages, IndexOf(stages, st.Merge[0].Stage))
	for _, e := range st.Merge[1:] {
		arity, kt, norm := ResolvedKeySpace(stages, IndexOf(stages, e.Stage))
		if arity != baseArity {
			return fmt.Errorf("%s: merged stages disagree on key arity (%q keys by %d part(s), %q by %d) — merged sides pair by key, so their key spaces must agree", where, st.Merge[0].Stage, baseArity, e.Stage, arity)
		}
		if !equalKeyTypes(baseKT, kt, baseArity) {
			return fmt.Errorf("%s: merged stages disagree on keyType (%q vs %q) — a rendering disagreement pairs NOTHING, silently; declare matching keyType on both", where, st.Merge[0].Stage, e.Stage)
		}
		if norm != baseNorm {
			return fmt.Errorf("%s: merged stages disagree on normalize (%q declares %q, %q declares %q) — a rendering disagreement pairs NOTHING, silently; declare matching normalize on both", where, st.Merge[0].Stage, baseNorm, e.Stage, norm)
		}
	}
	return nil
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
		if err := checkPath(kp, "keyPath"); err != nil {
			return err
		}
	}
	if st.ForEach != "" {
		if err := checkPath(st.ForEach, "forEach"); err != nil {
			return err
		}
	}
	for ji := range st.Joins {
		for _, on := range st.Joins[ji].On {
			if err := checkPath(on, "join on"); err != nil {
				return err
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
		return ResolvedKeySpace(stages, IndexOf(stages, st.Merge[0].Stage))
	}
	return len(st.KeyPath), st.KeyType, st.Normalize
}

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
	if len(keyParts) != arity {
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
// golden contract test pins this stability; a DELIBERATE semantic
// change that must reset unchanged configs gets an upgrade-notes
// callout, never an accident.
func Fingerprint(stages []Stage) string {
	bs, err := json.Marshal(stages)
	if err != nil {
		// Stages are plain data; Marshal cannot fail on them. Guard anyway:
		// a non-marshalable future field must not silently reuse stale state.
		return fmt.Sprintf("unmarshalable:%v", err)
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
