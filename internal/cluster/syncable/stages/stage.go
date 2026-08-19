package stages

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Stage key/order comparison types (numeric vs lexical).
const (
	KeyTypeText   = "text"
	KeyTypeNumber = "number"
)

// ValidateWhen checks when-clause shapes: exactly one of equals or null
// per clause, scalar literals only, no empty paths.
func ValidateWhen(clauses []WhenClause, where string) error {
	for _, cl := range clauses {
		if cl.Path == "" {
			return fmt.Errorf("%s: when entry with empty path", where)
		}
		if (cl.Equals != nil) == cl.Null {
			return fmt.Errorf("%s: when entry for %q: exactly one of equals or null is required", where, cl.Path)
		}
		if cl.Equals != nil && !IsScalar(cl.Equals) {
			return fmt.Errorf("%s: when entry for %q: equals must be a scalar literal, got %T", where, cl.Path, cl.Equals)
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
	Name    string
	From    string
	KeyPath []string
	When    []WhenClause
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
	Reduce      string
	OrderBy     string
	OrderByType string
	TieBy       string
	TieByType   string
	Joins       []Join
	Emit        []Emit
	// ForEach fans each input into N element-inputs (the deliberately
	// multi-valued path selects them): keyPath and emit/join paths resolve
	// against the ELEMENT, `$parent.` reaches the enclosing input, and a
	// re-emitted input reconciles (vanished elements retract; the input's
	// tombstone retracts them all). Elements feed the stage's reduce like
	// any input, so forEach + aggregate is fan-then-fold in one stage.
	ForEach string
	// DeleteWhen (reduce = "liveSet" only) classifies delete-shaped
	// events: a key is LIVE while it has qualifying inputs and ZERO
	// inputs matching DeleteWhen — created-minus-deleted as a set
	// difference, no ordering needed. A delete-shaped event is retained
	// as NEGATIVE evidence (it skips the when filter), so its own
	// retraction un-deletes the key. The live key emits from its
	// bytewise-largest non-delete input, like a reshape.
	DeleteWhen []WhenClause
	// ElementKey is a fanned element's IDENTITY (element-scoped path) when
	// it differs from keyPath — the aggregate sidecar's ElementKey
	// precedent. keyPath is the REDUCE key (which output an element folds
	// into); ElementKey is which retained input it IS (what a re-delivery
	// replaces). Defaults to keyPath: fine for 1:1 fan (element id = row
	// id), required when a reduce folds multiple same-key elements (two
	// same-workarea amounts must both count).
	ElementKey string
}

// Join is one filtering join of a stage: the stage's inputs
// participate only while the joined topic's row — addressed by the
// input's On value against the joined entity's KEY — exists and matches
// every Where clause. A dimension change refolds every dependent key
// (reverse-index fan-out); a dimension that has not arrived yet fails
// participation and heals when it lands. Joins FILTER (gap 6) — field
// resolution from joins is a later arm.
type Join struct {
	Topic string `mapstructure:"topic"`
	// From joins against a PRIOR stage instead of a topic (exactly one of
	// Topic or From): the dimension rows are that stage's outputs, keyed
	// by its out key and maintained by the drain — so cross-stage
	// correlation is a join, not a second input. Manifest order applies:
	// the joined stage must be declared earlier.
	From string `mapstructure:"from"`
	On   string `mapstructure:"on"`
	// Absent inverts the join into an ANTI-join: the input participates
	// only while NO dimension row matches (none exists, or none passes
	// Where). The fan-out gives the hard half for free: when a matching
	// row ARRIVES, dependents refold and retract — and when it leaves or
	// stops matching, they heal back in. A missing On value is vacuously
	// absent (nothing to reference), so it participates.
	Absent bool         `mapstructure:"absent"`
	Where  []WhenClause `mapstructure:"where"`
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
// retained inputs) or Count (rows). Array-of-tables so field names
// survive byte-exact.
type Emit struct {
	Field string `mapstructure:"field"`
	From  string `mapstructure:"from"`
	Expr  string `mapstructure:"expr"`
	Sum   string `mapstructure:"sum"`
	Min   string `mapstructure:"min"`
	Max   string `mapstructure:"max"`
	Count bool   `mapstructure:"count"`

	// compiled holds the admission-checked AST of whichever expression arm
	// is set (Expr/Sum/Min/Max), populated by validateProjectionConfig.
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
		if st.From == "" {
			return fmt.Errorf("%s: from is required — a topic id, or a prior stage's name", where)
		}
		if st.From == st.Name || (!names[st.From] && IndexOf(stages, st.From) >= 0) {
			return fmt.Errorf("%s: from %q references a stage at or after this one — stages chain in manifest order (move the producer above its consumer)", where, st.From)
		}
		names[st.Name] = true
		if len(st.KeyPath) == 0 {
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
			return fmt.Errorf("%s: reduce %q is invalid (want \"latest\", \"aggregate\", or omit for a reshape stage)", where, st.Reduce)
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
		for ji, j := range st.Joins {
			jw := fmt.Sprintf("%s join %d (%q)", where, ji+1, j.target())
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
			}
			if j.On == "" {
				return fmt.Errorf("%s: on is required — the input field holding the joined entity's key", jw)
			}
			if err := RejectMultiValued(j.On, jw+": on"); err != nil {
				return err
			}
			if err := ValidateWhen(j.Where, jw); err != nil {
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
			for _, set := range []bool{e.Sum != "", e.Min != "", e.Max != "", e.Count} {
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
					return fmt.Errorf("%s: an aggregate stage's emit carries exactly one of sum, min, max, or count", ew)
				}
			} else {
				if plain != 1 || folds != 0 {
					return fmt.Errorf("%s: a reshape stage's emit carries exactly one of from or expr", ew)
				}
			}
			if e.From != "" {
				if err := RejectMultiValued(e.From, ew); err != nil {
					return err
				}
			}
			for _, src := range []string{e.Expr, e.Sum, e.Min, e.Max} {
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

// Fingerprint identifies the stage definitions a store's state was
// derived under: any change to them invalidates the store
// (stagestore.Open resets on mismatch and the syncable re-derives from
// the log). JSON over the exported stage fields is deterministic for a
// given binary, and a marshal change across binaries just forces one
// harmless rebuild.
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
