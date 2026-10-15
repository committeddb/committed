package stages

import "fmt"

// source() names the source an input came from — the `from` entry, a
// topic id or a prior stage's name, as text. It is the UNION ALL branch
// literal (SELECT 'a' AS src … UNION ALL SELECT 'b' …) as a function: a
// listed divergence, since PostgreSQL has no such function; a SQL
// frontend compiles per-branch literals to it. It depends on the config
// alone, never on arrival, so it is deterministic. In a single-source
// stage it returns that source's name.
//
// It reads the INPUT, so it is valid wherever input paths are: when,
// deleteWhen, keyPath, a window's partitionBy/orderBy/tieBy, latest's
// order keys, emits, and aggregate arguments. It is rejected where there
// is no input: a join's where (the dimension row), a merge stage (no
// from), outside the aggregate calls of an aggregate stage's expressions,
// and table-level expressions.

type exprSource struct{}

func (exprSource) node() {}

// sourceScope carries an input's source name beside the object a stage
// evaluates — the scope source() reads. Paths see the object (rowObject
// and ResolvePath unwrap it), and it is never stored: stages wrap at
// evaluation, after retention and join lookup.
type sourceScope struct {
	obj any
	src string
}

// withSource wraps an input object with its source ("" = no source: a
// merge's tuple, which source() admission keeps away from).
func withSource(obj any, src string) any {
	if src == "" {
		return obj
	}
	return sourceScope{obj: obj, src: src}
}

// WithSource is withSource for evaluators outside the package (the
// dry-run's when observers), so source() judges a union's input as the
// stage does.
func WithSource(obj any, src string) any { return withSource(obj, src) }

// unwrapSource returns the object a sourceScope carries (any other value
// as is).
func unwrapSource(v any) any {
	if s, ok := v.(sourceScope); ok {
		return s.obj
	}
	return v
}

// parseSource parses source's empty argument list after its name.
func (p *exprParser) parseSource(pos int) (Node, error) {
	if err := p.expectOp("("); err != nil {
		return nil, err
	}
	if t := p.next(); t.kind != "op" || t.text != ")" {
		return nil, fmt.Errorf("source() at position %d takes no arguments — it names the input's from source", pos)
	}
	return exprSource{}, nil
}

func evalSource(payload any) (any, error) {
	if w, ok := payload.(windowRow); ok {
		payload = w.obj
	}
	if s, ok := payload.(sourceScope); ok {
		return s.src, nil
	}
	return nil, fmt.Errorf("source() has no stage input here")
}

// UsesSource reports whether a compiled expression calls source().
func UsesSource(n Node) bool {
	if _, ok := n.(exprSource); ok {
		return true
	}
	for _, c := range exprChildren(n) {
		if UsesSource(c) {
			return true
		}
	}
	return false
}

// RejectSourceInWhens rejects source() in when clauses evaluated where
// there is no stage input (a table rule's or source's when, a join's
// where over the dimension row).
func RejectSourceInWhens(clauses []WhenClause, where, why string) error {
	for _, c := range clauses {
		if c.Expr == "" {
			continue
		}
		n := c.compiled
		if n == nil {
			var err error
			if n, err = Compile(c.Expr); err != nil {
				continue // ValidateWhen reports it
			}
		}
		if UsesSource(n) {
			return fmt.Errorf("%s: expr [%s] calls source() — %s", where, c.Expr, why)
		}
	}
	return nil
}

// stageUsesSource reports whether any of a stage's own expressions (when,
// keyPath, emits) call source() — the merge-stage check.
func stageUsesSource(st *Stage) bool {
	for _, cls := range [][]WhenClause{st.When, st.DeleteWhen} {
		if RejectSourceInWhens(cls, "", "") != nil {
			return true
		}
	}
	for _, kp := range st.KeyPath {
		if n, err := compileKeyPart(kp); err == nil && n != nil && UsesSource(n) {
			return true
		}
	}
	for i := range st.Emit {
		if n := st.Emit[i].compiled; n != nil && UsesSource(n) {
			return true
		}
	}
	return false
}
