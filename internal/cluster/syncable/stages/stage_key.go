package stages

import (
	"fmt"
	"strings"

	"github.com/PaesslerAG/jsonpath"
	"go.uber.org/zap"
)

// Computed stage keys — SQL's GROUP BY <expression>. A keyPath entry that
// compiles in the closed language to something other than a bare path is
// an expression (`date_trunc('month', $.paid_at::timestamptz)`,
// `lower(trim($.email))`), evaluated against the input and materialized
// like any stored value before keyType/normalize render it; a null result
// or a data error is non-membership, as a missing path is. Every other
// entry is a path, resolved exactly as before expressions were allowed —
// including the jsonpath library's own bare forms (`tenant_id`, `a.b`),
// which resolve today and must keep resolving.
//
// The classification changes only entries that never resolved a key: one
// the jsonpath library cannot parse, or one it parses as a function call
// (it has no functions, so the call always fails). Those are now either an
// expression or a loud admission error.

// ClassifyKeyPath reports whether a keyPath entry is an expression
// (isExpr) or a path, and returns the compile error for an entry that is
// neither a resolvable path nor a valid expression.
func ClassifyKeyPath(kp string) (n Node, isExpr bool, err error) {
	n, cerr := Compile(kp)
	if cerr == nil {
		if _, isPath := n.(exprPath); !isPath {
			return n, true, nil
		}
		return nil, false, nil
	}
	check := kp
	if rest, ok := strings.CutPrefix(check, "$parent"); ok {
		check = "$" + rest
	}
	if _, perr := jsonpath.New(check); perr == nil && !hasCallOutsideBrackets(kp) {
		return nil, false, nil // a path the jsonpath library resolves (tenant_id, a.b)
	}
	return nil, false, cerr
}

// hasCallOutsideBrackets reports a ( outside [...] and quotes — the
// jsonpath library's function-call form, which never resolves (filters and
// scripts live inside brackets).
func hasCallOutsideBrackets(s string) bool {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '[':
			depth++
		case c == ']':
			depth--
		case c == '(' && depth == 0:
			return true
		}
	}
	return false
}

// compileKeyPart compiles an expression keyPath entry; a path compiles to
// nil (resolved by ResolvePath).
func compileKeyPart(kp string) (Node, error) {
	n, isExpr, err := ClassifyKeyPath(kp)
	if err != nil || !isExpr {
		return nil, err
	}
	return n, nil
}

// validateKeyPart is admission for one keyPath entry: a path must be
// single-valued; an expression must compile (the language's own checks —
// a key is a stored value, so a bare division is rejected too).
func validateKeyPart(kp, where string) error {
	_, isExpr, err := ClassifyKeyPath(kp)
	if err != nil {
		return fmt.Errorf("%s [%s] is neither a path nor a valid expression: %w", where, kp, err)
	}
	if !isExpr {
		return RejectMultiValued(kp, where)
	}
	return nil
}

// walkKeyPaths visits the jsonpaths a keyPath entry reads: the entry
// itself, or every path inside its expression.
func walkKeyPaths(kp string, visit func(string)) {
	n, err := compileKeyPart(kp)
	if err != nil {
		return // validateKeyPart reports it
	}
	if n == nil {
		visit(kp)
		return
	}
	walkExprPaths(n, visit)
}

// compileKeyParts compiles a stage's keyPath for its graph node; an entry
// that fails to compile (admission rejects such configs) stays a path,
// which resolves nothing.
func compileKeyParts(kps []string) []Node {
	out := make([]Node, len(kps))
	for i, kp := range kps {
		if n, err := compileKeyPart(kp); err == nil {
			out[i] = n
		}
	}
	return out
}

// resolveKeyValue resolves key part i of a stage for one input.
func (n *graphNode) resolveKeyValue(i int, data, parent any) (any, error) {
	kp := n.def.KeyPath[i]
	if i >= len(n.keyExprs) || n.keyExprs[i] == nil {
		return ResolvePath(kp, data, parent)
	}
	v, err := Eval(n.keyExprs[i], data, parent)
	if err == nil {
		v, err = Materialize(v)
	}
	if err != nil {
		if !n.keyExprWarned {
			n.keyExprWarned = true
			zap.L().Warn("[stage] a keyPath expression failed on an input — such inputs are not members of any key; if every input fails, the stage's output is silently empty",
				zap.String("stage", n.def.Name),
				zap.String("keyPath", kp),
				zap.Error(err))
		}
		return nil, err
	}
	return v, nil
}
