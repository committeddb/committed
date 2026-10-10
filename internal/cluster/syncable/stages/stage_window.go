package stages

import (
	"fmt"
	"strings"
)

// validateWindowStage holds a window stage's shape rules: no keyPath (its
// rows key by the input's own key), no declared key space (inherited), no
// fan (fan in a prior stage), an orderBy, and compilable partition and
// order keys — which it compiles in place.
func validateWindowStage(st *Stage, where string) error {
	switch {
	case len(st.KeyPath) > 0:
		return fmt.Errorf("%s: keyPath is not used on a window stage — each input keeps its own row, keyed by the input's own key; partitionBy groups the rows", where)
	case len(st.KeyType) > 0 || st.Normalize != "":
		return fmt.Errorf("%s: keyType/normalize are not declarable on a window stage — its rows key by the input's own key (a stage input's key space is inherited)", where)
	case st.hasFan():
		return fmt.Errorf("%s: forEach/fan is not supported on a window stage — fan in a prior stage keyed by the element's identity, and window its outputs", where)
	case st.OrderBy != "" || st.TieBy != "" || st.OrderByType != "" || st.TieByType != "":
		return fmt.Errorf("%s: a window stage's orderBy/tieBy are expression lists (PartitionBy/WindowOrderBy/WindowTieBy); orderByType/tieByType are latest's — cast instead ($.at::timestamptz, $.n::numeric)", where)
	case len(st.WindowOrderBy) == 0:
		return fmt.Errorf("%s: reduce = \"window\" needs orderBy — each partition's business order (expressions, each optionally asc|desc and nulls first|last); arrival order diverges under backfill", where)
	}
	for _, p := range st.PartitionBy {
		if err := validateKeyPart(p, where+": partitionBy"); err != nil {
			return err
		}
	}
	var err error
	if st.winOrder, err = compileWinKeys(st.WindowOrderBy, where+": orderBy"); err != nil {
		return err
	}
	if st.winTie, err = compileWinKeys(st.WindowTieBy, where+": tieBy"); err != nil {
		return err
	}
	var perr error
	windowKeyPathRefs(st, func(p, pos string) {
		if perr == nil && strings.HasPrefix(p, "$parent") {
			perr = fmt.Errorf("%s: %s [%s] reads $parent — a window stage does not fan, so there is no enclosing input", where, pos, p)
		}
	})
	if perr != nil {
		return perr
	}
	return nil
}

func compileWinKeys(srcs []string, where string) ([]winKey, error) {
	out := make([]winKey, len(srcs))
	for i, src := range srcs {
		k, err := compileWinKey(src)
		if err != nil {
			return nil, fmt.Errorf("%s [%s]: %w", where, src, err)
		}
		out[i] = k
	}
	return out, nil
}

// walkWindowKeyPaths visits the paths a partitionBy entry (a keyPath-style
// part) or an orderBy/tieBy entry (an expression with a direction) reads.
func walkWindowKeyPaths(src string, visit func(string)) {
	if k, err := compileWinKey(src); err == nil {
		walkExprPaths(k.expr, visit)
		return
	}
	walkKeyPaths(src, visit)
}

// windowKeyPathRefs visits every path a window stage reads at FOLD time —
// its partition and order keys — with the position it appears in.
func windowKeyPathRefs(st *Stage, visit func(path, pos string)) {
	for _, kp := range st.PartitionBy {
		walkKeyPaths(kp, func(p string) { visit(p, "partitionBy") })
	}
	for _, src := range st.WindowOrderBy {
		walkWindowKeyPaths(src, func(p string) { visit(p, "orderBy") })
	}
	for _, src := range st.WindowTieBy {
		walkWindowKeyPaths(src, func(p string) { visit(p, "tieBy") })
	}
}
