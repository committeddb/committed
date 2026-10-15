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

// validateWindowFrames applies PostgreSQL's RANGE-offset rules at
// admission: an offset frame measures by exactly one orderBy key, and when
// that key's type is declared by a cast, the offset must suit it (a number
// for a number, an interval for a date/time). An undeclared key's type is
// checked on data.
func validateWindowFrames(st *Stage, where string) error {
	for _, s := range st.winSlots {
		f := s.frame
		if f == nil || f.rows || (!f.start.offset() && !f.end.offset()) {
			continue
		}
		if len(st.winOrder) != 1 {
			return fmt.Errorf("%s: %s(…) over (range … preceding|following) needs exactly one orderBy key — the frame measures by it (as in PostgreSQL); use rows, or order by one expression", where, s.fn)
		}
		cast, ok := st.winOrder[0].expr.(exprCast)
		if !ok {
			continue
		}
		for _, b := range []winBound{f.start, f.end} {
			if !b.offset() {
				continue
			}
			_, isInterval := b.off.(intervalVal)
			switch cast.typ {
			case "numeric", "integer", "bigint":
				if isInterval {
					return fmt.Errorf("%s: %s(…): a range frame offset over a %s orderBy is a number, not an interval", where, s.fn, cast.typ)
				}
			case "timestamptz", "timestamp", "date", "interval":
				if !isInterval {
					return fmt.Errorf("%s: %s(…): a range frame offset over a %s orderBy is an interval (interval '1 hour'), not a number", where, s.fn, cast.typ)
				}
			default:
				return fmt.Errorf("%s: %s(…): a range frame offset needs a number or date/time orderBy, not %s", where, s.fn, cast.typ)
			}
		}
	}
	return nil
}
