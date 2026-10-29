package stages

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// renderContexts are the places an expression compiles: each compiles the
// source and reports the tree, which Render must print back to source that
// compiles to the same tree in the same context.
var renderContexts = map[string]func(string) (any, error){
	"plain": func(s string) (any, error) { return Compile(s) },
	"aggregate": func(s string) (any, error) {
		var slots []exprAgg
		n, err := compileAggregate(s, &slots)
		return n, err
	},
	"window": func(s string) (any, error) {
		var slots []exprWin
		n, err := compileWindowExpr(s, &slots)
		return n, err
	},
	"order key": func(s string) (any, error) { return compileWinKey(s) },
}

func renderAny(v any) string {
	if k, ok := v.(winKey); ok {
		return renderWinKey(k)
	}
	return Render(v.(Node))
}

func equalAny(a, b any) bool {
	if ka, ok := a.(winKey); ok {
		kb, ok := b.(winKey)
		return ok && ka.desc == kb.desc && ka.nullsFirst == kb.nullsFirst && sameTree(ka.expr, kb.expr)
	}
	return sameTree(a.(Node), b.(Node))
}

// checkRender asserts one source's fidelity in every context it compiles
// in, and reports whether it compiled in any.
func checkRender(t *testing.T, src string) bool {
	t.Helper()
	compiled := false
	for _, name := range []string{"plain", "aggregate", "window", "order key"} {
		n, err := renderContexts[name](src)
		if err != nil {
			continue
		}
		compiled = true
		text := renderAny(n)
		back, err := renderContexts[name](text)
		if err != nil {
			t.Errorf("%s %q renders as %q, which does not compile: %v", name, src, text, err)
			continue
		}
		if !equalAny(n, back) {
			t.Errorf("%s %q renders as %q, which compiles to a different tree (renders back as %q)", name, src, text, renderAny(back))
		}
	}
	return compiled
}

func TestRender_RoundTripsEveryConstruct(t *testing.T) {
	for _, src := range []string{
		"1", "1.50", "007", "-5", "- - 5", "'it''s'", "''", "true", "FALSE", "NULL", "$.a", "$parent.b", "$.items[0]",
		"$.a + $.b * 2 - 1", "($.a + $.b) * 2", "$.a || 'x' || $.b", "-$.a::int", "not $.a and $.b or $.c",
		"$.a = 1", "$.a <> 'x'", "$.a < 1", "$.a <= 1", "$.a > 1", "$.a >= 1",
		"$.a between 1 and 2", "$.a not between 1 and 2", "$.a in (1, 2, 'x')", "$.a not in (1)",
		"$.a like 'x%'", "$.a not like 'x%'", "$.a is null", "$.a is not null",
		"$.a is distinct from $.b", "$.a is not distinct from $.b",
		"coalesce($.a, $.b, 0)", "nullif($.a, 0)", "round($.a / $.b, 2)", "trunc($.a)", "ROUND($.a, 0)",
		"cast($.a as decimal)", "try_cast($.a as int8)", "$.a::varchar", "$.a::timestamptz", "date '2024-01-15'", "interval '30 days'",
		"extract(YEAR from $.at::timestamptz)", "extract('dow' from $.at::date)", "date_part('Years', $.at::timestamptz)", "date_trunc('MONTH', $.at::timestamptz)",
		"case when $.a > 1 then 'big' when $.a > 0 then 'small' else 'none' end", "case $.a when 1 then 'one' end",
		"source()", "source() = 'orders'", "(array_agg($.a))[1]", "(array_agg($.a))[1:2]", "(array_agg($.a))[:2]", "(array_agg($.a))[2:]", "(array_agg($.a))[:]",
		"sum($.a)", "count(*)", "count(distinct $.a)", "round(avg($.a), 2)", "sum($.a) filter (where $.b = 'x')",
		"array_agg($.a order by $.b desc, $.c nulls first)", "array_agg(distinct $.a order by $.a)", "string_agg($.a, ', ' order by $.a desc nulls last)",
		"round(sum($.rev) / nullif(sum($.cost), 0), 4)", "count(*) > 5",
		"row_number()", "rank()", "dense_rank()", "lag($.a)", "lag($.a, 2)", "lead($.a, 0, 'none')", "lag($.a, 1, 0)",
		"first_value($.a)", "last_value($.a) over (rows between unbounded preceding and unbounded following)",
		"sum($.a) over (rows 3 preceding)", "sum($.a) over (rows between 2 preceding and 1 following)",
		"sum($.a) over (range between 10 preceding and current row)", "sum($.a) over (range between 1.5 preceding and current row)",
		"count(*) over (range between interval '1 day' preceding and current row)", "count(*) over (range between interval '1 year 2 mons 3 days 04:05:06.5' preceding and interval '0' following)",
		"sum($.a) filter (where $.b) over (rows between current row and unbounded following)",
		"sum($.a) is distinct from 0", "sum($.a) is not distinct from sum($.b)", "sum($.a) between 1 and 2", "1 <= sum($.a) and sum($.a) <= 2",
		"lag($.s) is distinct from $.s", "lag($.s) between 1 and 2", "1 <= $.a and $.a <= 2",
		"$.a desc", "$.a asc", "$.a desc nulls last", "$.a nulls first", "lower($.a) desc nulls first",
	} {
		if !checkRender(t, src) {
			t.Errorf("%q compiles in no context", src)
		}
	}
}

// TestRender_RoundTripsTheCorpus checks every expression the repository's
// tests, docs, and examples write: each Go string literal of the syncable
// tests, and each quoted string of the docs and examples, is compiled in
// every context, and wherever it compiles its rendering must compile back
// to the same tree.
func TestRender_RoundTripsTheCorpus(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	srcs := map[string]bool{}
	quoted := regexp.MustCompile(`"(?:[^"\\\n]|\\.)*"`)
	addQuoted := func(text string) {
		for _, q := range quoted.FindAllString(text, -1) {
			if s, err := strconv.Unquote(q); err == nil {
				srcs[s] = true
			}
		}
	}
	walk := func(dir string, visit func(path string) error) {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			return visit(path)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fset := token.NewFileSet()
	walk(filepath.Join("internal", "cluster", "syncable"), func(path string) error {
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					srcs[s] = true
					addQuoted(s) // TOML embedded in a test's config literal
				}
			}
			return true
		})
		return nil
	})
	for _, dir := range []string{"docs", "examples"} {
		walk(dir, func(path string) error {
			if !strings.HasSuffix(path, ".md") && !strings.HasSuffix(path, ".toml") {
				return nil
			}
			bs, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			addQuoted(string(bs))
			return nil
		})
	}
	sorted := make([]string, 0, len(srcs))
	for s := range srcs {
		sorted = append(sorted, s)
	}
	sort.Strings(sorted)
	compiled := 0
	for _, s := range sorted {
		if checkRender(t, s) {
			compiled++
		}
	}
	t.Logf("%d expressions compiled and round-tripped (of %d strings)", compiled, len(sorted))
	if compiled < 500 {
		t.Errorf("only %d expressions found — the corpus walk has lost its sources", compiled)
	}
}
