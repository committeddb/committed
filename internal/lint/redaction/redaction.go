// Package redaction is a static check that enforces committed's redaction
// contract: the text of an error — and any rendering of a cluster.Proposal,
// cluster.Actual, or cluster.Entity — reaches a persisted or exposed surface only
// through cluster.RedactedMessage, the one redaction choke point.
//
// The check is a flow-insensitive, interprocedural taint analysis over the
// module's packages, run as a test (see redaction_test.go) so CI fails when a
// new surface bypasses the choke point.
//
// Sources: any value whose static type is (or contains) an error — the
// interface or a concrete type implementing it — and the rendering of a
// cluster.Proposal / Actual / Entity: its String() result, or the value handed
// to a fmt formatting function (%v of a Proposal prints its keys). The entity
// values themselves are the data plane and flow freely. Taint follows local
// variables, function results, and pointer/receiver arguments (a
// strings.Builder written with error text is tainted). Field reads on an
// error (execError.label, ConfigError.Field) are NOT tainted: committed
// authors those fields PII-free; the vector is the foreign text Error() renders.
//
// Surfaces: a string-carrying field or element of a composite literal, an
// assignment to a field/element/package variable, an argument to a function
// whose parameter flows to a surface, and anything written to a
// net/http.ResponseWriter. A RedactedMessage method must itself return
// untainted text.
//
// Choke point: cluster.RedactedMessage, and the RedactedMessage() method of a
// cluster.RedactedError. Nothing else clears taint — a wrapper that only
// forwards its input (truncateDeadLetterMessage, capString) is transparent.
//
// Every function with a body in the module is summarized, Error() methods
// included: an Error() that renders authored fields yields clean text, one
// that renders a wrapped cause (ConfigError, execError) yields tainted text,
// and Error() on an error interface value is always tainted. So
// `writeError(w, 400, code, levelErr.Error())` passes while the same call on
// an `error` fails — the check is semantic, and re-derived on every change.
//
// Known limits: no field-sensitivity on non-source structs (a tainted string
// stored in a local struct then read back is flagged at the store, which is
// the conservative side), calls through interfaces and function values are
// treated as external (their arguments are not surfaces), and a closure's return
// value is not tracked. Node-local logs are deliberately not surfaces.
package redaction

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/types/typeutil"
)

// The domain package is recognized by its import-path suffix so the test
// fixtures can stub it under another root.
const (
	clusterPathSuffix = "/internal/cluster"
	chokePointName    = "RedactedMessage"
)

func isClusterPkg(p *types.Package) bool {
	return p != nil && strings.HasSuffix(p.Path(), clusterPathSuffix)
}

// Finding is one place where unredacted text reaches a surface.
type Finding struct {
	Pos     token.Position
	Message string
}

func (f Finding) String() string { return fmt.Sprintf("%s: %s", f.Pos, f.Message) }

// LoadMode is the packages.Load mode Analyze needs: syntax and type
// information for the requested packages, dependencies from export data.
const LoadMode = packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
	packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports

// Analyze checks pkgs as one program and returns every violation, ordered by
// position. Generated files are skipped.
func Analyze(pkgs []*packages.Package) []Finding {
	p := newProgram(pkgs)
	p.solve()
	return p.report()
}

// taint is the lattice value: real marks text that derives from a source;
// params is the bitset of the enclosing function's parameters it derives from
// (so a callee's surfaces are charged to its callers).
type taint struct {
	real   bool
	params uint64
}

func (t taint) or(u taint) taint { return taint{t.real || u.real, t.params | u.params} }
func (t taint) any() bool        { return t.real || t.params != 0 }
func (t taint) eq(u taint) bool  { return t.real == u.real && t.params == u.params }

// summary is what callers need to know about a function: which parameters
// (receiver first, for methods) flow to a surface, and the taint of each result.
type summary struct {
	surfaceParams uint64
	returns       []taint
}

func (s *summary) eq(o *summary) bool {
	if s == nil || o == nil {
		return s == o
	}
	if s.surfaceParams != o.surfaceParams || len(s.returns) != len(o.returns) {
		return false
	}
	for i := range s.returns {
		if !s.returns[i].eq(o.returns[i]) {
			return false
		}
	}
	return true
}

// definition is one way a local variable acquires a value: an explicit
// right-hand side (pos selects a tuple element, -1 the whole value) or, for a
// variable passed by pointer or as a receiver, the other arguments of that call.
type definition struct {
	expr     ast.Expr
	pos      int
	implicit []ast.Expr
}

type funcInfo struct {
	pkg        *packages.Package
	decl       *ast.FuncDecl
	fn         *types.Func
	sig        *types.Signature
	params     []*types.Var
	paramIndex map[*types.Var]int
	results    []*types.Var
	locals     map[*types.Var]bool
	defs       map[*types.Var][]definition
	// chokePoint is cluster.RedactedMessage itself, whose body is the one
	// place err.Error() legitimately becomes surface text; it is not checked.
	chokePoint bool
	// redactedMethod is a RedactedMessage() string method: its result must be
	// untainted, since callers trust it as redacted.
	redactedMethod bool
}

type program struct {
	funcs      map[*types.Func]*funcInfo
	order      []*funcInfo
	summaries  map[*types.Func]*summary
	errorIface *types.Interface
	rwIface    *types.Interface
}

func newProgram(pkgs []*packages.Package) *program {
	p := &program{
		funcs:      map[*types.Func]*funcInfo{},
		summaries:  map[*types.Func]*summary{},
		errorIface: types.Universe.Lookup("error").Type().Underlying().(*types.Interface),
	}
	seen := map[*packages.Package]bool{}
	for _, pkg := range pkgs {
		if seen[pkg] || pkg.Types == nil || pkg.TypesInfo == nil {
			continue
		}
		seen[pkg] = true
		for _, imp := range pkg.Types.Imports() {
			if imp.Path() == "net/http" && p.rwIface == nil {
				if obj := imp.Scope().Lookup("ResponseWriter"); obj != nil {
					p.rwIface, _ = obj.Type().Underlying().(*types.Interface)
				}
			}
		}
		for _, file := range pkg.Syntax {
			if ast.IsGenerated(file) {
				continue
			}
			for _, d := range file.Decls {
				decl, ok := d.(*ast.FuncDecl)
				if !ok || decl.Body == nil {
					continue
				}
				fn, ok := pkg.TypesInfo.Defs[decl.Name].(*types.Func)
				if !ok {
					continue
				}
				fi := p.newFuncInfo(pkg, decl, fn)
				p.funcs[fn] = fi
				p.order = append(p.order, fi)
			}
		}
	}
	for _, fi := range p.order {
		fi.collectDefs(p)
	}
	return p
}

func (p *program) newFuncInfo(pkg *packages.Package, decl *ast.FuncDecl, fn *types.Func) *funcInfo {
	sig := fn.Type().(*types.Signature)
	fi := &funcInfo{
		pkg: pkg, decl: decl, fn: fn, sig: sig,
		paramIndex: map[*types.Var]int{},
		locals:     map[*types.Var]bool{},
		defs:       map[*types.Var][]definition{},
	}
	if recv := sig.Recv(); recv != nil {
		fi.params = append(fi.params, recv)
	}
	for i := 0; i < sig.Params().Len(); i++ {
		fi.params = append(fi.params, sig.Params().At(i))
	}
	for i, v := range fi.params {
		fi.paramIndex[v] = i
	}
	for i := 0; i < sig.Results().Len(); i++ {
		fi.results = append(fi.results, sig.Results().At(i))
	}
	fi.chokePoint = sig.Recv() == nil && isClusterPkg(fn.Pkg()) && fn.Name() == chokePointName
	fi.redactedMethod = isRedactedMethod(fn)
	return fi
}

// isRedactedMethod reports whether fn is a RedactedMessage() string method —
// the RedactedError contract's redacting method.
func isRedactedMethod(fn *types.Func) bool {
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil || fn.Name() != chokePointName {
		return false
	}
	if sig.Params().Len() != 0 || sig.Results().Len() != 1 {
		return false
	}
	b, ok := sig.Results().At(0).Type().Underlying().(*types.Basic)
	return ok && b.Info()&types.IsString != 0
}

// collectDefs records every definition of every local variable in the body
// (closures included; they share the enclosing function's variables).
func (fi *funcInfo) collectDefs(p *program) {
	info := fi.pkg.TypesInfo
	ast.Inspect(fi.decl.Body, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.Ident:
			if v, ok := info.Defs[n].(*types.Var); ok {
				fi.locals[v] = true
			}
		case *ast.AssignStmt:
			fi.assign(info, n.Lhs, n.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, len(n.Names))
			for i, id := range n.Names {
				lhs[i] = id
			}
			fi.assign(info, lhs, n.Values)
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{n.Key, n.Value} {
				if v := fi.varOf(info, e); v != nil {
					fi.defs[v] = append(fi.defs[v], definition{expr: n.X, pos: -1})
				}
			}
		case *ast.TypeSwitchStmt:
			as, ok := n.Assign.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 {
				break
			}
			ta, ok := as.Rhs[0].(*ast.TypeAssertExpr)
			if !ok {
				break
			}
			for _, c := range n.Body.List {
				if v, ok := info.Implicits[c].(*types.Var); ok {
					fi.locals[v] = true
					fi.defs[v] = append(fi.defs[v], definition{expr: ta.X, pos: -1})
				}
			}
		case *ast.CallExpr:
			if fn := typeutil.StaticCallee(info, n); fn == nil || p.funcs[fn.Origin()] == nil {
				fi.implicitDefs(info, n)
			}
		}
		return true
	})
}

func (fi *funcInfo) varOf(info *types.Info, e ast.Expr) *types.Var {
	id, ok := ast.Unparen(e).(*ast.Ident)
	if !ok || id.Name == "_" {
		return nil
	}
	v, _ := info.ObjectOf(id).(*types.Var)
	return v
}

func (fi *funcInfo) assign(info *types.Info, lhs, rhs []ast.Expr) {
	if len(rhs) == 0 {
		return
	}
	for i, l := range lhs {
		v := fi.varOf(info, l)
		if v == nil {
			continue
		}
		if len(lhs) == len(rhs) {
			fi.defs[v] = append(fi.defs[v], definition{expr: rhs[i], pos: -1})
		} else {
			fi.defs[v] = append(fi.defs[v], definition{expr: rhs[0], pos: i})
		}
	}
}

// implicitDefs models data flowing INTO a body-declared variable through an
// external call: `fmt.Fprintf(&sb, "%v", err)` and `sb.WriteString(err.Error())`
// both make sb carry error text. Only calls without a body in the module are
// modeled this way — a module function that stores an argument into its
// receiver or a pointer parameter is checked at that store, by its own
// summary. Parameters and receivers of the enclosing function are excluded.
func (fi *funcInfo) implicitDefs(info *types.Info, call *ast.CallExpr) {
	var recv ast.Expr
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok {
		if s := info.Selections[sel]; s != nil && s.Kind() == types.MethodVal {
			recv = sel.X
		}
	}
	others := func(skip ast.Expr) []ast.Expr {
		var out []ast.Expr
		if recv != nil && recv != skip {
			out = append(out, recv)
		}
		for _, a := range call.Args {
			if a != skip {
				out = append(out, a)
			}
		}
		return out
	}
	if recv != nil {
		if v := fi.varOf(info, recv); v != nil && fi.locals[v] && len(call.Args) > 0 {
			fi.defs[v] = append(fi.defs[v], definition{pos: -1, implicit: others(recv)})
		}
	}
	for _, a := range call.Args {
		u, ok := ast.Unparen(a).(*ast.UnaryExpr)
		if !ok || u.Op != token.AND {
			continue
		}
		if v := fi.varOf(info, u.X); v != nil && fi.locals[v] {
			fi.defs[v] = append(fi.defs[v], definition{pos: -1, implicit: others(a)})
		}
	}
}

// solve iterates the per-function summaries to a fixed point; taint only
// grows, so the loop terminates.
func (p *program) solve() {
	for iter := 0; iter < 32; iter++ {
		changed := false
		for _, fi := range p.order {
			s := p.evaluate(fi, false).summarize()
			if !s.eq(p.summaries[fi.fn]) {
				p.summaries[fi.fn] = s
				changed = true
			}
		}
		if !changed {
			return
		}
	}
}

func (p *program) report() []Finding {
	out := make([]Finding, 0, len(p.order))
	for _, fi := range p.order {
		out = append(out, p.evaluate(fi, true).findings...)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i].Pos, out[j].Pos
		if a.Filename != b.Filename {
			return a.Filename < b.Filename
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Column < b.Column
	})
	return out
}

// eval is one evaluation of one function body against the current summaries.
type eval struct {
	p        *program
	fi       *funcInfo
	info     *types.Info
	report   bool
	memo     map[ast.Expr]taint
	active   map[*types.Var]bool
	checked  map[*ast.CallExpr]bool
	reported map[token.Pos]bool
	surfaces uint64
	returns  []taint
	findings []Finding
}

func (p *program) evaluate(fi *funcInfo, report bool) *eval {
	e := &eval{
		p: p, fi: fi, info: fi.pkg.TypesInfo, report: report,
		memo:     map[ast.Expr]taint{},
		active:   map[*types.Var]bool{},
		checked:  map[*ast.CallExpr]bool{},
		reported: map[token.Pos]bool{},
		returns:  make([]taint, len(fi.results)),
	}
	if fi.chokePoint {
		return e
	}
	e.walk(fi.decl.Body, false)
	return e
}

func (e *eval) summarize() *summary {
	s := &summary{surfaceParams: e.surfaces, returns: e.returns}
	if e.fi.redactedMethod {
		// Callers treat the result as redacted; a tainted body is reported
		// (checkReturn), not propagated.
		s.returns = make([]taint, len(e.returns))
	}
	return s
}

// walk visits every statement, evaluating each surface site. Returns inside a
// closure belong to the closure, not to the enclosing function's summary.
func (e *eval) walk(n ast.Node, inClosure bool) {
	ast.Inspect(n, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			e.walk(n.Body, true)
			return false
		case *ast.CallExpr:
			e.flow(n) // runs the call's surface checks
		case *ast.CompositeLit:
			e.checkLiteral(n)
		case *ast.AssignStmt:
			e.checkAssign(n)
		case *ast.ReturnStmt:
			if !inClosure {
				e.checkReturn(n)
			}
		}
		return true
	})
}

// surface records that tainted text reached target: a real taint is a finding;
// a parameter-dependent one makes those parameters surfaces for callers.
func (e *eval) surface(at ast.Node, t taint, target string) {
	e.surfaces |= t.params
	if t.real && e.report && !e.reported[at.Pos()] {
		e.reported[at.Pos()] = true
		e.findings = append(e.findings, Finding{
			Pos:     e.fi.pkg.Fset.Position(at.Pos()),
			Message: "unredacted error/entity text reaches " + target + " (route it through cluster.RedactedMessage)",
		})
	}
}

// litElem is one carrier-typed element of a composite literal: the value
// expression and a description of where it lands.
type litElem struct {
	val  ast.Expr
	name string
}

// carrierElements resolves each element of a composite literal to the type it
// is stored into and keeps those that can carry text — a string field, a
// []string element, an any value. An error-typed field or a nested struct is
// left out: storing the error itself keeps the chain redactable, and a
// nested literal is checked on its own.
func (e *eval) carrierElements(lit *ast.CompositeLit) []litElem {
	tv, ok := e.info.Types[lit]
	if !ok {
		return nil
	}
	under := deref(tv.Type).Underlying()
	var out []litElem
	for i, elt := range lit.Elts {
		key, val := ast.Expr(nil), elt
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			key, val = kv.Key, kv.Value
		}
		var target types.Type
		var name string
		switch u := under.(type) {
		case *types.Struct:
			var f *types.Var
			if id, ok := key.(*ast.Ident); ok {
				f, _ = e.info.ObjectOf(id).(*types.Var)
			} else if key == nil && i < u.NumFields() {
				f = u.Field(i)
			}
			if f == nil {
				continue
			}
			target, name = f.Type(), "field "+f.Name()+" of "+e.typeName(tv.Type)
		case *types.Slice:
			target, name = u.Elem(), "an element of "+e.typeName(tv.Type)
		case *types.Array:
			target, name = u.Elem(), "an element of "+e.typeName(tv.Type)
		case *types.Map:
			if key != nil && isCarrier(u.Key()) {
				out = append(out, litElem{val: key, name: "a key of " + e.typeName(tv.Type)})
			}
			target, name = u.Elem(), "a value of "+e.typeName(tv.Type)
		default:
			continue
		}
		if isCarrier(target) {
			out = append(out, litElem{val: val, name: name})
		}
	}
	return out
}

func (e *eval) checkLiteral(lit *ast.CompositeLit) {
	for _, el := range e.carrierElements(lit) {
		if t := e.taint(el.val); t.any() {
			e.surface(el.val, t, el.name)
		}
	}
}

// checkAssign treats a store into anything but a body-declared variable — a
// field, an element, a dereference, a package variable — as a surface.
func (e *eval) checkAssign(as *ast.AssignStmt) {
	if len(as.Rhs) == 0 {
		return
	}
	for i, l := range as.Lhs {
		if v := e.fi.varOf(e.info, l); v != nil {
			if _, isParam := e.fi.paramIndex[v]; isParam || e.fi.locals[v] {
				continue
			}
		}
		if id, ok := ast.Unparen(l).(*ast.Ident); ok && id.Name == "_" {
			continue
		}
		tv, ok := e.info.Types[l]
		if !ok || !isCarrier(tv.Type) {
			continue
		}
		var t taint
		if len(as.Lhs) == len(as.Rhs) {
			t = e.taint(as.Rhs[i])
		} else {
			t = e.taintAt(as.Rhs[0], i)
		}
		if t.any() {
			e.surface(l, t, "the assignment to "+types.ExprString(l))
		}
	}
}

func (e *eval) checkReturn(r *ast.ReturnStmt) {
	n := len(e.fi.results)
	if n == 0 {
		return
	}
	get := func(i int) taint {
		switch {
		case len(r.Results) == 0:
			return e.varTaint(e.fi.results[i])
		case len(r.Results) == 1 && n > 1:
			return e.taintAt(r.Results[0], i)
		case i < len(r.Results):
			return e.taint(r.Results[i])
		}
		return taint{}
	}
	for i := 0; i < n; i++ {
		t := filterType(e.fi.results[i].Type(), get(i))
		if e.fi.redactedMethod {
			if t.real && e.report {
				e.findings = append(e.findings, Finding{
					Pos:     e.fi.pkg.Fset.Position(r.Pos()),
					Message: "RedactedMessage returns unredacted error/entity text; it must be authored PII-free",
				})
			}
			continue
		}
		e.returns[i] = e.returns[i].or(t)
	}
}

// varTaint is the taint of a variable: its type, its parameter index, and
// every definition it has in this function.
func (e *eval) varTaint(v *types.Var) taint {
	t := e.p.byType(v.Type())
	if i, ok := e.fi.paramIndex[v]; ok {
		t.params |= bit(i)
	}
	if e.active[v] {
		return t
	}
	e.active[v] = true
	for _, d := range e.fi.defs[v] {
		var dt taint
		switch {
		case d.expr != nil && d.pos >= 0:
			dt = e.taintAt(d.expr, d.pos)
		case d.expr != nil:
			dt = e.taint(d.expr)
		default:
			for _, x := range d.implicit {
				dt = dt.or(e.taint(x))
			}
		}
		t = t.or(filterType(v.Type(), dt))
	}
	e.active[v] = false
	return t
}

// taintAt is the taint of element pos of a tuple-valued expression.
func (e *eval) taintAt(x ast.Expr, pos int) taint {
	x = ast.Unparen(x)
	tv := e.info.Types[x]
	var elem types.Type
	if tup, ok := tv.Type.(*types.Tuple); ok && pos < tup.Len() {
		elem = tup.At(pos).Type()
	}
	var t taint
	switch x := x.(type) {
	case *ast.CallExpr:
		if s := e.calleeSummary(x); s != nil {
			e.flow(x) // runs the call's surface checks
			if pos < len(s.returns) {
				t = e.applySummaryReturn(x, s.returns[pos])
			}
		} else {
			t = e.flow(x)
		}
	case *ast.TypeAssertExpr:
		if pos == 0 {
			t = e.taint(x.X)
		}
	case *ast.IndexExpr, *ast.UnaryExpr:
		if pos == 0 {
			t = e.flow(x)
		}
	default:
		t = e.flow(x)
	}
	if elem == nil {
		return t
	}
	return filterType(elem, t).or(e.p.byType(elem))
}

// taint is the taint of an expression's value: what flows into it plus what
// its static type says it is (an error-typed expression is always tainted).
// A tuple-valued call has no value taint of its own; see taintAt.
func (e *eval) taint(x ast.Expr) taint {
	if x == nil {
		return taint{}
	}
	x = ast.Unparen(x)
	return e.flow(x).or(e.p.byType(e.info.TypeOf(x)))
}

// flow is the data-flow part of an expression's taint, memoized per
// evaluation.
func (e *eval) flow(x ast.Expr) taint {
	if x == nil {
		return taint{}
	}
	x = ast.Unparen(x)
	if t, ok := e.memo[x]; ok {
		return t
	}
	e.memo[x] = taint{} // cycle guard
	t := e.compute(x)
	e.memo[x] = t
	return t
}

func (e *eval) compute(x ast.Expr) taint {
	switch x := x.(type) {
	case *ast.BasicLit, *ast.FuncLit:
		return taint{}
	case *ast.Ident:
		if v, ok := e.info.ObjectOf(x).(*types.Var); ok {
			return e.varTaint(v)
		}
		return taint{}
	case *ast.SelectorExpr:
		return e.selector(x)
	case *ast.CallExpr:
		return e.call(x)
	case *ast.CompositeLit:
		// The literal's value carries only what its carrier-typed elements
		// carry: a struct holding an error in an error field is not itself
		// text, and its authored string fields stay clean.
		var t taint
		for _, el := range e.carrierElements(x) {
			t = t.or(e.taint(el.val))
		}
		return t
	case *ast.KeyValueExpr:
		return e.taint(x.Value)
	case *ast.UnaryExpr:
		return e.taint(x.X)
	case *ast.StarExpr:
		return e.taint(x.X)
	case *ast.BinaryExpr:
		switch x.Op {
		case token.EQL, token.NEQ, token.LSS, token.LEQ, token.GTR, token.GEQ, token.LAND, token.LOR:
			return taint{}
		}
		return e.taint(x.X).or(e.taint(x.Y))
	case *ast.IndexExpr:
		return e.taint(x.X)
	case *ast.IndexListExpr:
		return e.taint(x.X)
	case *ast.SliceExpr:
		return e.taint(x.X)
	case *ast.TypeAssertExpr:
		return e.taint(x.X)
	}
	return taint{}
}

// selector: a field of an error value is tainted only by its own type — the
// authored string fields of a ConfigError or execError are safe, an error
// field is not. A field of anything else carries its base's taint.
func (e *eval) selector(x *ast.SelectorExpr) taint {
	sel := e.info.Selections[x]
	if sel == nil {
		// Qualified identifier (pkg.Name).
		if v, ok := e.info.ObjectOf(x.Sel).(*types.Var); ok {
			return e.varTaint(v)
		}
		return taint{}
	}
	if sel.Kind() != types.FieldVal {
		return taint{}
	}
	if e.p.isSource(e.info.TypeOf(x.X)) {
		return e.p.byType(sel.Type())
	}
	return filterType(sel.Type(), e.taint(x.X))
}

// callArgs returns the call's arguments with the receiver first for a method
// value call, matching the callee's parameter numbering.
func (e *eval) callArgs(call *ast.CallExpr) []ast.Expr {
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok {
		if s := e.info.Selections[sel]; s != nil && s.Kind() == types.MethodVal {
			return append([]ast.Expr{sel.X}, call.Args...)
		}
	}
	return call.Args
}

func (e *eval) calleeSummary(call *ast.CallExpr) *summary {
	fn := typeutil.StaticCallee(e.info, call)
	if fn == nil {
		return nil
	}
	return e.p.summaries[fn.Origin()]
}

// argAt maps a callee parameter index to the argument expression(s) feeding
// it; a variadic parameter collects every trailing argument.
func (e *eval) argsFor(call *ast.CallExpr, fn *types.Func, i int) []ast.Expr {
	args := e.callArgs(call)
	sig := fn.Type().(*types.Signature)
	n := sig.Params().Len()
	if sig.Recv() != nil {
		n++
	}
	if sig.Variadic() && i == n-1 {
		if i < len(args) {
			return args[i:]
		}
		return nil
	}
	if i < len(args) {
		return args[i : i+1]
	}
	return nil
}

func (e *eval) applySummaryReturn(call *ast.CallExpr, r taint) taint {
	fn := typeutil.StaticCallee(e.info, call)
	t := taint{real: r.real}
	for i := 0; i < 64 && r.params>>i != 0; i++ {
		if r.params&bit(i) == 0 {
			continue
		}
		for _, a := range e.argsFor(call, fn, i) {
			t = t.or(e.taint(a))
		}
	}
	return t
}

func (e *eval) call(call *ast.CallExpr) taint {
	fun := ast.Unparen(call.Fun)
	if tv, ok := e.info.Types[fun]; ok && tv.IsType() {
		// Conversion: string(b), []byte(s), MyErr(x).
		var t taint
		for _, a := range call.Args {
			t = t.or(e.taint(a))
		}
		return filterType(tv.Type, t)
	}
	result := e.info.TypeOf(call)
	render := e.renderTaint(call)
	if obj, ok := e.info.ObjectOf(calleeIdent(fun)).(*types.Func); ok && e.p.isChokePoint(obj) {
		return taint{}
	}
	if fn := typeutil.StaticCallee(e.info, call); fn != nil {
		if s := e.p.summaries[fn.Origin()]; s != nil {
			if !e.checked[call] {
				e.checked[call] = true
				for i := 0; i < 64 && s.surfaceParams>>i != 0; i++ {
					if s.surfaceParams&bit(i) == 0 {
						continue
					}
					for _, a := range e.argsFor(call, fn, i) {
						if t := e.taint(a); t.any() {
							e.surface(a, t, "argument "+types.ExprString(a)+" of "+fn.Name())
						}
					}
				}
			}
			t := render
			for _, r := range s.returns {
				t = t.or(e.applySummaryReturn(call, r))
			}
			return filterType(result, t)
		}
	}
	// External or dynamic call: every argument's taint reaches the result; a
	// receiver's only through Error()/String(), or when it is not itself a
	// source (a tainted strings.Builder's String()) — a source's other
	// methods (ConfigError.Details) return authored values.
	t := render
	for _, a := range call.Args {
		t = t.or(e.taint(a))
	}
	if sel, ok := fun.(*ast.SelectorExpr); ok {
		if s := e.info.Selections[sel]; s != nil && s.Kind() == types.MethodVal {
			if sel.Sel.Name == "Error" || sel.Sel.Name == "String" || !e.p.isSource(e.info.TypeOf(sel.X)) {
				t = t.or(e.taint(sel.X))
			}
		}
	}
	e.checkWriter(call)
	return filterType(result, t)
}

// checkWriter: anything tainted handed to an external call that also involves
// a net/http.ResponseWriter (http.Error, fmt.Fprintf(w, …), w.Write) is written
// to a response body. Module functions are covered by their summaries instead.
func (e *eval) checkWriter(call *ast.CallExpr) {
	if e.p.rwIface == nil {
		return
	}
	args := e.callArgs(call)
	writer := false
	for _, a := range args {
		if e.p.isWriter(e.info.TypeOf(a)) {
			writer = true
			break
		}
	}
	if !writer {
		return
	}
	for _, a := range args {
		if e.p.isWriter(e.info.TypeOf(a)) {
			continue
		}
		if t := e.taint(a); t.any() {
			e.surface(a, t, "the HTTP response body")
		}
	}
}

// typeName renders t unqualified in its own package and package-name
// qualified elsewhere (a.Record), the way a reader writes it.
func (e *eval) typeName(t types.Type) string {
	return types.TypeString(t, func(p *types.Package) string {
		if p == e.fi.pkg.Types {
			return ""
		}
		return p.Name()
	})
}

func calleeIdent(fun ast.Expr) *ast.Ident {
	switch f := fun.(type) {
	case *ast.Ident:
		return f
	case *ast.SelectorExpr:
		return f.Sel
	case *ast.IndexExpr:
		return calleeIdent(f.X)
	case *ast.IndexListExpr:
		return calleeIdent(f.X)
	}
	return nil
}

func (p *program) isChokePoint(fn *types.Func) bool {
	if fn.Name() != chokePointName {
		return false
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return isClusterPkg(fn.Pkg())
	}
	return isRedactedMethod(fn)
}

func (p *program) isWriter(t types.Type) bool {
	if t == nil || p.rwIface == nil {
		return false
	}
	return types.Implements(t, p.rwIface) || types.Implements(types.NewPointer(t), p.rwIface)
}

// isSource: an error, interface or concrete, directly or behind a pointer.
func (p *program) isSource(t types.Type) bool {
	if t == nil {
		return false
	}
	t = deref(t)
	return types.Implements(t, p.errorIface) || types.Implements(types.NewPointer(t), p.errorIface)
}

// isRendered: a cluster Proposal / Actual / Entity, or a container of them —
// values whose String()/fmt rendering embeds entity keys and data. The values
// themselves are the data plane and flow freely; only their renderings taint.
func (p *program) isRendered(t types.Type) bool { return p.containsRendered(t, map[types.Type]bool{}) }

func (p *program) containsRendered(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch u := t.(type) {
	case *types.Named:
		if p.isClusterType(u, "Proposal", "Actual", "Entity") {
			return true
		}
	case *types.Pointer:
		return p.containsRendered(u.Elem(), seen)
	}
	switch u := t.Underlying().(type) {
	case *types.Pointer:
		return p.containsRendered(u.Elem(), seen)
	case *types.Slice:
		return p.containsRendered(u.Elem(), seen)
	case *types.Array:
		return p.containsRendered(u.Elem(), seen)
	case *types.Map:
		return p.containsRendered(u.Key(), seen) || p.containsRendered(u.Elem(), seen)
	}
	return false
}

// renderTaint: a String() call on a rendered type, or a rendered value handed
// to a fmt formatting function, yields text carrying entity keys and data.
func (e *eval) renderTaint(call *ast.CallExpr) taint {
	if sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr); ok {
		if s := e.info.Selections[sel]; s != nil && s.Kind() == types.MethodVal && sel.Sel.Name == "String" && e.p.isRendered(e.info.TypeOf(sel.X)) {
			return taint{real: true}
		}
	}
	if fn, ok := e.info.ObjectOf(calleeIdent(ast.Unparen(call.Fun))).(*types.Func); ok && fn.Pkg() != nil && fn.Pkg().Path() == "fmt" {
		for _, a := range call.Args {
			if e.p.isRendered(e.info.TypeOf(a)) {
				return taint{real: true}
			}
		}
	}
	return taint{}
}

func (p *program) isClusterType(t types.Type, names ...string) bool {
	n, ok := t.(*types.Named)
	if !ok || !isClusterPkg(n.Obj().Pkg()) {
		return false
	}
	for _, name := range names {
		if n.Obj().Name() == name {
			return true
		}
	}
	return false
}

// byType: a value is tainted by its static type when that type is, or
// contains (slice, array, map, pointer), a source.
func (p *program) byType(t types.Type) taint {
	return taint{real: p.containsSource(t, map[types.Type]bool{})}
}

func (p *program) containsSource(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	if p.isSource(t) {
		return true
	}
	switch u := t.Underlying().(type) {
	case *types.Pointer:
		return p.containsSource(u.Elem(), seen)
	case *types.Slice:
		return p.containsSource(u.Elem(), seen)
	case *types.Array:
		return p.containsSource(u.Elem(), seen)
	case *types.Map:
		return p.containsSource(u.Key(), seen) || p.containsSource(u.Elem(), seen)
	}
	return false
}

// isCarrier: a type that can hold rendered text — string, []byte, an empty
// interface, or a container of those. An error-typed or struct-typed target
// is not a carrier: storing the error itself keeps the chain redactable, and a
// struct is checked at its own literal.
func isCarrier(t types.Type) bool { return carrier(t, map[types.Type]bool{}) }

func carrier(t types.Type, seen map[types.Type]bool) bool {
	if t == nil || seen[t] {
		return false
	}
	seen[t] = true
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Info()&types.IsString != 0
	case *types.Interface:
		return u.NumMethods() == 0
	case *types.Pointer:
		return carrier(u.Elem(), seen)
	case *types.Slice:
		if b, ok := u.Elem().Underlying().(*types.Basic); ok && b.Kind() == types.Byte {
			return true
		}
		return carrier(u.Elem(), seen)
	case *types.Array:
		return carrier(u.Elem(), seen)
	case *types.Map:
		return carrier(u.Key(), seen) || carrier(u.Elem(), seen)
	case *types.Tuple:
		for i := 0; i < u.Len(); i++ {
			if carrier(u.At(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

// filterType drops taint that cannot ride on a value of type t (a bool, an
// int, a duration): text does not survive len() or a comparison.
func filterType(t types.Type, tt taint) taint {
	if t == nil {
		return tt
	}
	if b, ok := t.Underlying().(*types.Basic); ok && b.Info()&types.IsString == 0 && b.Kind() != types.UntypedNil {
		return taint{}
	}
	if tup, ok := t.(*types.Tuple); ok && tup.Len() == 0 {
		return taint{}
	}
	return tt
}

// bit is the parameter bitset member for index i; parameters past the 63rd
// are not tracked.
func bit(i int) uint64 {
	if i < 0 || i >= 64 {
		return 0
	}
	return uint64(1) << i
}

func deref(t types.Type) types.Type {
	if p, ok := t.(*types.Pointer); ok {
		return p.Elem()
	}
	return t
}
