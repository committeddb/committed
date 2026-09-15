package redaction_test

import (
	"go/ast"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/tools/go/packages"

	"github.com/committeddb/committed/internal/lint/redaction"
)

// TestFixtures runs the analyzer over testdata/src (a GOPATH-style tree whose
// lintfixture/internal/cluster stubs the domain package — the analyzer keys
// on the import-path suffix, and a path containing "committed" would be
// swallowed by the repo's binary ignore rule) and checks every finding against
// the `// want` comment on its line — a deliberate leak of each shape fails,
// and each redacted shape is clean.
func TestFixtures(t *testing.T) {
	gopath, err := filepath.Abs("testdata")
	require.NoError(t, err)
	cfg := &packages.Config{
		Mode: redaction.LoadMode,
		Dir:  gopath,
		Env:  append(os.Environ(), "GOPATH="+gopath, "GO111MODULE=off", "GOWORK=off", "GOFLAGS="),
	}
	pkgs, err := packages.Load(cfg, "lintfixture/...")
	require.NoError(t, err)
	require.False(t, packages.PrintErrors(pkgs) > 0, "fixture packages must load cleanly")
	require.Len(t, pkgs, 3, "a, b, and the cluster stub")

	// Collect the expectations: file:line -> regexp.
	wants := map[string]*regexp.Regexp{}
	wantRe := regexp.MustCompile("// want `([^`]*)`")
	for _, pkg := range pkgs {
		for _, f := range pkg.Syntax {
			for _, cg := range f.Comments {
				for _, c := range cg.List {
					m := wantRe.FindStringSubmatch(c.Text)
					if m == nil {
						continue
					}
					pos := pkg.Fset.Position(c.Pos())
					wants[key(pos.Filename, pos.Line)] = regexp.MustCompile(m[1])
				}
			}
		}
	}
	require.NotEmpty(t, wants)

	matched := map[string]bool{}
	for _, f := range redaction.Analyze(pkgs) {
		k := key(f.Pos.Filename, f.Pos.Line)
		re, ok := wants[k]
		if !ok {
			t.Errorf("unexpected finding: %s", f)
			continue
		}
		if !re.MatchString(f.Message) {
			t.Errorf("finding %s does not match want %q", f, re)
		}
		matched[k] = true
	}
	for k, re := range wants {
		if !matched[k] {
			t.Errorf("expected a finding at %s matching %q", k, re)
		}
	}
}

// TestNoUnredactedTextReachesASurface is the tripwire: every persisted or exposed
// surface under ./internal routes error text through cluster.RedactedMessage.
// A finding names the line to fix; the fix is to route the text through the
// choke point (redactedMessage/redactedDetail in db/http, safeDeadLetterMessage
// in db), never to bake err.Error() into a record or a response.
func TestNoUnredactedTextReachesASurface(t *testing.T) {
	root := moduleRoot(t)
	cfg := &packages.Config{Mode: redaction.LoadMode, Dir: root}
	pkgs, err := packages.Load(cfg, "./internal/...", "./cmd/...")
	require.NoError(t, err)
	require.False(t, packages.PrintErrors(pkgs) > 0, "module packages must load cleanly")
	require.NotEmpty(t, pkgs)

	files := 0
	for _, pkg := range pkgs {
		for _, f := range pkg.Syntax {
			if !ast.IsGenerated(f) {
				files++
			}
		}
	}
	require.Greater(t, files, 100, "the load must cover the module, not a stub")

	findings := redaction.Analyze(pkgs)
	lines := make([]string, 0, len(findings))
	for _, f := range findings {
		rel, err := filepath.Rel(root, f.Pos.Filename)
		require.NoError(t, err)
		lines = append(lines, rel+":"+itoa(f.Pos.Line)+": "+f.Message)
	}
	require.Empty(t, findings, "unredacted surfaces:\n  "+strings.Join(lines, "\n  "))
}

func key(file string, line int) string { return file + ":" + itoa(line) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod above %s", dir)
		dir = parent
	}
}
