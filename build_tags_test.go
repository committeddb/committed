package main

import (
	"bufio"
	"go/build/constraint"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ciJob is one CI job's compile scope: the directory prefix it builds and
// the build tags it passes. Kept in step with .github/workflows/ci.yml and
// the Makefile targets it calls.
type ciJob struct {
	name   string
	prefix string // "" = the whole module
	tags   []string
}

var ciJobs = []ciJob{
	{"race (make test/ci)", "", nil},
	{"integration", "", []string{"integration"}},
	{"cdc", "e2e/cdc/", []string{"docker"}},
	{"upgrade", "e2e/upgrade/", []string{"upgrade"}},
	{"backup", "e2e/backup/", []string{"backup"}},
	{"multinode", "e2e/multinode/", []string{"multinode"}},
	{"adversarial", "internal/cluster/db/", []string{"adversarial"}},
}

// TestEveryTestFileCompilesInSomeCIJob pins the build-tag convention that
// decides whether a test ever runs in CI. Each job compiles one directory
// scope with one tag set; a test file whose build constraint no job
// satisfies compiles for nobody but a developer's laptop. Eighteen dialect
// files tagged `docker` alone — the entire SQL Server suite among them —
// sat that way until 2026-09-05. Tag docker-backed tests
// `docker || integration`; any other spelling has to name a tag a job
// passes, in a directory that job builds.
func TestEveryTestFileCompilesInSomeCIJob(t *testing.T) {
	var orphans []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "third_party", "node_modules", ".claude-scratch", ".worktrees":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		expr, err := buildConstraint(path)
		if err != nil {
			return err
		}
		if expr == nil {
			return nil // unconstrained: the race job compiles it
		}
		for _, job := range ciJobs {
			if job.prefix != "" && !strings.HasPrefix(filepath.ToSlash(path), job.prefix) {
				continue
			}
			if expr.Eval(func(tag string) bool {
				for _, have := range job.tags {
					if tag == have {
						return true
					}
				}
				return false // GOOS/GOARCH tags read false: a `!windows` file counts as covered
			}) {
				return nil
			}
		}
		orphans = append(orphans, path+"  ("+expr.String()+")")
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(orphans) > 0 {
		t.Fatalf("test files whose build constraint no CI job satisfies (see ciJobs; tag docker-backed tests `docker || integration`):\n  %s",
			strings.Join(orphans, "\n  "))
	}
}

// TestCIJobsMatchTheMakefileAndCI keeps ciJobs honest end to end: each entry
// must correspond to a Make target whose `go test` invocation passes exactly
// that tag set over that directory scope, and CI (.github/workflows/ci.yml)
// must call that target. A job renamed, retagged, re-scoped, or dropped from
// the workflow fails here instead of silently leaving files uncovered. Exact
// tag sets, not substrings: the local test-all target passes
// "docker integration" over the module and must not stand in for the
// integration job.
func TestCIJobsMatchTheMakefileAndCI(t *testing.T) {
	mk, err := os.ReadFile("Makefile")
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	ci, err := os.ReadFile(filepath.Join(".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read ci.yml: %v", err)
	}
	// target -> its `go test` invocations.
	invocations := map[string][]string{}
	target := ""
	for _, line := range strings.Split(string(mk), "\n") {
		if !strings.HasPrefix(line, "\t") && strings.Contains(line, ":") && !strings.HasPrefix(line, "#") {
			target = strings.TrimSpace(strings.SplitN(line, ":", 2)[0])
			continue
		}
		if trimmed := strings.TrimSpace(line); strings.HasPrefix(trimmed, "go test") {
			invocations[target] = append(invocations[target], trimmed)
		}
	}
	for _, job := range ciJobs {
		scope := "./..."
		if job.prefix != "" {
			scope = "./" + strings.TrimSuffix(job.prefix, "/") + "/..."
		}
		want := tagSet(job.tags)
		var matching []string
		for tgt, invs := range invocations {
			for _, inv := range invs {
				if invocationTags(inv) == want && hasToken(inv, scope) {
					matching = append(matching, tgt)
				}
			}
		}
		if len(matching) == 0 {
			t.Errorf("ciJobs entry %q (tags %v, scope %s): no Make target runs `go test` with exactly those tags over that scope", job.name, job.tags, scope)
			continue
		}
		called := false
		for _, tgt := range matching {
			if strings.Contains(string(ci), "make "+tgt) {
				called = true
			}
		}
		if !called {
			t.Errorf("ciJobs entry %q: Make target(s) %v match it, but ci.yml calls none of them", job.name, matching)
		}
	}
}

// tagSet renders a tag list as a canonical space-joined, sorted string.
func tagSet(tags []string) string {
	sorted := append([]string(nil), tags...)
	sort.Strings(sorted)
	return strings.Join(sorted, " ")
}

// invocationTags extracts the -tags value of a `go test` line as a tagSet
// ("" when the line passes no tags).
func invocationTags(inv string) string {
	fields := strings.Fields(inv)
	for i, f := range fields {
		if f != "-tags" || i+1 >= len(fields) {
			continue
		}
		raw := fields[i+1]
		if strings.HasPrefix(raw, "\"") { // -tags "a b": rejoin the quoted run
			for j := i + 1; j < len(fields); j++ {
				if strings.HasSuffix(fields[j], "\"") {
					raw = strings.Join(fields[i+1:j+1], " ")
					break
				}
			}
		}
		return tagSet(strings.Fields(strings.Trim(raw, "\"")))
	}
	return ""
}

func hasToken(inv, token string) bool {
	for _, f := range strings.Fields(inv) {
		if f == token {
			return true
		}
	}
	return false
}

// buildConstraint returns the file's //go:build expression, or nil when it
// has none. Only the header (before the package clause) is read.
func buildConstraint(path string) (constraint.Expr, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if strings.HasPrefix(line, "package ") {
			return nil, nil
		}
		if constraint.IsGoBuild(line) {
			return constraint.Parse(line)
		}
	}
	return nil, sc.Err()
}
