package metrics

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// repoRoot walks up from the test's working directory (the package dir) to the
// module root (the directory holding go.mod), so the doc-consistency guards below
// don't depend on a brittle relative path.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "reached the filesystem root without finding go.mod")
		dir = parent
	}
}

// TestMetricsDocCatalogComplete guards docs/operations/metrics.md against drift:
// every committed.* instrument registered in this package must appear in the
// catalog, and the doc must describe the OTLP enablement switch. It fails if a
// metric is added without documenting it, or if the doc goes missing — the exact
// gap that let the observability docs rot (no metrics page, undocumented switch).
func TestMetricsDocCatalogComplete(t *testing.T) {
	root := repoRoot(t)

	docBytes, err := os.ReadFile(filepath.Join(root, "docs", "operations", "metrics.md"))
	require.NoError(t, err, "docs/operations/metrics.md must exist")
	doc := string(docBytes)
	require.Contains(t, doc, "OTEL_EXPORTER_OTLP_ENDPOINT",
		"metrics.md must document the enablement switch")

	// Collect committed.* names from this package's non-test source.
	nameRe := regexp.MustCompile(`"(committed\.[a-z_.]+)"`)
	names := map[string]bool{}
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		require.NoError(t, err)
		for _, m := range nameRe.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
		}
	}
	require.NotEmpty(t, names, "expected committed.* metric names in the package source")

	var undocumented []string
	for n := range names {
		if !strings.Contains(doc, n) {
			undocumented = append(undocumented, n)
		}
	}
	require.Empty(t, undocumented,
		"metrics registered in code but missing from docs/operations/metrics.md")
}

// TestDocsUsePrometheusSuffixes guards the PromQL spellings in the docs: the
// OTLP→Prometheus conversion appends _total to a counter and _seconds to a
// histogram in seconds, so a doc naming the bare underscored form points an
// alert at a series that does not exist. Every underscored mention of a
// counter or seconds histogram must carry its suffix.
func TestDocsUsePrometheusSuffixes(t *testing.T) {
	root := repoRoot(t)
	instRe := regexp.MustCompile(`meter\.(Int64Counter|Float64Histogram)\("(committed\.[a-z_.]+)"`)
	suffixed := map[string]string{} // bare underscored name -> its PromQL suffix
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		require.NoError(t, err)
		for _, m := range instRe.FindAllStringSubmatch(string(b), -1) {
			suffix := "_total"
			if m[1] == "Float64Histogram" {
				suffix = "_seconds" // every histogram here is in seconds
			}
			suffixed[strings.ReplaceAll(m[2], ".", "_")] = suffix
		}
	}
	require.NotEmpty(t, suffixed)

	tokenRe := regexp.MustCompile(`\bcommitted_[a-z_]+\b`)
	var wrong []string
	for _, dir := range []string{"docs", filepath.Join("docs", "operations"), filepath.Join("docs", "recipes")} {
		files, err := os.ReadDir(filepath.Join(root, dir))
		require.NoError(t, err)
		for _, f := range files {
			if !strings.HasSuffix(f.Name(), ".md") {
				continue
			}
			b, err := os.ReadFile(filepath.Join(root, dir, f.Name()))
			require.NoError(t, err)
			for _, tok := range tokenRe.FindAllString(string(b), -1) {
				if suffix, ok := suffixed[tok]; ok {
					wrong = append(wrong, filepath.Join(dir, f.Name())+": "+tok+" (PromQL: "+tok+suffix+")")
				}
			}
		}
	}
	require.Empty(t, wrong, "docs name a counter or histogram without its Prometheus suffix")
}

// TestNoStaleScrapeClaim keeps the "Prometheus scrapes each node" misdirection
// from returning to the operations docs — committed exports via OTLP push and
// serves no scrape endpoint.
func TestNoStaleScrapeClaim(t *testing.T) {
	opsDir := filepath.Join(repoRoot(t), "docs", "operations")
	entries, err := os.ReadDir(opsDir)
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".md") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(opsDir, e.Name()))
		require.NoError(t, err)
		require.NotContainsf(t, string(b), "Prometheus scrapes each node",
			"%s claims a scrape model committed does not provide (OTLP push only)", e.Name())
	}
}
