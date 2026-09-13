package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEveryNodeSettingIsDocumented pins the environment contract: every
// COMMITTED_* variable the binary reads — a quoted full name in non-test Go
// source at the repo root, under cmd/, or under internal/ — is documented by
// name in the README or under docs/. `committed node --help` lists the
// common settings and points at those pages for the rest; this is what
// keeps "the rest" true when a knob is added.
func TestEveryNodeSettingIsDocumented(t *testing.T) {
	quoted := regexp.MustCompile(`"(COMMITTED_[A-Z0-9_]+)"`)
	settings := map[string]bool{}
	collect := func(path string) error {
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range quoted.FindAllStringSubmatch(string(src), -1) {
			if strings.HasSuffix(m[1], "_") {
				continue // a prefix in a message ("COMMITTED_TLS_*"), not a setting
			}
			settings[m[1]] = true
		}
		return nil
	}
	rootFiles, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, path := range rootFiles {
		if !strings.HasSuffix(path, "_test.go") {
			require.NoError(t, collect(path))
		}
	}
	for _, root := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "third_party" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			return collect(path)
		})
		require.NoError(t, err)
	}
	require.NotEmpty(t, settings)

	var docs strings.Builder
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)
	docs.Write(readme)
	err = filepath.WalkDir("docs", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		page, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		docs.Write(page)
		docs.WriteByte('\n')
		return nil
	})
	require.NoError(t, err)

	var undocumented []string
	for s := range settings {
		// Whole-name match: a shorter setting must not pass on the strength
		// of a longer one that merely starts with it.
		if !regexp.MustCompile(`\b` + regexp.QuoteMeta(s) + `\b`).MatchString(docs.String()) {
			undocumented = append(undocumented, s)
		}
	}
	sort.Strings(undocumented)
	require.Empty(t, undocumented, "settings the binary reads that no page names — document each in README.md or under docs/")
}
