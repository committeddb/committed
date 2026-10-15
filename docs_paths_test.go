package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDocPathsInMessagesExist pins every docs/….md path that appears inside a
// string literal in non-test Go source — the error, log, and --help text that
// sends an operator to a procedure. Moving or renaming a doc must update those
// messages, or the operator who hits the error lands on nothing.
func TestDocPathsInMessagesExist(t *testing.T) {
	docPath := regexp.MustCompile(`docs/[A-Za-z0-9_./-]+\.md`)
	missing := map[string][]string{}
	seen := 0
	check := func(path string) error {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			for _, p := range docPath.FindAllString(lit.Value, -1) {
				seen++
				if _, err := os.Stat(p); err != nil {
					missing[p] = append(missing[p], path)
				}
			}
			return true
		})
		return nil
	}
	rootFiles, err := filepath.Glob("*.go")
	require.NoError(t, err)
	for _, path := range rootFiles {
		if !strings.HasSuffix(path, "_test.go") {
			require.NoError(t, check(path))
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
			return check(path)
		})
		require.NoError(t, err)
	}
	require.NotZero(t, seen, "no doc paths found in messages: the scan is broken")
	require.Empty(t, missing, "messages name docs that do not exist (path → files)")
}
