package main

import (
	"bufio"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDockerTestsRunInCI pins the build-tag convention that decides whether a
// docker-backed test ever runs in CI. The integration job builds
// `-tags integration`; the only job that passes `-tags docker` is the CDC
// e2e job, and it builds e2e/cdc alone. So a test file tagged `docker` by
// itself anywhere else compiles for nobody but a developer's laptop —
// eighteen dialect files sat that way, the entire SQL Server suite among
// them, until 2026-09-05. Tag docker-backed tests `docker || integration`.
func TestDockerTestsRunInCI(t *testing.T) {
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
		if !strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, filepath.Join("e2e", "cdc")+string(filepath.Separator)) {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		first, _ := bufio.NewReader(f).ReadString('\n')
		if strings.TrimSpace(first) == "//go:build docker" {
			orphans = append(orphans, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(orphans) > 0 {
		t.Fatalf("test files tagged `//go:build docker` outside e2e/cdc never run in CI (no job passes -tags docker there); tag them `//go:build docker || integration`:\n  %s",
			strings.Join(orphans, "\n  "))
	}
}
