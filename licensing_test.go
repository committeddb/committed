package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestThirdPartyNoticesCoverage asserts THIRD_PARTY_NOTICES.md attributes every
// third-party Go module linked into the committed binary (the non-test
// dependencies of the main package). It fails if the file is missing or has gone
// stale after a dependency change — regenerate with `make third-party-notices`.
// Test-only deps (sqlmock, go-mysql-server, testcontainers) are not linked into
// the binary and so are correctly absent from both sides.
func TestThirdPartyNoticesCoverage(t *testing.T) {
	notices, err := os.ReadFile("THIRD_PARTY_NOTICES.md")
	if err != nil {
		t.Fatalf("read THIRD_PARTY_NOTICES.md (regenerate with `make third-party-notices`): %v", err)
	}
	text := string(notices)

	out, err := exec.Command("go", "list", "-deps",
		"-f", "{{with .Module}}{{.Path}}{{end}}", ".").Output()
	if err != nil {
		t.Fatalf("enumerate binary dependencies via `go list -deps`: %v", err)
	}

	seen := map[string]bool{}
	var missing []string
	for _, path := range strings.Fields(string(out)) {
		if path == "github.com/committeddb/committed" || seen[path] {
			continue
		}
		seen[path] = true
		// Headings are "## <module-path> <version>"; the trailing space anchors
		// the match to a whole module path.
		if !strings.Contains(text, "## "+path+" ") {
			missing = append(missing, path)
		}
	}

	if len(missing) > 0 {
		t.Fatalf("THIRD_PARTY_NOTICES.md is missing %d shipped module(s); regenerate with `make third-party-notices`:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}
