package wal

import (
	"errors"
	"testing"
)

// TestConfigErrorEvidenceRanking pins the evidence contract on the
// degraded-config record store: a success clears only records at or below its
// own evidence strength. Concretely, Parser.Validate passing (a side-effect-free
// prediction) must never erase a recorded live-build failure (the fact) — the
// race that let refreshAfterRestore's async validateConfigSecrets wipe the
// build error RestoreSnapshot had just recorded, leaving the handle missing
// while the gauge read healthy.
func TestConfigErrorEvidenceRanking(t *testing.T) {
	s := &Storage{configErrors: make(map[string]configErr)}
	buildErr := errors.New("dial: bad DSN")
	validateErr := errors.New("missing ${SECRET}")

	// A validate success must not clear a build failure.
	s.recordConfigError("database", "a", configErrBuild, buildErr)
	s.clearConfigError("database", "a", configErrValidate)
	if got := s.ConfigBuildErrorCount(); got != 1 {
		t.Fatalf("validate success cleared a build failure: count = %d, want 1", got)
	}
	// A build success clears it.
	s.clearConfigError("database", "a", configErrBuild)
	if got := s.ConfigBuildErrorCount(); got != 0 {
		t.Fatalf("build success did not clear: count = %d, want 0", got)
	}

	// Validate failure / validate success round-trips.
	s.recordConfigError("syncable", "b", configErrValidate, validateErr)
	s.clearConfigError("syncable", "b", configErrValidate)
	if got := s.ConfigBuildErrorCount(); got != 0 {
		t.Fatalf("validate success did not clear a validate failure: count = %d, want 0", got)
	}

	// A validate re-record on top of a build failure keeps build strength:
	// the weaker checker failing too must not downgrade what it takes to clear.
	s.recordConfigError("database", "c", configErrBuild, buildErr)
	s.recordConfigError("database", "c", configErrValidate, validateErr)
	s.clearConfigError("database", "c", configErrValidate)
	if got := s.ConfigBuildErrorCount(); got != 1 {
		t.Fatalf("validate re-record downgraded a build failure: count = %d, want 1", got)
	}
}
