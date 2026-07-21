package wal

import (
	"errors"
	"testing"
)

// TestSweepConfigErrorsExcept pins the compacted-delete stale-record fix: a
// degraded-config record whose id is no longer in the config bucket is swept
// by the reconcile/load paths (nothing else ever re-checks a deleted id, so a
// survivor would overcount committed_config_build_errors forever — the
// apply-path delete clear cannot fire for a delete learned via
// InstallSnapshot). Records for ids still present — including still-degraded
// ones — survive, as do records of OTHER kinds.
func TestSweepConfigErrorsExcept(t *testing.T) {
	s := &Storage{configErrors: make(map[string]configErr)}
	err := errors.New("missing ${SECRET}")
	s.recordConfigError("syncable", "gone", configErrBuild, err)
	s.recordConfigError("syncable", "kept", configErrBuild, err)
	s.recordConfigError("ingestable", "gone", configErrBuild, err)

	s.sweepConfigErrorsExcept("syncable", map[string]struct{}{"kept": {}})

	if got := s.ConfigBuildErrorCount(); got != 2 {
		t.Fatalf("count = %d, want 2 (syncable/gone swept; syncable/kept and ingestable/gone survive)", got)
	}
	s.configErrMu.Lock()
	_, keptOK := s.configErrors["syncable/kept"]
	_, otherKindOK := s.configErrors["ingestable/gone"]
	_, goneOK := s.configErrors["syncable/gone"]
	s.configErrMu.Unlock()
	if !keptOK || !otherKindOK || goneOK {
		t.Fatalf("sweep touched the wrong records: kept=%v otherKind=%v gone=%v", keptOK, otherKindOK, goneOK)
	}
}
