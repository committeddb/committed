package sqlserver

// SetSnapshotBatchHookForTest installs the per-batch failure-injection hook —
// how the resume tests abort a snapshot mid-enumeration exactly as a dropped
// source connection would. Test-only: export_test.go compiles only for tests,
// exposing the seam without widening the API (the shared dialect pattern).
func (d *SQLServerDialect) SetSnapshotBatchHookForTest(hook func(table string, batch int) error) {
	d.snapshotBatchHook = hook
}
