package mysql

// SetMaxPendingEntitiesForTest lowers the per-flush soft limit so a test can
// force several partial flushes out of one small RowsEvent (instead of needing a
// giant event), and returns a restore func. Test-only: export_test.go is compiled
// solely for tests, so this exposes the internal knob without widening the API.
func SetMaxPendingEntitiesForTest(n int) func() {
	orig := maxPendingEntities
	maxPendingEntities = n
	return func() { maxPendingEntities = orig }
}
