package postgres

import "time"

// SetSnapshotBatchHookForTest installs the test-only failure-injection seam
// called before each snapshot batch read. It lets the resume-after-transient-
// error tests abort a snapshot mid-enumeration exactly as a dropped source
// connection would. Test-only; production code never sets the hook.
func (d *PostgreSQLDialect) SetSnapshotBatchHookForTest(h func(table string, batch int) error) {
	d.snapshotBatchHook = h
}

// SetPartitionPollIntervalForTest shortens how often a streaming worker reads
// its watched tables' partitions, returning a restore func.
func SetPartitionPollIntervalForTest(d time.Duration) func() {
	prev := partitionPollInterval
	partitionPollInterval = d
	return func() { partitionPollInterval = prev }
}
