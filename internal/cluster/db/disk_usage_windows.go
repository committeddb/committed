//go:build windows

package db

// diskUsage has no statfs(2) on Windows. Rather than pull in a Windows-only
// syscall path for a platform committed only cross-compiles for completeness
// (production runs on Linux), the probe returns errDiskWatchUnsupported. The
// watcher logs it once and disables itself, leaving the node fully writable —
// the historical behavior before disk limits existed.
func diskUsage(string) (free, total uint64, err error) {
	return 0, 0, errDiskWatchUnsupported
}
