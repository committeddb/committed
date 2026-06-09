//go:build !windows

package db

import "syscall"

// diskUsage reports the free and total byte capacity of the filesystem
// backing path via statfs(2). free uses the blocks available to an
// unprivileged process (Bavail) — the same number `df` reports as available
// — so the watcher's "how close to full are we" signal matches what an
// operator sees. total is the filesystem's full block count (Blocks).
//
// Bsize is int64 on Linux and uint32 on Darwin; converting it to uint64
// before the multiply keeps this one implementation compiling on both. Bavail
// and Blocks are already uint64 on both platforms.
func diskUsage(path string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bsize := uint64(st.Bsize)
	return st.Bavail * bsize, st.Blocks * bsize, nil
}
