//go:build linux || darwin

package durablefs

import (
	"errors"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

func lockDirectory(f *os.File) error {
	fd := f.Fd()
	if fd > math.MaxInt {
		return unix.EBADF
	}
	for {
		err := unix.Flock(int(fd), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return errors.Join(ErrLocked, err)
		}
		return err
	}
}
