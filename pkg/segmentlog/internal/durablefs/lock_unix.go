//go:build linux || darwin

package durablefs

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func lockDirectory(f *os.File) error {
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return errors.Join(ErrLocked, err)
		}
		return err
	}
}
