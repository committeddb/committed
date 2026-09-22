package tidwall

import (
	"errors"

	native "github.com/tidwall/wal"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
)

// LegacyCompression exposes native background compression without transferring
// ownership of the handle. Closing/replacing the native handle may race a step;
// ErrClosed lets the owner retry against its replacement.
type LegacyCompression struct{ Log *native.Log }

var _ eventlog.SealedCompressor = LegacyCompression{}

func (c LegacyCompression) CompressNextSealed() (bool, error) {
	if c.Log == nil {
		return false, eventlog.ErrInvalid
	}
	did, err := c.Log.CompressNextSealed()
	if errors.Is(err, native.ErrClosed) {
		err = errors.Join(eventlog.ErrClosed, err)
	}
	return did, err
}
