package durablefs

import (
	"io"
	"os"
)

// CaptureFile offers exactly size bytes from path. The owner keeps that prefix
// immutable until visit returns. visit must consume write synchronously.
func CaptureFile(path, name string, size int64, visit func(string, int64, func(io.Writer) error) error) error {
	return visit(name, size, func(w io.Writer) error {
		f, err := os.Open(path) // #nosec G304 -- Caller supplies an owned storage file.
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.CopyN(w, f, size)
		return err
	})
}
