//go:build !linux && !darwin

package durablefs

import "os"

func lockDirectory(*os.File) error { return ErrUnsupported }
