package cluster

import "errors"

var (
	ErrResourceNotFound = errors.New("resource not found")
	ErrVersionNotFound  = errors.New("version not found")
)

// VersionInfo holds metadata about a single version of a versioned resource.
type VersionInfo struct {
	Version uint64 `json:"version"`
	Current bool   `json:"current"`
}
