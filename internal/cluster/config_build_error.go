package cluster

// ConfigBuildError describes a persisted configuration that THIS node
// could not build into a live object — a node-local, ephemeral condition
// (most often a missing ${VAR} secret on this node), not a defect in the
// replicated config bytes themselves. The raw bytes are valid and present
// on every node; only this node's local construction failed.
//
// It is the shared shape between the storage layer (which records the
// degraded set) and the http layer (which serves GET /node/status),
// defined here so the http layer doesn't import the wal package. The
// committed_config_build_errors gauge counts these; GET /node/status
// lists them.
//
// Error names the failing ${VAR} when a secret is unset; it never carries
// an interpolated secret value — interpolation failed, so no value exists.
type ConfigBuildError struct {
	Kind  string
	ID    string
	Error string
}
