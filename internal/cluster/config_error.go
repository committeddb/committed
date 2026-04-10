package cluster

// ConfigError wraps a configuration parsing error. HTTP handlers use
// errors.As to detect this type and return 400 instead of 500.
type ConfigError struct {
	Err error
}

func (e *ConfigError) Error() string { return e.Err.Error() }
func (e *ConfigError) Unwrap() error { return e.Err }
