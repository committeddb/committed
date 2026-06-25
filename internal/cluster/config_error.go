package cluster

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// ConfigError wraps a configuration parsing/validation error. HTTP handlers use
// errors.As to detect this type and return 400 instead of 500. When the
// underlying error identifies a specific config field, Field/Issue carry it so
// the handler can surface structured details ({field, issue}) alongside the
// human-readable message.
type ConfigError struct {
	Err error
	// Field is the config path the error is about, e.g. "sql.dialect". Empty
	// when the error isn't tied to one field.
	Field string
	// Issue is the field-scoped problem without the field prefix, e.g.
	// `unknown dialect "x"; valid: mysql, postgres`. Empty when Field is.
	Issue string
}

func (e *ConfigError) Error() string { return e.Err.Error() }
func (e *ConfigError) Unwrap() error { return e.Err }

// NewConfigError wraps a parser/validator error as a ConfigError, lifting field
// context from a *FieldError in the cause chain if one is present. The Propose*
// paths use this so a field-scoped parser error becomes a field-scoped HTTP 400
// without each call site re-deriving the field.
func NewConfigError(err error) *ConfigError {
	ce := &ConfigError{Err: err}
	var fe *FieldError
	if errors.As(err, &fe) {
		ce.Field = fe.Field
		ce.Issue = fe.Issue
	}
	return ce
}

// FieldError is a configuration error scoped to a specific TOML field. Parsers
// return it for the actionable failures — unknown dialect, an
// unresolvable/missing reference, a missing required field — so the field path
// reaches the operator both in the message (`<field>: <issue>`) and as
// structured HTTP error details. Err, when set, is the underlying cause,
// preserved for errors.Is/errors.As.
type FieldError struct {
	Field string
	Issue string
	Err   error
}

func (e *FieldError) Error() string {
	switch {
	case e.Field == "":
		return e.Issue
	case e.Issue == "":
		return e.Field
	default:
		return e.Field + ": " + e.Issue
	}
}

func (e *FieldError) Unwrap() error { return e.Err }

// UnknownDialectError builds a FieldError for a missing or unrecognized
// sql.dialect, listing the registered dialect names so the message is
// self-correcting. valid is the set of registered dialect names (any order; it
// is sorted here for a stable message).
func UnknownDialectError(got string, valid []string) *FieldError {
	sorted := append([]string(nil), valid...)
	sort.Strings(sorted)
	list := strings.Join(sorted, ", ")
	if got == "" {
		return &FieldError{Field: "sql.dialect", Issue: "required; valid dialects: " + list}
	}
	return &FieldError{Field: "sql.dialect", Issue: fmt.Sprintf("unknown dialect %q; valid: %s", got, list)}
}
