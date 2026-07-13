// Package config provides environment-variable interpolation for the
// TOML/JSON configuration operators submit for databases, ingestables,
// and syncables.
//
// The point is to keep secrets — database passwords, replication
// credentials, webhook tokens — out of the Raft log and bbolt. A stored
// config holds ${VAR} templates; each node expands them locally from its
// own environment at parse time. The raw template is what gets proposed
// and persisted, so a proposal dump, an API response, or the bbolt file
// never contains a resolved secret. Rotation is "change the env var and
// restart the node," not "propose a new config."
package config

import (
	"fmt"
	"os"
	"strings"
)

// MissingVarError reports a ${VAR} reference whose variable is not set in
// the environment. It is a distinct type so callers (notably the startup
// re-validation) can treat "operator forgot a secret env var" — which is
// fatal and node-wide — differently from an ordinary malformed-config
// error.
type MissingVarError struct {
	Name string
}

func (e *MissingVarError) Error() string {
	// Interpolation runs on every string value, so this same error fires
	// whether the ${...} is a real secret reference or a literal that
	// happens to live in a non-secret field (a projection default, a
	// webhook body). Name both fixes so the operator doesn't have to
	// guess which case they are in.
	return fmt.Sprintf("environment variable %q referenced in config is not set; "+
		"provide it in this node's environment, or — if ${%s} is meant as literal config text — escape it as $${%s}",
		e.Name, e.Name, e.Name)
}

// lookupFunc resolves a variable name to its value, the bool reporting
// whether it was set (mirroring os.LookupEnv). Injected in tests;
// production callers use os.LookupEnv.
type lookupFunc func(string) (string, bool)

// ExpandString expands ${VAR} references in s against the process
// environment. See expand for the grammar.
func ExpandString(s string) (string, error) {
	return expand(s, os.LookupEnv)
}

// Interpolate expands ${VAR} references in every string value of the
// settings tree, in place, recursing into nested maps and slices. It is
// meant to run on a parsed config tree (viper's AllSettings), never on
// raw TOML/JSON text: interpolating parsed values means a secret can
// contain quotes, newlines, or '=' without any risk of injecting new
// config keys. A reference to an unset variable returns *MissingVarError.
func Interpolate(settings map[string]interface{}) error {
	return interpolateMap(settings, os.LookupEnv)
}

func interpolateMap(m map[string]interface{}, lookup lookupFunc) error {
	for k, v := range m {
		nv, err := interpolateValue(v, lookup)
		if err != nil {
			return err
		}
		m[k] = nv
	}
	return nil
}

func interpolateValue(v interface{}, lookup lookupFunc) (interface{}, error) {
	switch t := v.(type) {
	case string:
		return expand(t, lookup)
	case map[string]interface{}:
		return t, interpolateMap(t, lookup)
	case []interface{}:
		for i, e := range t {
			ne, err := interpolateValue(e, lookup)
			if err != nil {
				return nil, err
			}
			t[i] = ne
		}
		return t, nil
	default:
		return v, nil
	}
}

// expand implements the interpolation grammar:
//
//	${NAME}  expands to the value of variable NAME. An unset NAME is a
//	         hard error (*MissingVarError) so a missing secret fails
//	         loudly rather than silently sending an empty credential.
//	$$       escapes a literal '$', so a password may contain a dollar
//	         sign (e.g. "p$$w0rd" → "p$w0rd").
//	$${NAME} escapes a literal "${NAME}": the leading $$ collapses to a
//	         single '$' and the following "{NAME}" is copied verbatim, so
//	         nothing is expanded. Use this when a config VALUE must hold a
//	         literal "${...}" that is not a secret reference — remember
//	         interpolation runs on every string, not only connection
//	         strings (e.g. "$${HOME}" → "${HOME}").
//	$        any other '$' is preserved verbatim. Bare $VAR shell-style
//	         expansion is deliberately unsupported: ${...} is required so
//	         the syntax never collides with values that just happen to
//	         contain a dollar sign.
func expand(s string, lookup lookupFunc) (string, error) {
	if !strings.ContainsRune(s, '$') {
		return s, nil
	}

	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] != '$' {
			b.WriteByte(s[i])
			i++
			continue
		}
		// s[i] == '$'
		if i+1 < len(s) && s[i+1] == '$' {
			b.WriteByte('$')
			i += 2
			continue
		}
		if i+1 < len(s) && s[i+1] == '{' {
			rel := strings.IndexByte(s[i+2:], '}')
			if rel < 0 {
				// Report the position, never the value: interpolation runs on
				// every string, so s may be a connection string an operator
				// inlined a plaintext password into — %q of it would leak the
				// secret into an HTTP 400 body.
				return "", fmt.Errorf("unterminated ${...} reference at position %d", i)
			}
			name := s[i+2 : i+2+rel]
			if name == "" {
				return "", fmt.Errorf("empty ${} reference at position %d", i)
			}
			val, ok := lookup(name)
			if !ok {
				return "", &MissingVarError{Name: name}
			}
			b.WriteString(val)
			i += 2 + rel + 1
			continue
		}
		// lone '$' — preserved verbatim
		b.WriteByte('$')
		i++
	}
	return b.String(), nil
}
