package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/pelletier/go-toml/v2"
)

// ParsedConfig is the decoded form of a Configuration's TOML/JSON
// payload — the document handed to every DatabaseParser /
// IngestableParser / SyncableParser. It exists so the decode pipeline
// is committed's own seam rather than a third-party type in the parser
// contract.
//
// Matching semantics, deliberately split two ways:
//
//   - Committed's field names match case-insensitively: `Topic =`,
//     `[SQL]`, `PRIMARYKEY =` all keep working. Stored configs are
//     re-parsed from the log on every node restart, so this tolerance
//     (inherited from the previous viper pipeline) is load-bearing
//     backward compatibility, pinned by the tolerance_test.go corpus
//     across the parser packages.
//
//   - User data is preserved byte-exact — including map keys. The
//     previous pipeline lowercased every map key at read time, which
//     silently corrupted user-supplied strings in key position (a
//     jsonpath like $.eventType became $.eventtype). With this seam,
//     config shapes may carry user data in keys again; tolerance comes
//     from case-insensitive *matching* at lookup time, never from
//     rewriting the data.
type ParsedConfig struct {
	values map[string]any
}

// ParseConfigBytes decodes a config payload by mime type: TOML by
// default, JSON for "application/json" (both shapes the HTTP API
// accepts). It only decodes — ${VAR} secret interpolation is the
// caller's concern (the db/parser package runs it at the parse
// boundary; type configs deliberately skip it).
func ParseConfigBytes(mimeType string, data []byte) (*ParsedConfig, error) {
	values := map[string]any{}
	if mimeType == "application/json" {
		if err := json.Unmarshal(data, &values); err != nil {
			return nil, err
		}
		return &ParsedConfig{values: values}, nil
	}
	if err := toml.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return &ParsedConfig{values: values}, nil
}

// Values exposes the root of the decoded tree, mutable in place — the
// hook the parse boundary uses to run ${VAR} interpolation before any
// sub-parser reads a value.
func (c *ParsedConfig) Values() map[string]any {
	return c.values
}

// lookup walks a dotted path ("sql.topic") through nested maps. Each
// segment matches its map key exactly first, then case-insensitively
// (sorted order breaks ties deterministically) — tolerant on
// committed's key names without rewriting anyone's data.
func (c *ParsedConfig) lookup(path string) (any, bool) {
	var current any = c.values
	for _, segment := range strings.Split(path, ".") {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		v, ok := m[segment]
		if !ok {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if strings.EqualFold(k, segment) {
					v, ok = m[k], true
					break
				}
			}
			if !ok {
				return nil, false
			}
		}
		current = v
	}
	return current, true
}

// IsSet reports whether the path is present in the document. Presence,
// not truthiness: `none = false` is set.
func (c *ParsedConfig) IsSet(path string) bool {
	_, ok := c.lookup(path)
	return ok
}

// Get returns the raw decoded value at path (nil when absent). Nested
// tables are map[string]any, arrays are []any, TOML integers are
// int64, JSON numbers are float64 — the same shapes encoding/json and
// go-toml produce.
func (c *ParsedConfig) Get(path string) any {
	v, _ := c.lookup(path)
	return v
}

// GetString returns the value at path coerced to a string ("" when
// absent). Coercions mirror the previous pipeline: numbers and bools
// stringify, everything else must already be a string.
func (c *ParsedConfig) GetString(path string) string {
	v, ok := c.lookup(path)
	if !ok {
		return ""
	}
	return toString(v)
}

// GetInt returns the value at path coerced to an int (0 when absent or
// not coercible): TOML int64, JSON float64, numeric strings, bools.
func (c *ParsedConfig) GetInt(path string) int {
	switch v := c.Get(path).(type) {
	case int64:
		return int(v)
	case int:
		return v
	case float64:
		return int(v)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return 0
		}
		return n
	case bool:
		if v {
			return 1
		}
	}
	return 0
}

// GetBool returns the value at path coerced to a bool (false when
// absent or not coercible): bools, strconv-style strings, non-zero
// numbers.
func (c *ParsedConfig) GetBool(path string) bool {
	switch v := c.Get(path).(type) {
	case bool:
		return v
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return false
		}
		return b
	case int64:
		return v != 0
	case int:
		return v != 0
	case float64:
		return v != 0
	}
	return false
}

// GetStringSlice returns the value at path as a string slice (nil when
// absent): arrays element-wise stringified; a bare string splits on
// whitespace (previous-pipeline compatibility).
func (c *ParsedConfig) GetStringSlice(path string) []string {
	switch v := c.Get(path).(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			out = append(out, toString(e))
		}
		return out
	case string:
		return strings.Fields(v)
	}
	return nil
}

// GetStringMapString returns the table at path with values stringified
// and keys lowercased (empty map when absent). The lowercasing is
// deliberate previous-pipeline compatibility: these tables carry
// committed/dialect option names, which consumers index by their
// canonical lowercase form — never user data.
func (c *ParsedConfig) GetStringMapString(path string) map[string]string {
	out := map[string]string{}
	m, ok := c.Get(path).(map[string]any)
	if !ok {
		return out
	}
	for k, v := range m {
		out[strings.ToLower(k)] = toString(v)
	}
	return out
}

// UnmarshalKey decodes the subtree at path into target via
// mapstructure (the `mapstructure` tags on config structs), matching
// field names case-insensitively while leaving the data itself —
// values and any user-supplied map keys — untouched. An absent path
// decodes nothing and returns nil.
func (c *ParsedConfig) UnmarshalKey(path string, target any) error {
	v, ok := c.lookup(path)
	if !ok {
		return nil
	}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{Result: target})
	if err != nil {
		return err
	}
	return decoder.Decode(v)
}

func toString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}
