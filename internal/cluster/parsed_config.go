package cluster

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

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
	observeRead(path)
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
//
// STRICT: a key in the document that no struct field consumes is an
// error naming the key. "The parser accepted it" has to mean something —
// the field incident this guards was probe keys (latestBy, expr, …)
// returning 200 while silently inert, which manufactures belief in
// features that don't exist. A deliberate partial decode (the pipeline
// endpoint peeking topics out of a wide section) uses
// UnmarshalKeyLenient instead — never a config-admission path.
func (c *ParsedConfig) UnmarshalKey(path string, target any) error {
	return c.unmarshalKey(path, target, true)
}

// UnmarshalKeyLenient is UnmarshalKey without the unknown-key check, for
// callers that INTENTIONALLY decode a subset of a section (read-side
// peeks). Config admission must use the strict UnmarshalKey.
func (c *ParsedConfig) UnmarshalKeyLenient(path string, target any) error {
	return c.unmarshalKey(path, target, false)
}

func (c *ParsedConfig) unmarshalKey(path string, target any, strict bool) error {
	v, ok := c.lookup(path)
	if !ok {
		return nil
	}
	cfg := &mapstructure.DecoderConfig{Result: target}
	var md mapstructure.Metadata
	if strict {
		cfg.Metadata = &md
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return err
	}
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if strict && len(md.Unused) > 0 {
		sort.Strings(md.Unused)
		quoted := make([]string, len(md.Unused))
		for i, k := range md.Unused {
			quoted[i] = strconv.Quote(k)
		}
		return fmt.Errorf("unknown key(s) %s under %q — not part of this config's vocabulary (check the spelling against the docs; unknown keys are rejected rather than silently ignored)",
			strings.Join(quoted, ", "), path)
	}
	return nil
}

// SectionKeys returns the immediate child keys of the table at path
// (lowercased — key matching is case-insensitive throughout), or nil when
// the path is absent or not a table. The allowed-key checks for flat
// (Get*-read) config sections diff against this.
func (c *ParsedConfig) SectionKeys(path string) []string {
	m, ok := c.Get(path).(map[string]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, strings.ToLower(k))
	}
	sort.Strings(out)
	return out
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

// RejectUnknownKeys checks the immediate keys of the table at section
// against the vocabulary the parser reads from it — the parser DECLARES
// what it consumes, beside the reads, and everything else is a typo or a
// probe for a feature that does not exist. Matching is case-insensitive
// (the same tolerance lookup has). A key naming a nested table or array
// counts as one key (its inside is the nested decode's concern: strict
// UnmarshalKey for structs, wholesale reads for free-form maps). An absent
// section is fine. The error is a NotAdmissible FieldError naming the key in
// the document's own spelling, with the closest known key when one is near.
func (c *ParsedConfig) RejectUnknownKeys(section string, known ...string) error {
	m, ok := c.Get(section).(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !knownKey(k, known) {
			return NotAdmissible(&FieldError{
				Field: section + "." + k,
				Issue: unknownKeyIssue(k, known, "key"),
			})
		}
	}
	return nil
}

// RejectUnknownSections is RejectUnknownKeys for the document's top-level
// tables: a config kind reads a fixed set of sections ([ingestable] and the
// type's own, say), and a table outside it is a misspelled section.
func (c *ParsedConfig) RejectUnknownSections(known ...string) error {
	keys := make([]string, 0, len(c.values))
	for k := range c.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !knownKey(k, known) {
			return NotAdmissible(&FieldError{
				Field: k,
				Issue: unknownKeyIssue(k, known, "section"),
			})
		}
	}
	return nil
}

func knownKey(k string, known []string) bool {
	for _, want := range known {
		if want != "" && strings.EqualFold(k, want) {
			return true
		}
	}
	return false
}

// unknownKeyIssue phrases the rejection, suggesting the nearest known name
// when the typo is within two edits of one.
func unknownKeyIssue(k string, known []string, what string) string {
	msg := fmt.Sprintf("unknown %s — not part of this config's vocabulary (check the spelling against the docs; unknown %ss are rejected rather than silently ignored)", what, what)
	if near := nearestName(k, known); near != "" {
		msg += fmt.Sprintf("; did you mean %q?", near)
	}
	return msg
}

// nearestName returns the known name within two case-insensitive edits of
// k (insert, delete, or replace one character), or "" when none is close.
func nearestName(k string, known []string) string {
	best, bestDist := "", 3
	lk := strings.ToLower(k)
	for _, want := range known {
		if want == "" {
			continue
		}
		if d := editDistance(lk, strings.ToLower(want)); d < bestDist {
			best, bestDist = want, d
		}
	}
	return best
}

func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

// The read observer makes the keys a parser READS observable, so a test can
// prove they equal the keys it DECLARES (RejectUnknownKeys' vocabulary) —
// the two are otherwise separate specifications that drift silently in one
// direction: a declared key nobody reads is a typo-tolerant hole again.
// Test support only; nil in production, and one observation at a time.
var (
	readObserverMu sync.Mutex
	readObserver   func(path string)
)

func observeRead(path string) {
	readObserverMu.Lock()
	fn := readObserver
	readObserverMu.Unlock()
	if fn != nil {
		fn(path)
	}
}

// ObserveConfigReads runs fn with every ParsedConfig lookup recorded and
// returns, per top-level section, the keys any parser asked for during fn —
// present in the document or not (a parser's vocabulary is what it asks
// for), lowercased, sorted, unique. A lookup of a bare section ("sql", by
// RejectUnknownKeys itself) names no key and is not counted.
func ObserveConfigReads(fn func()) map[string][]string {
	seen := map[string]map[string]bool{}
	readObserverMu.Lock()
	readObserver = func(path string) {
		segs := strings.SplitN(strings.ToLower(path), ".", 3)
		if len(segs) < 2 {
			return
		}
		if seen[segs[0]] == nil {
			seen[segs[0]] = map[string]bool{}
		}
		seen[segs[0]][segs[1]] = true
	}
	readObserverMu.Unlock()
	defer func() {
		readObserverMu.Lock()
		readObserver = nil
		readObserverMu.Unlock()
	}()
	fn()
	out := make(map[string][]string, len(seen))
	for sec, keys := range seen {
		for k := range keys {
			out[sec] = append(out[sec], k)
		}
		sort.Strings(out[sec])
	}
	return out
}

// VocabularyDiff compares a section's declared vocabulary with the keys
// observed being read: undeclared are read but not declared (they would be
// rejected at POST); unread are declared but never read (a typo-tolerant
// hole). Both empty means the declaration is exactly the reads.
func VocabularyDiff(declared, read []string) (undeclared, unread []string) {
	d := map[string]bool{}
	for _, k := range declared {
		if k != "" {
			d[strings.ToLower(k)] = true
		}
	}
	r := map[string]bool{}
	for _, k := range read {
		r[strings.ToLower(k)] = true
	}
	for k := range r {
		if !d[k] {
			undeclared = append(undeclared, k)
		}
	}
	for k := range d {
		if !r[k] {
			unread = append(unread, k)
		}
	}
	sort.Strings(undeclared)
	sort.Strings(unread)
	return undeclared, unread
}
