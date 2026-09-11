package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/committeddb/committed/internal/cluster"
)

// Options is the ingest [sql.options] table: committed's own knobs on a
// source, typed and closed like every other section (a key outside the
// vocabulary, or one the configured dialect does not read, is refused at
// POST rather than silently ignored). Each dialect declares which of these
// it reads (Dialect.OptionKeys), beside the reads.
type Options struct {
	// SlotName and Publication name the Postgres logical-replication slot
	// and publication committed creates for the ingestable (and drops on
	// delete). Give each ingestable its own. Defaults: committed_slot,
	// committed_pub.
	SlotName    string
	Publication string
	// PollInterval is the SQL Server Change Tracking poll cadence: read
	// models trail the source by roughly this much at rest. Default 3s.
	PollInterval time.Duration
	// BatchSize is the rows per snapshot keyset batch; 0 means the dialect's
	// default.
	BatchSize int
	// SnapshotReaders is the number of parallel primary-key-range readers
	// per table during a snapshot (MySQL). 1, the default, is the single
	// stream; the ceiling is MaxSnapshotReaders.
	SnapshotReaders int
}

// The vocabulary, as the TOML spells it.
const (
	OptionSlotName        = "slotName"
	OptionPublication     = "publication"
	OptionPollInterval    = "pollInterval"
	OptionBatchSize       = "batchSize"
	OptionSnapshotReaders = "snapshotReaders"
)

var optionKeys = []string{OptionSlotName, OptionPublication, OptionPollInterval, OptionBatchSize, OptionSnapshotReaders}

// renamedOptions maps the pre-0.8.0 spellings to their 0.8.0 names, so a
// config carrying one parks with the rename instead of a bare unknown-key
// error.
var renamedOptions = map[string]string{
	"slot_name":        OptionSlotName,
	"poll_interval":    OptionPollInterval,
	"batch_size":       OptionBatchSize,
	"snapshot_readers": OptionSnapshotReaders,
}

// removedOptionTables are the pre-0.8.0 [sql.<dialect>] spellings of
// [sql.options].
var removedOptionTables = []string{"postgres", "mysql", "sqlserver"}

// rawOptions is the TOML decode shape: strict (an unused key fails the
// decode), durations as strings like every other duration key.
type rawOptions struct {
	SlotName        string `mapstructure:"slotName"`
	Publication     string `mapstructure:"publication"`
	PollInterval    string `mapstructure:"pollInterval"`
	BatchSize       int    `mapstructure:"batchSize"`
	SnapshotReaders int    `mapstructure:"snapshotReaders"`
}

// BatchSizeOr returns the configured batch size, or def when unset.
func (o Options) BatchSizeOr(def int) int {
	if o.BatchSize > 0 {
		return o.BatchSize
	}
	return def
}

// PollIntervalOr returns the configured poll cadence, or def when unset.
func (o Options) PollIntervalOr(def time.Duration) time.Duration {
	if o.PollInterval > 0 {
		return o.PollInterval
	}
	return def
}

// Readers returns the snapshot reader count, 1 (the single stream) when
// unset — so a hand-built Config behaves like a parsed one.
func (o Options) Readers() int {
	if o.SnapshotReaders < 1 {
		return 1
	}
	return o.SnapshotReaders
}

// rejectRemovedOptionTables refuses the pre-0.8.0 [sql.<dialect>] tables
// with the rename, before the generic unknown-key rejection would name them
// as typos.
func rejectRemovedOptionTables(v *cluster.ParsedConfig) error {
	// The [sql] table's own keys, not dotted probes: a removed spelling is
	// not part of the vocabulary, so it must not register as a read of it.
	m, ok := v.Get("sql").(map[string]any)
	if !ok {
		return nil
	}
	for k := range m {
		for _, name := range removedOptionTables {
			if !strings.EqualFold(k, name) {
				continue
			}
			return cluster.NotAdmissible(&cluster.FieldError{
				Field: "sql." + k,
				Issue: fmt.Sprintf("the [sql.%s] table was removed in 0.8.0 (it was the older spelling of [sql.options]): move its keys to [sql.options] under their 0.8.0 names (%s), then re-POST the config — the ingestable resumes from its checkpoint", name, renameList()),
			})
		}
	}
	return nil
}

func renameList() string {
	parts := make([]string, 0, len(renamedOptions))
	for _, old := range []string{"slot_name", "poll_interval", "batch_size", "snapshot_readers"} {
		parts = append(parts, old+" → "+renamedOptions[old])
	}
	return strings.Join(parts, ", ")
}

// parseOptions reads [sql.options]: the removed spellings park with the
// rename, the vocabulary is closed, every value is typed and validated, and
// a key the configured dialect does not read is refused rather than
// accepted and ignored.
func parseOptions(v *cluster.ParsedConfig, dialectName string, dialect Dialect) (Options, error) {
	if err := rejectRemovedOptionTables(v); err != nil {
		return Options{}, err
	}
	if m, ok := v.Get("sql.options").(map[string]any); ok {
		for k := range m {
			if renamed, was := renamedOptions[strings.ToLower(k)]; was {
				return Options{}, cluster.NotAdmissible(&cluster.FieldError{
					Field: "sql.options." + k,
					Issue: fmt.Sprintf("renamed in 0.8.0: spell it %q, then re-POST the config — the ingestable resumes from its checkpoint", renamed),
				})
			}
		}
	}
	if err := v.RejectUnknownKeys("sql.options", optionKeys...); err != nil {
		return Options{}, err
	}
	var raw rawOptions
	if err := v.UnmarshalKey("sql.options", &raw); err != nil {
		issue, _ := cluster.RedactedMessage(err)
		return Options{}, cluster.NotAdmissible(&cluster.FieldError{Field: "sql.options", Issue: issue})
	}

	reads := dialect.OptionKeys()
	for _, key := range optionKeys {
		if !v.IsSet("sql.options."+key) || knownOption(key, reads) {
			continue
		}
		return Options{}, cluster.NotAdmissible(&cluster.FieldError{
			Field: "sql.options." + key,
			Issue: fmt.Sprintf("not read by the %s dialect (its options: %s); remove it rather than rely on a setting that would never take effect", dialectName, strings.Join(reads, ", ")),
		})
	}

	out := Options{SlotName: raw.SlotName, Publication: raw.Publication, BatchSize: raw.BatchSize, SnapshotReaders: raw.SnapshotReaders}
	if v.IsSet("sql.options."+OptionBatchSize) && raw.BatchSize < 1 {
		return Options{}, cluster.NotAdmissible(&cluster.FieldError{Field: "sql.options." + OptionBatchSize, Issue: "must be a positive integer"})
	}
	if v.IsSet("sql.options." + OptionSnapshotReaders) {
		if raw.SnapshotReaders < 1 || raw.SnapshotReaders > MaxSnapshotReaders {
			return Options{}, cluster.NotAdmissible(&cluster.FieldError{
				Field: "sql.options." + OptionSnapshotReaders,
				Issue: fmt.Sprintf("must be between 1 and %d (each reader holds a source connection running a range scan)", MaxSnapshotReaders),
			})
		}
	}
	if raw.PollInterval != "" {
		d, err := time.ParseDuration(raw.PollInterval)
		if err != nil || d <= 0 {
			return Options{}, cluster.NotAdmissible(&cluster.FieldError{
				Field: "sql.options." + OptionPollInterval,
				Issue: fmt.Sprintf("must be a positive Go duration (e.g. \"3s\"): %q", raw.PollInterval),
			})
		}
		out.PollInterval = d
	}
	return out, nil
}

func knownOption(key string, reads []string) bool {
	for _, r := range reads {
		if r == key {
			return true
		}
	}
	return false
}
