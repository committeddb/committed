package loopback

import (
	"fmt"
	"strings"

	"github.com/PaesslerAG/jsonpath"

	"github.com/committeddb/committed/internal/cluster"
)

// SyncableParser parses loopback syncable TOML. Proposer is the write seam
// back into the cluster (the node wires db.DB in); a nil Proposer refuses to
// build rather than producing a sink that fails on first Sync.
type SyncableParser struct {
	Proposer Proposer
}

func (p *SyncableParser) Parse(v *cluster.ParsedConfig, storage cluster.DatabaseStorage) (cluster.Syncable, error) {
	config, err := p.ParseConfig(v)
	if err != nil {
		return nil, err
	}
	if p.Proposer == nil {
		return nil, fmt.Errorf("[loopback.parser] no Proposer wired (node wiring bug)")
	}
	types, ok := storage.(cluster.TypeResolver)
	if !ok {
		return nil, fmt.Errorf("[loopback.parser] storage cannot resolve types")
	}

	// The target type must exist NOW, and its entity kind decides the
	// replay semantics the operator is signing up for. Loud at POST (and at
	// build) — not a per-row failure once the worker runs. A snapshot-kind
	// target converges replays by key; every other kind (including an
	// undeclared one) appends on replay and needs the explicit
	// acknowledgment.
	t, err := types.ResolveType(cluster.LatestTypeRef(config.TargetTopic))
	if err != nil || t == nil {
		return nil, &cluster.FieldError{
			Field: "loopback.target",
			Issue: fmt.Sprintf("target type %q is not declared; POST the type first", config.TargetTopic),
			Err:   err,
		}
	}
	if t.EntityKind != cluster.EntityKindSnapshot && !config.AcknowledgeAppendSemantics {
		return nil, &cluster.FieldError{
			Field: "loopback.target",
			Issue: fmt.Sprintf(
				"target type %q is not snapshot-kind (%s): a replay (crash recovery, re-materialization) APPENDS duplicates to it instead of converging by key. Declare entityKind = \"snapshot\" on the target (recommended), or set loopback.acknowledgeAppendSemantics = true to accept append semantics",
				config.TargetTopic, t.EntityKind),
		}
	}

	return New(p.Proposer, types, config), nil
}

// ParseConfig validates the [loopback] section without touching storage.
func (p *SyncableParser) ParseConfig(v *cluster.ParsedConfig) (*Config, error) {
	source := v.GetString("loopback.topic")
	if source == "" {
		return nil, &cluster.FieldError{Field: "loopback.topic", Issue: "required (the source topic to derive from)"}
	}
	target := v.GetString("loopback.target")
	if target == "" {
		return nil, &cluster.FieldError{Field: "loopback.target", Issue: "required (the derived topic to propose into)"}
	}
	if source == target {
		return nil, &cluster.FieldError{
			Field: "loopback.target",
			Issue: fmt.Sprintf("must differ from loopback.topic (%q): a topic deriving into itself is an infinite consensus loop", source),
		}
	}
	// Re-keying is not supported: the transform preserves the source key so a
	// delete tombstone (which carries ONLY the key) translates through the
	// derivation chain — the RTBF guarantee. Refuse the key loudly rather
	// than silently ignoring it (the admission-validation rule: silent
	// acceptance of an unhonored intent is the dangerous kind).
	if v.GetString("loopback.keyPath") != "" {
		return nil, &cluster.FieldError{
			Field: "loopback.keyPath",
			Issue: "re-keying is not supported: loopback transforms preserve the source key so RTBF deletes chase the derivation chain; remove keyPath",
		}
	}

	var mappings []Mapping
	if err := v.UnmarshalKey("loopback.mappings", &mappings); err != nil {
		return nil, fmt.Errorf("[loopback.parser] parse loopback.mappings: %w", err)
	}
	seen := make(map[string]bool, len(mappings))
	for i, m := range mappings {
		field := strings.TrimSpace(m.Field)
		if field == "" {
			return nil, &cluster.FieldError{
				Field: "loopback.mappings",
				Issue: fmt.Sprintf("mapping %d: field is required", i+1),
			}
		}
		if seen[field] {
			return nil, &cluster.FieldError{
				Field: "loopback.mappings",
				Issue: fmt.Sprintf("field %q is mapped twice", field),
			}
		}
		seen[field] = true
		// Compile at config time so a broken path is a clean 400 here, not a
		// per-row dead-letter once the worker runs (mirrors the sql kinds).
		if m.JsonPath == "" {
			return nil, &cluster.FieldError{
				Field: "loopback.mappings",
				Issue: fmt.Sprintf("mapping for field %q: jsonPath is required", field),
			}
		}
		if _, err := jsonpath.New(m.JsonPath); err != nil {
			return nil, &cluster.FieldError{
				Field: "loopback.mappings",
				Issue: fmt.Sprintf("mapping for field %q: invalid jsonpath %q: %v", field, m.JsonPath, err),
			}
		}
	}

	return &Config{
		SourceTopic:                source,
		TargetTopic:                target,
		Mappings:                   mappings,
		AcknowledgeAppendSemantics: v.GetBool("loopback.acknowledgeAppendSemantics"),
	}, nil
}

// TopicsFromConfig implements cluster.SyncableTopicExtractor: the loopback
// consumes the single topic at loopback.topic. Read straight from the config
// (no build), so dependency enumeration and the derivation-graph checks run
// no I/O.
func (p *SyncableParser) TopicsFromConfig(v *cluster.ParsedConfig) []string {
	topic := v.GetString("loopback.topic")
	if topic == "" {
		return nil
	}
	return []string{topic}
}

// DerivedTopicsFromConfig implements cluster.SyncableDerivedTopicExtractor:
// the loopback produces the single topic at loopback.target. This is the
// edge the derivation-DAG and fan-in guards walk.
func (p *SyncableParser) DerivedTopicsFromConfig(v *cluster.ParsedConfig) []string {
	target := v.GetString("loopback.target")
	if target == "" {
		return nil
	}
	return []string{target}
}
