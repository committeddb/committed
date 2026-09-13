// Package version exposes the build-time identity of the running
// binary. Version, Commit, and BuildDate are placeholders overridden
// by `-ldflags "-X ..."` in the Makefile; an unstamped `go build`
// yields the "dev"/"unknown" defaults, which is useful in local
// development but should never appear in a release artifact.
package version

import "runtime"

// These variables are overridden at link time. They are not constants
// so the linker's -X flag can set them.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// FeatureLevel is the highest cluster feature level this binary supports. It is
// a monotonic integer — the compatibility axis for *semantic* version skew,
// distinct from the marketing Version string: unlike "an older binary can
// decode the wire", it answers "can this node correctly APPLY a feature's
// entries". Each node self-announces its FeatureLevel (see db.announceVersion),
// and a feature that an older binary would mis-apply gates its emission on the
// cluster-agreed minimum (see db.featureEnabled), so its entries are only ever
// committed once every member can apply them.
//
// Bump this — and add a `featureLevel*` requirement constant at the emitting
// site — whenever you introduce state an older peer cannot correctly apply (a
// new built-in system type, a new semantic marker). NEVER renumber or reuse a
// level; it only ever increases. See docs/api-compatibility.md.
//
// Level 1: the baseline for the first binary carrying this mechanism —
// includes the refresh-boundary marker (featureLevelRefreshBoundary). A binary
// that predates the mechanism announces nothing and is treated as level 0, so
// the gate holds any level-1 emission until every such node is upgraded.
//
// Level 2: the restatement interpretation registry (featureLevelRestatements). The
// Restatement record is a GATED system type — a node that cannot fold restatements must
// not skip them (its syncables would emit stale readings) — so a restatement is
// only admitted once every member announces level 2.
//
// Level 3: zone-pinned syncable ownership (featureLevelZonePinning). The
// NodeZone announcement itself is ungated (an old node skips it safely), but
// ownership RESOLUTION must not activate until every member resolves zones:
// otherwise an old leader (resolving "leader owns everything") and a new
// pinned node would both stream to the same sink — two concurrent writers.
// Every node resolves leader-owns until the cluster minimum reaches 3, and
// a `zone` syncable config is only admitted from level 3.
//
// Level 4: RTBF delete-key erasure (featureLevelRTBFErase). The event-log
// rewrite every replica performs for a Scrub command must be byte-identical
// across nodes; a Scrub carrying HashDeleteKeys additionally rewrites gated
// delete-tombstone keys to the erased sentinel, which an older binary's
// scrubber would not do — diverging the rewritten logs. The proposer sets the
// flag only once the cluster minimum reaches 4, so every member computes the
// same rewrite.
//
// Level 5: canonical uniqueidentifier rendering on SQL Server ingest
// (sqlserver.featureLevelCanonicalUUID). Pre-0.8.0 binaries render a
// uniqueidentifier as the driver's UPPERCASE GUID; from level 5 it renders
// RFC 4122 lowercase — the same bytes PostgreSQL's uuid ingests, so one
// logical UUID keys and joins identically across engines. The spelling is in
// entity KEYS, so a mixed-version cluster must never produce both: every
// node renders the old way until the cluster minimum reaches 5, then a
// session resuming a checkpoint written the old way re-snapshots once at a
// bumped epoch (its closing markers sweep the old spellings on keyed sinks).
// The checkpoint records its rendering; once canonical it stays canonical.
//
// Level 6: the re-materialization verb (db.featureLevelRematerialization).
// The in-progress record itself is ungated (an old node skips it), but the
// replay it drives stamps every re-emitted row with an epoch and ends with
// a sweep that deletes rows below it. An older owner taking over mid-replay
// (a leader-first roll, a one-node rollback) would write rows WITHOUT the
// stamp and advance the checkpoint, and the resuming new owner's sweep would
// delete them — silent row loss on a keyed sink. The verb is refused until
// the cluster minimum reaches 6, so no such owner can exist while a replay
// is in flight.
//
// Level 7: the transaction-scoped ingest dedup record
// (db.featureLevelTxnScopedDedup). A dialect that checkpoints per
// transaction stamps its proposals TxnScopedDedup, and the apply fold then
// writes the ingestable's dedup record with the transaction identity
// appended — a shape a pre-level-7 binary decodes as "nothing seen". An
// older owner taking over such an ingestable (an election mid-roll, a
// rollback) would re-ingest its resume window, and a keyless destination
// keeps those rows twice, permanently. The ingest worker clears the stamp
// while the cluster minimum is below 7, so the record keeps the legacy
// scalar shape every member can read until the roll completes; once an
// ingestable's record has flipped it stays transaction-scoped (the
// transition is one-way per ingestable).
//
// Level 7 also gates the retained type document (db.featureLevelTypeDocument):
// a type entry carries the operator's submitted document for the read-backs
// to return, and a pre-level-7 binary applying that entry re-marshals the
// type from its own struct, dropping the field — members would then disagree
// on what the type reads back as. The document is proposed only once the
// cluster minimum reaches 7; a type written earlier reads back synthesized
// until a document is re-submitted for it.
const FeatureLevel uint64 = 7

// Info is the JSON shape returned by /version and printed by the
// --version flag. GoVersion is derived from runtime rather than
// stamped, since the Go toolchain already records it in the binary.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
	GoVersion string `json:"goVersion"`
}

// Get returns the current build's Info. Intended for use by the HTTP
// handler, the --version flag, and the startup log line so they all
// read from the same source.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
	}
}
