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
// level; it only ever increases.
//
// Level 1: the baseline for the first binary carrying this mechanism —
// includes the refresh-boundary marker (featureLevelRefreshBoundary). A binary
// that predates the mechanism announces nothing and is treated as level 0, so
// the gate holds any level-1 emission until every such node is upgraded.
//
// Level 2: every capability 0.8.0 ships. Features that ship in one release
// share its level — a 0.7.x member cannot tell them apart, and a level is
// never renumbered — so each names its own constant but all resolve to 2.
// What each gate protects:
//
//   - Restatements (db.featureLevelRestatements): the Restatement record is a
//     GATED system type — a node that cannot fold restatements must not skip
//     them (its syncables would emit stale readings).
//   - Zone pinning (db.featureLevelZonePinning): the NodeZone announcement is
//     ungated, but ownership RESOLUTION must not activate until every member
//     resolves zones, or an old leader (resolving "leader owns everything") and
//     a new pinned node would both stream to one destination. Every node
//     resolves leader-owns until the minimum reaches 2.
//   - RTBF delete-key erasure (db.featureLevelRTBFErase): every replica's
//     scrub rewrite must be byte-identical; a Scrub carrying HashDeleteKeys
//     rewrites tombstone keys an older scrubber would leave, diverging logs.
//   - Canonical uniqueidentifier rendering on SQL Server ingest
//     (sqlserver.featureLevelCanonicalUUID): the spelling is in entity KEYS,
//     so a mixed cluster must never produce both; every node renders the old
//     way until the minimum reaches 2, then a session resuming an old-style
//     checkpoint re-snapshots once (its closing markers sweep the old
//     spellings on keyed destinations). Once canonical it stays canonical.
//   - Re-materialization (db.featureLevelRematerialization): the replay stamps
//     every re-emitted row with an epoch and ends with a sweep below it; an
//     older owner taking over mid-replay would write unstamped rows the sweep
//     then deletes. Refused until no such owner can exist.
//   - Transaction-scoped ingest dedup (db.featureLevelTxnScopedDedup): the
//     record gains the transaction identity, a shape an older binary decodes
//     as "nothing seen" and would re-ingest — twice, permanently, on a keyless
//     destination. The stamp is cleared below the level; once a record flips
//     it stays transaction-scoped.
//   - The fields 0.8.0 added to two replicated records: their apply paths
//     unmarshal into the binary's own struct and re-marshal, so a field the
//     applying binary does not know is DROPPED, permanently and per member.
//     The type record (db.featureLevelTypeRecord) clears the submitted
//     document below the level and REFUSES an announce-typed type or a
//     nonConvertible bump; the checkpoint's interpretation coordinate
//     (db.featureLevelInterpretationPin) is cleared, since a checkpoint cannot
//     be refused, and the first bump after the roll records the real one.
//
// Level 3: 0.8.2.
//
//   - Typed date/time rendering on SQL Server ingest
//     (sqlserver.featureLevelTypedTemporal): a date as YYYY-MM-DD, a time of
//     day as HH:MM:SS, zone-less datetimes without a zone, datetimeoffset
//     with its offset. The rendering is in entity KEYS (a temporal primary
//     key), so, as with the uniqueidentifier spelling, every node keeps the
//     old RFC 3339 rendering until the minimum reaches 3; then a session
//     resuming an old-style checkpoint re-snapshots once. Once typed it
//     stays typed.
//   - The type record's onSupersede field (db.featureLevelOnSupersede): it
//     changes which entities the scrub's rewrite removes, and an older binary
//     re-marshals the type without it, so a declaration that changes the
//     kind's default is refused until every member is at level 3.
//   - Ordered, pipelined ingest (db.featureLevelIngestOrder): an ingest
//     worker numbers its ordered proposals (cluster.Proposal.IngestSeq) and
//     keeps many in flight, and apply rejects one that is out of order,
//     storing it in the event log as an empty entry. An older binary would
//     apply it, so a worker numbers nothing until every member is at level
//     3, and its ordered proposals stay one at a time meanwhile.
//
// Level 4: 0.8.3.
//
//   - PostgreSQL snapshot text by output function
//     (postgres.featureLevelOutputRendering): boolean as t/f, char(n) padded,
//     a host inet without /32 — the text the replication stream carries. The
//     rendering is in entity KEYS (a boolean primary key), so every node
//     keeps the `::text` cast until the minimum reaches 4; then a session
//     resuming an old-style checkpoint whose tables have such a column
//     re-snapshots once. Once by output function it stays so.
const FeatureLevel uint64 = 4

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
