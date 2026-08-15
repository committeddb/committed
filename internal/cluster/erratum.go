package cluster

// ErratumTypeID is the reserved system-type UUID for the errata
// interpretation-registry record (clusterpb.LogErratum) — the first GATED
// entry in the system-type namespace. Minted as 0.8.x format groundwork so
// the identity is fixed before the semantics land: nothing emits, registers,
// or applies the type yet (the errata-interpretation-registry work later in
// the series does), and until then the ID deliberately stays UNREGISTERED —
// a node encountering the record without the registry code takes the
// namespace's gated-unknown path (fatal), never a silent skip or a
// half-understood apply. Gated because errata are correctness-bearing: a
// node that skipped them would serve stale readings to its syncables.
// Emission will additionally be FeatureLevel-gated.
var ErratumTypeID = reservedSystemID(compatGated, 0)
