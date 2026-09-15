// Package segmentlog provides experimental, application-independent segmented
// storage for ordered records with sparse uint64 IDs and opaque payloads.
//
// This first slice implements immutable segments and selective replacement.
// It does not yet implement a durable append log, atomic catalog publication,
// reader pins, or physical retirement. Callers must not activate this format
// as production storage. The API and on-disk format are not stable.
package segmentlog
