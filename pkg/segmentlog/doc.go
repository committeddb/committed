// Package segmentlog provides experimental, application-independent segmented
// storage for ordered records with sparse uint64 IDs and opaque payloads.
//
// The implementation includes immutable segments, selective replacement, and a
// synchronized single-file active tail, and atomic local catalog publication.
// It does not yet implement a complete durable log lifecycle, directory locking,
// reader pins, or physical retirement. Callers must not activate this format
// as production storage. The API and on-disk format are not stable.
package segmentlog
