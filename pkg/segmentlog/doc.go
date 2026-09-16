// Package segmentlog provides experimental, application-independent segmented
// storage for ordered records with sparse uint64 IDs and opaque payloads.
//
// The implementation includes a synchronous append/read/rotate lifecycle,
// immutable segments, selective replacement preparation, and atomic catalogs.
// Managed logs hold an advisory directory lock until Close. The implementation
// does not yet provide integrated scrubbing, reader pins,
// background sealing, or physical retirement. Callers must not activate this format
// as production storage. The API and on-disk format are not stable.
package segmentlog
