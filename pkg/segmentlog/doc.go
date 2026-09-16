// Package segmentlog provides experimental, application-independent segmented
// storage for ordered records with sparse uint64 IDs and opaque payloads.
//
// The implementation includes a synchronous append/read/rotate lifecycle,
// immutable segments, transactional whole-log rewriting, and atomic catalogs.
// Managed logs hold an advisory directory lock until Close. The implementation
// supports explicit reclamation of obsolete managed files. Concurrent rewriting,
// reader pins, background sealing, and retirement for captured views remain pending. Callers must not activate this format
// as production storage. The API and on-disk format are not stable.
package segmentlog
