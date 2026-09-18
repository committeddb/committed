# Segment cache core

The package contains an internal cache core and immutable decoded segment
representation. Managed closed-range reads and rewrites support an internally
attached historical cache. **Normal CreateLog/OpenLog calls leave caching
disabled.** There is no active-tail cache or public cache configuration.

## Contents and ownership

`materializeSegment` consumes a validating range source completely before
returning an entry. It checks coverage, record count, increasing sparse IDs, and
propagates source errors without returning a partially populated entry. Payloads
are copied immediately into private storage, so a source may reuse its buffers.
The immutable representation contains payload bytes and an ID/offset index.
Validation of physical framing and checksums remains the source's responsibility.

Seek and iteration return private payload copies. A caller or transform cannot
modify the cache by changing those returned bytes. Readers hold ordinary Go
references to entries; eviction or invalidation removes cache ownership without
clearing or reusing the entry's storage. Reading an entry requires no cache lock.

## Retention policies

One cache is scoped to one log directory. Its identity key is the complete
`SegmentRef`, including filename, digest, coverage, count, and frozen tail size.
Entries with different immutable revisions cannot match the same key.

- Recent entries are ordered by logical range start, newest first. Acquisitions
  do not change this order. An old arrival cannot evict newer ranges to fit.
- Historical entries use strict LRU. Each segment acquisition refreshes recency;
  reads through the returned entry do not. There are no scrub-specific rules.
- A single identity belongs to at most one list. Duplicate retention returns the
  existing object. Promotion from historical to recent transfers ownership
  without copying contents; a rejected promotion leaves historical retention
  intact.
- Each policy has an independent byte budget. Zero disables retention in that
  policy. An oversized entry remains usable by its caller but is not retained.
- Recent eviction does not automatically populate the historical cache.
- Explicit invalidation removes an identity from either policy.

Policy changes operate under one mutex. No file I/O, source iteration, payload
copying, or user callbacks run under that mutex. Recent insertion uses a simple
ordered list; historical operations use a map and doubly linked list. The core
does not load files or coordinate concurrent cache misses.

Charges include the entry object and allocated capacities of the payload and
record-index arrays. They exclude map/list bookkeeping, caller payload copies,
construction temporaries, and evicted entries still referenced by readers. The
budgets bound cache-retained charges, not total process memory. Statistics report
retained bytes and entries per policy, acquisition hits/misses, and capacity
evictions. Explicit invalidation and promotion are not counted as evictions.

## Validation

Tests cover recent ordering independent of access and insertion order, strict
historical LRU at segment acquisition, separate budgets, oversized and disabled
admission, identity changes, promotion without duplication, failed promotion,
payload ownership, sparse seeks, source buffer reuse, corrupt/incomplete source
rejection, and concurrent acquisition/invalidation/eviction with surviving readers.

## Managed acquisition

Seek, Scan, and sealed-range rewrite preparation share `acquireRange` under the
existing log mutex. A hit supplies immutable in-memory contents without opening
the file. A miss checks the file and catalog metadata, consumes the validating
source into a complete cached representation, closes the file, then admits the
entry to historical LRU. Failed loading or closing never admits a partial entry.
A nil cache or zero historical budget retains the streaming file-backed path.
Recent entries, if present internally, can be acquired through the same path;
this integration does not populate recent entries or change active-tail reads.

Cold cached acquisition materializes the entire selected range, including
indexed blocks outside a requested read interval. This differs from uncached
indexed reads, which validate only selected blocks. Frozen append files retain
their full prevalidation pass on a miss; warm accesses use the already validated
contents. Loading temporaries and oversized entries are outside retained-cache
budgets. Managed operations still serialize; this change does not provide
concurrent scan/append or scrub execution.

Successful rewrite publication discards replaced cache identities while retaining
unchanged entries. New indexed replacements load through their own catalog
identity. Existing references to old entries remain immutable and usable after
discard and file reclamation. Close drops the log's cache reference. Explicit
Verify bypasses the cache and checks selected files on disk.

Managed tests remove a warmed file temporarily to prove that read hits use memory
while Verify still detects its absence. They cover payload mutation, replacement
identity, retention of unchanged ranges, reading indexed replacements after
reclamation, close, and rejection of a corrupt cold source before callbacks.
