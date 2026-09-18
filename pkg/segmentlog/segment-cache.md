# Segment cache core

The package contains an internal cache core and immutable decoded segment
representation. **Managed log operations do not use this cache.** There is no
active-tail cache or public cache configuration in this implementation.

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
