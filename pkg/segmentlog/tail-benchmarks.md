# Tail scan allocations

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. Each before/after measurement uses
100 iterations, run sequentially without concurrent tests.

`BenchmarkTailScan` reads an in-memory tail containing one append group with 16 or
4,096 sparse records and 32-byte payloads. Construction is outside the timer.
The verification case has no visitor, as used by tail recovery; the delivery case
visits every record. Both validate the entire group. No disk I/O or sync is timed.

| Records | Operation | Before B/op | Current B/op | Before allocs/op | Current allocs/op |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16 | Verify | 1,920 | 960 | 7 | 3 |
| 16 | Deliver | 1,920 | 1,472 | 7 | 4 |
| 4,096 | Verify | 627,383 | 204,920 | 17 | 3 |
| 4,096 | Deliver | 627,329 | 335,938 | 17 | 4 |

For the larger group, verification allocates about 67% fewer bytes and delivery
about 46% fewer. Verification counts and validates frames without collecting
record descriptors. Delivery allocates the descriptor list once using the bounded
group count, after the first frame validates. Both retain the group buffer during
validation; delivered payloads can retain that buffer afterward.

Tests exercise incorrect counts, an incorrect last ID, and a bad final frame
with valid outer checksums. Verification and delivery report the same validated
prefix, and a corrupt group delivers no records. Existing recovery tests cover
incomplete groups, failed syncs, and rewritten-tail checkpoints.

These synthetic allocation results do not establish database recovery time or
application scan throughput. Tail encoding and durability behavior are unchanged.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkTailScan$' -benchtime=100x -count=1
```

## Reusing descriptors across append groups

Visitor scans now reuse one internal record-descriptor list. Each record is
passed to the visitor by value; payload bytes still belong to independent group
buffers. Clearing the list before reuse removes old payload references from
entries that a smaller next group would leave unused. The largest descriptor
allocation remains until the scan finishes. This reduces cumulative allocation,
not necessarily peak memory. Validation-only scans still allocate no descriptors.

A test scans groups containing 2, 8, 1, and 4 records and checks retained payloads.
It also appends a group whose final frame is corrupt but whose enclosing checksum
is valid, requiring no records from that group to be delivered. Existing tail
recovery, cancellation, incomplete-group, and checkpoint tests remain in place.

`BenchmarkMultiGroupTailScan` uses 80 groups with either 32-byte or 4,080-byte
payloads. Groups contain 5,461 or 64 records respectively: 436,880 small records
or 5,120 larger records, approximately 20 MiB of original frames in either case.
Delivery scans check every sparse ID and payload. Both delivery and validation
check the final count and byte end. In-memory fixture construction is untimed;
there is no filesystem I/O, synchronization, or publication in the measurements.

Recorded September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local
OrbStack VM. The baseline tail scanner is from `116e2fc`. Before/after binaries
ran three sequential pairs with 20 iterations per case, reversing order in the
second pair. Each process finished before the next started, after validation
completed. All runs passed. Host and VM load were uncontrolled. Values below are
medians across the three runs.

| Payload / operation | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 / validate | 262,256 | 262,256 | 4 | 4 | 5.053 ms | 5.055 ms |
| 32 / deliver | 35,389,588 | 21,151,864 | 163 | 84 | 8.034 ms | 7.997 ms |
| 4,080 / validate | 270,448 | 270,448 | 4 | 4 | 4.288 ms | 4.219 ms |
| 4,080 / deliver | 21,811,329 | 21,629,308 | 163 | 84 | 5.203 ms | 5.263 ms |

Small-record delivery saves about 13.6 MiB per scan; larger-record delivery saves
about 178 KiB. Validation allocation is unchanged. Timing differences are small
and mixed, so this establishes an allocation reduction rather than a general
speedup. Storage race suites, Linux segmentlog tests, lint, and gosec passed.
