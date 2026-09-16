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
