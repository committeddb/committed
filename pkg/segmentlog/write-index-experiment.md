# Building the index during segment encoding

`WriteSegment` retains block descriptors, then encodes the index after writing
all blocks. An experiment replacing that descriptor list with an incrementally
encoded index was not adopted. The writer remains unchanged.

## Experiment

Both candidates encoded each index entry immediately after writing its block,
retained the encoded bytes until the final index write, and derived the block
count from index length. The block limit, write order, format fields, and
checksums were unchanged.

The first candidate moved the existing field-by-field `AppendUint` operations
into block flush. The refinement appended one zeroed 48-byte entry, then filled
its fields with `PutUint` calls. This avoided multiple growth operations within
an entry, but still grew the encoded index incrementally.

`BenchmarkWriteSegmentIndex` writes one empty-payload record per block to
`io.Discard`. It covers zero, one, 80, 4,096, and 65,536 blocks. This emphasizes
index construction and tiny-block overhead; it is not representative payload
throughput. `BenchmarkMultiBlockEncoding` separately writes 20 MiB in 80 blocks,
with repeated or random 4,080-byte payloads and plain or default zstd encoding.
Neither benchmark measures durability or filesystem performance.

## Measurements

September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local OrbStack VM.
Baseline `aee84a1` and the first candidate ran three sequential pairs, 20
iterations per case, reversing order in the second pair. Validation finished
before measurement; each process completed before the next started. Host and VM
load were uncontrolled. These are medians across three runs.

| Blocks | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op | Baseline time | Candidate time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 408 | 408 | 12 | 12 | 525 ns | 527 ns |
| 1 | 528 | 552 | 16 | 18 | 550 ns | 625 ns |
| 80 | 16,768 | 12,968 | 23 | 26 | 4.327 µs | 4.098 µs |
| 4,096 | 1,024,384 | 908,480 | 31 | 40 | 220.087 µs | 721.190 µs |
| 65,536 | 19,219,240 | 16,792,916 | 43 | 53 | 7.421 ms | 10.719 ms |

In the 20 MiB benchmark, the first candidate saved about 3,800 allocated bytes
but added three allocations for every encoding case. Median encoding time was
effectively unchanged (about 3.94–6.51 ms across the four cases).

The refinement ran one additional sequential baseline/candidate pair of 20
iterations per case. For 20 MiB encoding it saved about 5,184 bytes without
changing allocation counts; timing ranged from about 3% faster to 3% slower.
At 4,096 blocks, allocation count rose from 31 to 37 and time rose from 181 µs
to 1,054 µs. At 65,536 blocks, allocated bytes fell from 19,219,513 to 16,791,712,
allocation count rose from 44 to 50, and time fell from 9.198 ms to 6.929 ms.

The timing variation does not establish the cause of the observed regressions.
The small saving at ordinary block counts, increased large-index allocation
counts, and inconsistent timing did not justify adopting either candidate.
These are cumulative allocations, not peak memory measurements.

## Retained validation

The benchmark and `TestWriteSegmentBlockLimit` are retained. The test verifies
that exactly 65,536 blocks can be written, opened, fully verified, and read at
the final record. One additional block is rejected before its data or the
index/footer is written. The initial candidate passed segmentlog race tests,
Linux tests, lint, and gosec; the final implementation uses the original writer.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^(BenchmarkWriteSegmentIndex|BenchmarkMultiBlockEncoding)$' -benchtime=20x
```
