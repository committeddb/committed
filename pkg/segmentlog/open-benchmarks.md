# Indexed segment metadata opening

`OpenSegment` allocates its block-descriptor list once using the footer's block
count. Allocation follows the block-count limit, file-bound checks, complete
index read, and index checksum validation. Individual entries still undergo the
same semantic validation before the segment is returned. The file format and
payload validation are unchanged.

`BenchmarkOpenSegment` isolates this metadata parsing using an in-memory reader.
Each block contains one empty-payload record with a sparse ID; fixture encoding
is outside the timer. Each timed open checks block count, record count, and
coverage. The benchmark includes empty, single-block, 80-block, 4,096-block, and
maximum-size (65,536-block) indexes. It measures neither filesystem I/O nor
payload validation and is not a whole-log recovery benchmark.

## Measurements

September 17, 2026, Go 1.26.6, Linux/arm64, Alpine 3.20 on the local OrbStack VM.
Baseline `60b79c6` and changed binaries ran three sequential pairs of 100
iterations per case, reversing order in the second pair. Tests and lint finished
before measurement; each benchmark process finished before the next started.
All runs passed. Host and VM load were uncontrolled. Values are medians across
three runs.

| Blocks | Before B/op | After B/op | Before allocs/op | After allocs/op | Before time | After time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 128 | 128 | 3 | 3 | 90 ns | 80 ns |
| 1 | 224 | 224 | 5 | 5 | 147 ns | 166 ns |
| 80 | 16,464 | 8,320 | 12 | 5 | 3.085 µs | 1.774 µs |
| 4,096 | 1,024,192 | 393,357 | 20 | 5 | 340.769 µs | 193.922 µs |
| 65,536 | 19,218,543 | 6,291,627 | 31 | 5 | 2.818 ms | 1.685 ms |

The allocation reduction comes from eliminating repeated descriptor-list growth
and copying. At the format limit, cumulative allocation falls from about 18.3 MiB
to 6 MiB. These figures include the encoded index and decoded descriptors; they
are not peak-memory measurements. Timing is a small local sample, including a
slower single-block median, and does not establish production recovery latency.

The segmentlog race suite, Linux segmentlog tests, lint, and gosec passed.

```sh
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkOpenSegment$' -benchtime=100x
```
