# Initial compression measurements

Local exploratory run, 2026-09-15: Apple M4 Max, macOS arm64, Go 1.26.6,
`klauspost/compress` v1.19.0. One short sample per case (`-benchtime 100ms`).
These measurements identify tradeoffs; they are not statistically robust results
or a comparison with the existing database engine.

The benchmark writes 256 records (~1.03 MiB payload total), using either a repeated
JSON-shaped payload or deterministic random bytes. It compares 256 KiB and 1 MiB
decoded block targets. Writers go to `io.Discard`; seeks read an in-memory encoded
segment. The write time includes encoder creation. There is no filesystem I/O,
fsync, background sealing, network, or cache. Allocations are total allocated bytes
per operation, not peak resident memory. Encoder window is fixed at 1 MiB.

## Selected results: 256 KiB blocks

| Input | Policy | Stored / payload | Write time | Allocated / write |
| --- | --- | ---: | ---: | ---: |
| Repeated | Plain | 100.4% | 0.281 ms | 1.30 MB |
| Repeated | ZstdFast | 0.3193% | 0.455 ms | 5.09 MB |
| Repeated | ZstdDefault | 0.2831% | 0.443 ms | 6.14 MB |
| Repeated | ZstdBetter | 0.2820% | 0.865 ms | 9.09 MB |
| Repeated | ZstdBest | 0.2802% | 2.122 ms | 40.48 MB |
| Random | Plain | 100.4% | 0.293 ms | 1.30 MB |
| Random | ZstdDefault | 100.4% | 0.681 ms | 7.65 MB |
| Random | ZstdBest | 100.4% | 10.030 ms | 42.13 MB |

The extremely small repeated-payload ratios are an artifact of this deliberately
repetitive dataset. They should not be used to estimate database backup sizes.

For repeated input, best effort saved about 1% of the bytes already compressed by
default effort, while taking about 4.8 times as long. For random input, fallback
preserved plain storage size but could not recover CPU spent trying compression.
These cases support keeping encoder effort configurable; they do not justify
selecting a production level yet.

With default effort, increasing the target to 1 MiB reduced the repeated-input
ratio from 0.2831% to 0.2462%. A seek decoded a larger block: about 219 microseconds
and 1.22 MB allocated versus 67 microseconds and 0.42 MB at 256 KiB. Reads create a
fresh decoder per block, which is included in these numbers.

## Reproduce

```sh
go test ./pkg/segmentlog -run '^$' -bench BenchmarkCompression -benchtime 100ms -count 1
```

Policy numbers in benchmark names: 0=plain, 1=fast, 2=default, 3=better, 4=best.
These benchmarks use synthetic payloads and do not measure production workloads,
peak memory, or a tidwall baseline under matching durability/cache settings.
