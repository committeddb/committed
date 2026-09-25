# Full-sized segment read-cache measurements

`BenchmarkEventLogCachedReads` compares the same logical history through the
experimental EventLog implementations. It measures steady reads with a warm
filesystem and warmed logical read windows.

## Workload

- 23,040 sparse records, IDs increasing by ten, with 4,080-byte payloads.
- Plain encoding and a 20 MiB segment target. For segmented storage this produces
  four full closed append files and a half-full active tail (90 MiB framed data).
  Tidwall's envelope and framing produce different physical boundaries.
- Append batches of 64 records; fixture construction and cleanup are untimed.
- Cached segmented storage has independent 64 MiB recent and 64 MiB historical
  budgets. The active tail and temporary allocations are additional. Recent
  contents come from rollover; historical contents are loaded during warmup.
- Each seek case warms and then cycles through a 128-record window in the active
  tail, a recent closed segment, or the oldest closed segment. Each scan visits
  the same 64 historical records. Results validate IDs, payload lengths, and the
  embedded ID in each payload. Timings include these checks and payload copies.
- Cases run separately and sequentially. There are no concurrent appends,
  scrubs, Actual decoding, or interleaved live/historical readers in this benchmark.

The tidwall case is the **experimental wrapper with its default two-segment
cache**. Its sparse-ID lookup binary-searches physical records, potentially
visiting multiple segments for one seek. Warming the requested window does not
ensure those search probes stay resident. Production uses a configurable cache
with a default of 16 segments and a different read path. This is not a production
tidwall comparison or a comparison with equal memory budgets.

## Reproduction

Measured on an Apple M4 Max, darwin/arm64, Go 1.26.6, with GOMAXPROCS=16 on
2026-09-19. Engine baseline: `652f3ca`. Three one-second samples per case;
no other agent-started build or lint job ran alongside these samples.

```sh
GOCACHE="$PWD/.claude-scratch/go-build" go test ./internal/cluster/db/eventlog \
  -run '^$' -bench '^BenchmarkEventLogCachedReads$' -benchtime=1s -count=3
```

Allocated bytes are per-operation allocations, not retained cache size or RSS.
These local measurements do not establish cold-load latency, cache behavior on
larger working sets, Linux performance, or concurrent streaming throughput.

## Results

Medians of three samples; latency is per seek or per complete 64-record scan.

| Backend | Read | Latency | Allocated bytes/op | Allocs/op |
| --- | --- | ---: | ---: | ---: |
| segmented-uncached | active-tail | 2,745.65 µs | 10,674,538 | 108 |
| segmented-uncached | recent-sealed | 5,121.66 µs | 1,218,772 | 90 |
| segmented-uncached | historical | 5,106.81 µs | 1,218,790 | 90 |
| segmented-uncached | historical-scan-64 | 5,191.46 µs | 1,366,369 | 99 |
| segmented-cached | active-tail | 7.69 µs | 11,632 | 58 |
| segmented-cached | recent-sealed | 11.58 µs | 13,736 | 71 |
| segmented-cached | historical | 11.52 µs | 13,736 | 71 |
| segmented-cached | historical-scan-64 | 32.25 µs | 271,971 | 141 |
| tidwall-default-cache | active-tail | 12.47 µs | 127,616 | 31 |
| tidwall-default-cache | recent-sealed | 12.78 µs | 127,552 | 31 |
| tidwall-default-cache | historical | 4,554.59 µs | 64,165,626 | 91 |
| tidwall-default-cache | historical-scan-64 | 4,617.86 µs | 64,701,519 | 224 |

Caching removes repeated tail scanning and frozen-file validation from these warm
reads. Per-seek allocations remain roughly 11–14 KiB in the cached segmented
cases, including a 4,080-byte caller-owned payload. A warm cache therefore does
not eliminate allocation or managed metadata-access costs. The 64-record scan
amortizes segment acquisition across its records while retaining private payload
copies.

The experimental tidwall wrapper's historical cases allocate roughly 61 MiB per
operation despite warmup. Its two-entry cache and cross-segment binary-search
probes make these results sensitive to the wrapper's lookup strategy and cache
capacity. They do not demonstrate a fundamental historical-read disadvantage of
tidwall's storage format.

## Tail-only metadata shortcut

Seek and Scan use the header's active-tail start to skip closed-range iteration
when the requested interval lies entirely in the tail. Previously, that empty
iteration opened a second bbolt read transaction and decoded the header again.
Header validation, the managed log lock, and private result payloads remain in
place; the shortcut also applies with caching disabled.

On the same machine and fixture, a fresh two-second pre-change sample measured
7.667 µs, 11,632 B/op, and 58 allocations for the cached active-tail seek. Three
two-second post-change samples measured 4.100, 4.131, and 4.125 µs, each with
7,256 B/op and 28 allocations. The median is about 46% lower latency and 38% fewer
allocated bytes. These measurements ran without concurrent agent-started builds.
The table above records the earlier baseline; this shortcut does not affect its
historical or recent-sealed cases. Tail-only scan timing was not measured here.

```sh
GOCACHE="$PWD/.claude-scratch/go-build" go test ./internal/cluster/db/eventlog \
  -run '^$' -bench '^BenchmarkEventLogCachedReads/segmented-cached/active-tail$' \
  -benchtime=2s -count=3
```
