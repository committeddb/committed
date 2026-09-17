# On-disk lifecycle scale baseline

These measurements predate the bbolt catalog. The complete-catalog publisher
described here has been removed; see the [bbolt results](../../../../pkg/segmentlog/bbolt-experiment.md#benchmarks)
for the later measurements.

Local run: macOS arm64, Apple M4 Max, Go 1.26.6. One iteration per case, run
sequentially without concurrent test processes. These are warm-filesystem,
synthetic measurements with no latency distribution or cold-cache eviction.

## Workload and checks

`BenchmarkEventLogScale` uses 1 MiB segment targets and batches of 255 records.
Each record contains 4 KiB of deterministic varying text; IDs are spaced by ten.
Eight batches contain 2,040 records (7.96875 MiB of payload); 64 batches contain
16,320 records (63.75 MiB). Both backends receive the same records and batches.
Encoding labels select their existing plain or zstd configuration; active data
and incompressible blocks need not be compressed.

Each iteration builds a new history, reclaims setup artifacts, and closes it.
Timed phases reopen it, append one record, erase one middle record, reclaim
obsolete files, and scan every survivor. The append forces segmented rotation:
255 original frames consume 1,048,560 bytes, just below the 1 MiB target.
Tidwall does not rotate on this measured append: its current file is below its
rollover threshold in both cases. These append timings are not a comparison of
rollover latency between backends.
The comparison uses the experimental tidwall generation container, not production
Storage's append path.

The scan checks every surviving ID and payload length. An additional untimed
reopen checks that the erased ID remains absent, the appended payload remains
byte-identical, and original append progress is preserved. The benchmark fails
if those checks fail.

## Recorded baseline before retained append files

All times below are milliseconds. Each row is a single sample.

| Backend | Encoding | Batches | Build | Reopen | Append | Scrub | Reclaim | Scan |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| segmented | plain | 8 | 421.8 | 10.34 | 55.77 | 52.64 | 31.07 | 3.72 |
| segmented | plain | 64 | 5,444 | 58.13 | 96.77 | 149.6 | 66.34 | 20.08 |
| tidwall | plain | 8 | 143.4 | 7.41 | 5.19 | 70.44 | 49.43 | 5.66 |
| tidwall | plain | 64 | 1,295 | 28.77 | 4.37 | 499.1 | 687.9 | 39.90 |
| segmented | zstd | 8 | 412.2 | 10.56 | 58.51 | 71.12 | 34.23 | 4.30 |
| segmented | zstd | 64 | 5,099 | 47.20 | 87.00 | 204.2 | 86.21 | 20.76 |
| tidwall | zstd | 8 | 129.7 | 9.34 | 5.05 | 133.5 | 41.02 | 4.07 |
| tidwall | zstd | 64 | 1,123 | 31.00 | 5.22 | 1,042 | 311.6 | 26.62 |

Build includes payload generation and initial appends. It is reported separately
and excluded from Go's timed lifecycle and allocation metrics. Initial cleanup,
closing, and final recovery checks are also excluded. Phase timers include each
operation's local durability work; the scan includes its correctness assertions.

At 64 batches, reclamation removed about 2.13 MB for segmented storage versus
67.08 MB for tidwall. This includes obsolete artifacts from the measured append
and scrub together, including catalog files. It is not a measurement of backup
uploads, live storage footprint, or scrub-only output bytes.

Cumulative allocations across the timed lifecycle were about 628 MB (segmented
plain), 640 MB (segmented zstd), 1,585 MB (tidwall plain), and 2,273 MB (tidwall
zstd) at 64 batches. These are bytes allocated over several operations, not peak
resident memory or simultaneous retained data.

## Interpretation and limits

The selective rewrite reduced scrub time and obsolete-file volume in this run.
The segmented boundary append was substantially slower than tidwall's append.
The recorded baseline verified the complete referenced history on publication,
so rotation work grew with history even though it added one segment. Recovery and
reclamation also verify history. This experiment does not isolate verification,
codec, allocation, or sync costs from one another.

The larger histories are still only tens of MiB and at most dozens of segmented
ranges. These results do not establish TB/PB behavior, catalog-limit behavior,
peak memory, sustained application throughput, production compression ratios,
or recovery after a crash. The [concurrency tests](../../../../pkg/segmentlog/concurrency-testing.md)
exercise scheduling separately.

```sh
go test ./internal/cluster/db/eventlog -run '^$' -bench '^BenchmarkEventLogScale$' -benchtime=1x -count=1 -timeout=10m
```

Use a fixed iteration count: every iteration creates and validates an independent
on-disk history, so wall time exceeds the reported timed lifecycle.

## Incremental publication follow-up

After publication began reusing verification of exact unchanged immutable
references, a separate single-iteration run measured these boundary appends:

| Encoding | Batches | Baseline append (ms) | Follow-up append (ms) |
| --- | ---: | ---: | ---: |
| Plain | 8 | 55.77 | 41.18 |
| Plain | 64 | 96.77 | 42.07 |
| Zstd | 8 | 58.51 | 43.21 |
| Zstd | 64 | 87.00 | 46.00 |

In that measured version, publication stopped reading and syncing unchanged
sealed payloads. It still published complete catalogs and verified the complete
live layout during recovery, rewrite preflight, and reclamation.

That implementation still had four synchronous durable publications:
sealed segment, new active tail, catalog, and CURRENT, followed by the new append.
A separate local timing trace measured roughly 10 ms per publication. Sealing and
these durability steps blocked the caller. The follow-up did not establish
acceptable production append latency or solve the remaining synchronous stall.

## Retained append-file rollover

After rollover began retaining the completed append file unchanged, a separate
single-iteration run produced the following times in milliseconds. Tests and lint
had completed before this run. These are individual warm-filesystem samples,
not latency distributions or a controlled before/after comparison.

| Encoding | Batches | Build | Reopen | Append | Scrub | Reclaim | Scan |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain | 8 | 288.3 | 6.04 | 32.74 | 54.95 | 24.00 | 4.75 |
| Plain | 64 | 2,694 | 97.88 | 53.96 | 165.4 | 177.3 | 36.72 |
| Zstd | 8 | 322.3 | 38.16 | 51.68 | 80.87 | 41.30 | 4.81 |
| Zstd | 64 | 2,766 | 53.68 | 32.18 | 139.7 | 82.07 | 35.01 |

The verified structural change is removal of rollover's converted segment write
and publication. The old append file keeps its name, inode, and bytes. That version published the
new tail, catalog, and CURRENT before appending and syncing the new records. Hashing and validation of the newly immutable reference remain.
The append samples do not establish a consistent latency improvement over the
incremental-publication run, and synchronous boundary stalls remain.

Both encoding configurations now retain uncompressed append files on rollover.
The zstd setting applies to indexed rewrite outputs; the initial histories have
the same physical encoding. A changed range becomes an indexed replacement,
while unchanged ranges retain their append framing. Reclamation removed about
1.08 MB at 64 batches, compared with the earlier 2.13 MB baseline that also
retired a converted tail.

Closed append files use sequential validation and scanning instead of indexed
block reads. The measured 64-batch scans took about 35–37 ms, versus 20–21 ms in
the original segmented baseline. Recovery still verifies all referenced history;
these samples do not demonstrate improved recovery performance. Every measured
run checked survivor IDs, payload lengths, erasure, and append recovery.

## Explicit rollover preparation contract

With segment storage owning file preparation and the catalog publisher consuming
a private prepared-rollover handle, three single-iteration runs per segmented
case measured the following boundary appends. Tests and lint completed before
this run; the same machine and warm-filesystem workload were used.

| Encoding | Batches | Append range (ms) | Median (ms) |
| --- | ---: | ---: | ---: |
| plain | 8 | 30.27–35.78 | 34.48 |
| plain | 64 | 30.21–34.03 | 33.71 |
| zstd | 8 | 28.06–29.87 | 28.71 |
| zstd | 64 | 27.86–32.60 | 29.03 |

The measured code path omitted recovery-style opening of the new empty tail, repeated
verification and synchronization of prepared payloads, and the catalog layer's
extra directory sync before metadata installation. The old file was read
once to calculate its digest. New tail, catalog, and CURRENT installations
synchronized their files and directories, followed by the new append's sync.

These samples establish neither production latency nor a controlled speedup over
the earlier single samples. Boundary latency remains tens of milliseconds.
Recovery and read paths are unchanged by the preparation contract. The measured
lifecycle and subsequent reopen checks passed for all twelve runs.
