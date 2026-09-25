# Sustained append and actual rollover comparison

`BenchmarkEventLogRollover` compares the experimental segmented and tidwall
EventLog backends with 20 MiB targets and normal synchronous writes. It classifies
batches by observing new payload files after each append. Directory observation
is outside the append timer. Existing filenames must remain present, and each
batch must create at most one new payload file. This observation is test-only
knowledge of the two layouts, not a promise in the EventLog interface.

Each run writes 320 batches of 64 records. Payloads are 4,080 bytes, contain their
stable ID, and use IDs spaced by three. That is 20,480 records, 79.6875 MiB of
payload, or 80 MiB including segmented record framing (excluding group framing).
Both backends receive the same records and batches, without a reopen between
appends. Physical framing and exact boundary positions differ.

Ordinary and boundary timings cover the complete Append call, including backend
synchronization. A boundary sample is a 64-record batch, not a single record.
The fixture requires multiple actual rollovers in each backend. It then times
reopen, checks original progress, verifies every ID and payload, and verifies an
additional append through another reopen. Overall Go `ns/op` includes setup,
directory observation, and correctness checks; use named metrics for comparison.

Only plain encoding is compared. Tidwall can compress at rollover, whereas
segmented compression applies during rewrites, so compressed rollover would
perform different work. This compares the experimental tidwall generation
wrapper over the repository's fork, not production Storage or bare upstream WAL.
Reopen also performs different work: the tidwall wrapper verifies its full record
history, while the segmented backend checks metadata boundaries and its active
tail. Recovery timings do not measure equivalent validation coverage.

## Environment and limits

September 17, 2026, Go 1.26.6, Linux/arm64 in Alpine 3.20 on the local OrbStack VM.
Data resides on each container's disposable overlay filesystem; only the test
binary is bind-mounted read-only. Three pairs run sequentially after tests and
lint finish, reversing backend order in the second pair. The host and VM are not
load-controlled. Normal synchronization is enabled in both backends, but these
timings do not establish hardware power-loss behavior or equivalent durability
under every failure.

The per-batch directory walk introduces a gap between writes and touches
filesystem metadata; this is not a maximum-throughput ingestion test. The sample
has only a few boundaries, so its maximum is not a tail-latency percentile.
Warm-cache, small-history measurements do not establish cold recovery, 100 TB
operation, concurrent-reader latency, or production performance.

## Recorded results

All six benchmark runs passed. Each contained three observed boundary batches
and 317 ordinary batches. Times below are milliseconds; means and reopen values
are medians across the three runs. The largest observed batch is the maximum
across all three runs, not a percentile.

| Backend | Boundary mean | Range of boundary means | Ordinary mean | Largest boundary batch | Largest ordinary batch | Reopen |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Segmented/plain | 15.06 | 12.22–16.57 | 7.283 | 27.22 | 252.8 | 60.18 |
| Tidwall/plain | 12.36 | 10.72–19.83 | 6.782 | 28.89 | 150.9 | 218.9 |

Segmented boundary batches were faster in two pairs and slower in one. These
small, variable samples place both implementations in the same broad latency
range; they do not establish superiority or equivalence. This comparison includes
64 records per measured boundary call and should not be directly compared with
the single-record rollover benchmark.

Large ordinary-append outliers occur in both implementations. The benchmark does
not attribute them to allocation, garbage collection, scheduling, or filesystem
synchronization. Mean rollover cost alone does not characterize append stalls.
The segmented reopen advantage here also includes its narrower validation scope,
not just a faster implementation of the same recovery work.

## Validation and reproduction

`TestEventLogRolloverComparison` uses the same workload and assertions with 64 KiB
targets, eight-record batches, and ten batches. It passes under the macOS race
detector and on Linux. The full benchmark checks every record after recovery.

```sh
go test -race ./internal/cluster/db/eventlog -run '^TestEventLogRolloverComparison$' -count=1
go test ./internal/cluster/db/eventlog -run '^$' -bench '^BenchmarkEventLogRollover$' -benchtime=1x -count=3
```
