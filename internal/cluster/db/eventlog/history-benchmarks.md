# Event-log history benchmarks

Local exploratory run: macOS arm64, Apple M4 Max, Go 1.26.6. Three timed
iterations per case, recorded before the combined payload-verification pass. These are small synthetic, warm-filesystem measurements;
there is no cold-cache eviction, latency distribution, or production throughput
claim. Timing includes the local filesystem's behavior, not a power-loss proof.

## Workload

`BenchmarkEventLogHistory` runs the shared EventLog backends with a 32 KiB segment
target, 1 KiB deterministic varying-text payloads, sparse IDs spaced by ten, and
append batches of 31 records. Initial histories contain 124 or 992 records
(124 KiB or 992 KiB of logical payload). Both backends receive identical input and
batch sizes. Their physical framing, segment boundaries, and sync work differ.
The tidwall comparison uses the experimental generation container, not production
Storage's append path. Plain and zstd cases use each backend's existing encoding.

- **Reopen:** open, verify, and close an already constructed log. Includes the
  backend's recovery and durability confirmation.
- **Seek middle:** seek an exact existing ID in the middle of an open history and
  validate its ID and payload length. Repeated queries use the same ID; initial
  construction and open are excluded.
- **Append:** append one 1 KiB record above the original head. Every timed sample
  starts from a separately constructed history. Setup, reclamation, frontier
  checking, and close are outside the timer. For segmented storage this input
  fills each tail with 31 original frames, so the measured append forces rotation.
  The same input does not imply equivalent rotation work in tidwall.

Fixture construction and initial reclamation are excluded from reported times
and allocations. The benchmark's `history-B` metric is logical payload bytes,
not physical storage. Allocated bytes per operation are cumulative allocations,
not peak resident memory. The append case includes allocations for its batch.

## Recorded results

| Backend | Encoding | Records | Reopen (ms/op) | Middle seek (µs/op) | Append (ms/op) |
| --- | --- | ---: | ---: | ---: | ---: |
| segmented | plain | 124 | 1.760 | 84.667 | 43.148 |
| segmented | plain | 992 | 1.870 | 36.403 | 43.774 |
| tidwall | plain | 124 | 1.791 | 14.097 | 3.891 |
| tidwall | plain | 992 | 2.955 | 128.361 | 4.420 |
| segmented | zstd | 124 | 0.390 | 37.556 | 42.826 |
| segmented | zstd | 992 | 5.240 | 43.667 | 42.929 |
| tidwall | zstd | 124 | 1.724 | 34.486 | 4.460 |
| tidwall | zstd | 992 | 3.195 | 128.625 | 4.140 |

The segmented append samples took about 43 ms at both sizes in this run. That
includes sealing, multiple durability operations, and catalog publication; these
results do not isolate the cost of any one step. Reopen timings are noisy at this
sample count. The table does not establish an asymptotic timing curve.

Allocation measurements expose work that timing alone can obscure:

| Backend | Encoding | Records | Reopen allocated B/op | Append allocated B/op |
| --- | --- | ---: | ---: | ---: |
| segmented | plain | 124 | 292,738 | 476,080 |
| segmented | plain | 992 | 2,309,770 | 2,461,757 |
| tidwall | plain | 124 | 449,928 | 2,352 |
| tidwall | plain | 992 | 3,549,680 | 3,536 |
| segmented | zstd | 124 | 293,378 | 2,184,173 |
| segmented | zstd | 992 | 2,299,285 | 4,170,144 |
| tidwall | zstd | 124 | 573,816 | 2,352 |
| tidwall | zstd | 992 | 4,817,773 | 3,536 |

The recorded baseline used the removed complete-catalog publisher. It verified
all referenced payloads during publication and recovery, so work grew with stored
history even when rotation added only one segment. Those measurements predate
bbolt and do not describe the current publication or recovery path. See the
[bbolt measurements](../../../../pkg/segmentlog/bbolt-experiment.md#benchmarks).

Sparse segment reads use block indexes and do not perform full-history catalog
verification on each seek. The benchmark's seek allocation results cover one
repeated middle lookup, not all access patterns. Neither this run nor the unit
tests establish behavior at hundreds of TB or a PB.

## Combined verification check

After combining digest and frame verification, a counting-reader test checks that
each stored payload byte is requested exactly once, while all file bytes remain
covered by verification. That change removed one payload pass; it did not remove full-history
verification, metadata rereads, or file syncs.

A separate three-iteration local rerun of segmented reopen/append cases completed
without concurrent test processes. Append samples were about 40.9 ms (124/plain),
274.0 ms (992/plain), 45.5 ms (124/zstd), and 46.2 ms (992/zstd). The large plain
sample and the small sample count do not establish a consistent latency improvement
against the baseline. The established benefit is reduced payload read requests,
not a demonstrated throughput gain or reduction in physical disk traffic.

## Reproduce

```sh
go test ./internal/cluster/db/eventlog -run '^$' -bench '^BenchmarkEventLogHistory$' -benchtime=3x -count=1
```

Use the fixed iteration count to bound setup work: each append sample constructs
a separate history outside the timer, so elapsed benchmark duration exceeds the
reported append time. These results are independent of the completed-file churn
measurements in the [scrub experiment](../wal/segment_churn_experiment.md).
