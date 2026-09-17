# Live-segment churn and Linux validation

`TestLiveSegmentChurn` exercises three scrub/reclaim/reopen rounds over 32 closed
ranges plus an active tail, with plain and zstd rewrite encoding. It runs in the
normal test suite. `BenchmarkLiveSegmentChurn` uses 128 or 1,024 closed ranges and
six rounds, with the same assertions.

## Workload and checks

All files are created through managed append. Each range initially contains
eight 128-byte records with sparse IDs, filling a 1,152-byte original-frame target.
Every scrub revisits the same scattered one-in-sixteen ranges, including the
active tail: the first pass erases one record per affected range; every pass
changes the remaining payloads. Changed closed files become indexed segments;
unaffected append-format files remain selected with identical descriptors.

Each round checks stable coverage and unaffected references, exact retirement
counts, and all survivor IDs and payloads against an independent expected map
after reopen. The fixture now streams append batches and retains expected payload
digests rather than duplicate payload bytes. The final append must cross a boundary despite erasure. Another
reopen checks its payload; full verification checks selected file digests, and an
orphan sweep must find no unpublished files. Normal synchronization is enabled.

## Recorded Linux run

On September 17, 2026, Go 1.26.6 cross-compiled Linux/arm64 test binaries ran in
Alpine 3.20 containers on the local OrbStack Linux VM, using the container's
`overlay` filesystem. Fixture files were on the disposable Linux writable layer;
only the executable was bind-mounted read-only from macOS. Containers had no
network access. The full segmentlog and EventLog test binaries passed.

The benchmark ran three times per case. Times below are medians across those
runs; scrub, reclaim, and reopen values are each run's mean over six rounds.

| Closed ranges | Rewrite encoding | Boundary append ms | Scrub ms/round | Reclaim ms/round | Reopen ms/round |
| ---: | --- | ---: | ---: | ---: | ---: |
| 128 | Plain | 3.178 | 23.15 | 5.834 | 0.0766 |
| 128 | Zstd | 2.722 | 29.60 | 5.530 | 0.0655 |
| 1,024 | Plain | 2.751 | 110.20 | 33.130 | 0.0816 |
| 1,024 | Zstd | 3.660 | 140.40 | 36.390 | 0.0878 |

Metadata measurements were identical for both encodings and all three repetitions:

| Closed ranges | Initial metadata file | Final metadata file | Page allocation per scrub/reclaim round | Free / pending pages after last reclaim |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 128 KiB | 256 KiB | 56 KiB | 9 / 3 |
| 1,024 | 1 MiB | 1 MiB | 300 KiB | 65 / 8 |

Allocation counts bbolt pages allocated during the transactions, including reuse;
it is not file growth or measured device writes. The final file size includes the
post-churn append. Free/pending counters are sampled before the last reopen.
These runs show bounded file size over this short workload, not a general bound
on fragmentation. Rewrites still scan the full record stream.

## Reproduction and limits

On Linux with the repository's Go toolchain:

```sh
go test ./pkg/segmentlog/... ./internal/cluster/db/eventlog/...
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkLiveSegmentChurn$' -benchtime=1x -count=3
```

The benchmark's overall `ns/op` includes fixture construction and validation;
use the named phase metrics for the table above. This is roughly 129 KiB or
1 MiB of original payload, with small segments and warm filesystem state. It
does not measure 20 MiB rollover hashing, cold recovery, millions of physical
files, long-lived metadata readers, sustained fragmentation, or 100 TB payloads.
The VM's fsync behavior and scheduling do not establish production latency or
power-loss durability. The Linux binaries were built without the race detector;
the new regression test separately passed the macOS race detector.

The [full-size experiment](full-size-churn.md) uses the same checks with 64 closed
20 MiB ranges and a full active tail, and reports replacement and retired bytes.
