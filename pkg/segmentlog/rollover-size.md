# Full-size rollover and recovery measurements

`BenchmarkRolloverSize` measures 1, 20, and 32 MiB original-frame targets, including
the managed log's 20 MiB default. It uses 4,080-byte payloads plus 16-byte frames,
appended in batches of up to 64 records. Append-group framing is additional.

Each run measures five cycles:

1. Fill the active tail exactly to its original-frame target.
2. Close and time reopening the full active tail.
3. Time an append of one record that must cross the segment boundary.
4. Close and time reopening with one active record and growing closed history.

The fixture checks coverage, counts, frozen size, retained predecessor inode and
size, original append progress, and the newly appended record after recovery.
It scans every record against its expected payload and runs full verification at
the end. A 64 KiB version runs as `TestRolloverSizeWorkload` in the normal suite.

The benchmark adds test-only timers around the existing file installer and bbolt
commit function. Production code and synchronization behavior are unchanged.

## Linux results

Recorded September 17, 2026 with Go 1.26.6, Linux/arm64, and Alpine 3.20 in the
local OrbStack VM. Data resides on the container's disposable `overlay` writable
layer; only the executable is bind-mounted read-only. Tests and lint completed
before the benchmark. The VM and host were not reserved or load-controlled.

All nine runs passed their checks. Each value below is the median of three runs'
five-cycle means, in milliseconds. The range shows the smallest and largest
run means, not individual append extremes or tail-latency percentiles.

| Original-frame target | Boundary append | Range of boundary means | Full-tail reopen | One-record-tail reopen |
| ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 66.73 | 63.06–70.48 | 10.84 | 5.11 |
| 20 MiB | 77.09 | 57.71–81.83 | 34.40 | 10.17 |
| 32 MiB | 67.13 | 59.42–71.79 | 50.90 | 10.55 |

To keep phase sums meaningful, the breakdown below uses the same run that supplied
each size's median boundary mean:

| Target | New-tail installation | Metadata commit | Other boundary work | Total |
| ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 24.51 | 30.69 | 11.53 | 66.73 |
| 20 MiB | 19.43 | 28.92 | 28.74 | 77.09 |
| 32 MiB | 20.94 | 21.07 | 25.12 | 67.13 |

Installation includes writing/syncing the empty new tail and publishing its name
durably. Metadata commit includes the bbolt transaction and its synchronization.
Other work includes hashing the old tail, changing handles, and writing/syncing
the new record. It is **not** a measurement of hashing alone.

Durable installation and commit account for much of the measured boundary time.
The much slower 1 MiB result compared with the earlier tiny-segment Linux run,
and the non-monotonic boundary results, prevent attributing the whole difference
to segment size. Full-tail recovery grows with the amount of active data in these
samples. One-record-tail recovery stays much smaller despite accumulating up to
160 MiB of closed history, consistent with avoiding a historical payload scan.

These results do not establish production latency. They use warm filesystem
state, a VM-backed filesystem, only five closed ranges, and no concurrent log
users. Closing/reopening does not evict the operating-system cache. The workload
does not establish cold recovery, 100 TB scale, physical device flush behavior,
or power-loss durability. The earlier small-segment timings should not be used
as estimates for default-size rollover.

## Reproduction

```sh
go test -race ./pkg/segmentlog -run '^TestRolloverSizeWorkload$' -count=1
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkRolloverSize$' -benchtime=1x -count=3
```

The overall `ns/op` includes fixture construction and verification. Use the named
phase metrics for comparison. The workload passed on Linux without the race
detector and separately under the macOS race detector.
