# Full-size rewrite CPU profile

The live-churn fixture labels each managed rewrite with `segmentlog.phase=rewrite`,
`segmentlog.round` (one-based), and `segmentlog.codec` (the numeric compression
option). Label setup is outside the rewrite timer. Labels include the transform
callback and its expected-digest updates. Fixture appends, survivor checks,
reclamation, reopen, and final verification run outside the labeled scope.

Labels also identify iterator-coroutine work whose stack does not contain
`Log.Rewrite`. Filtering only by that function misses part of the rewrite work.
The labels are benchmark instrumentation; the storage implementation is unchanged.

## Recorded profile

September 17, 2026, implementation `81b4cd0` plus these labels, Go 1.26.6,
Linux/arm64, Alpine 3.20 on OrbStack. One plain and one zstd case ran sequentially,
each with a fresh 1,300 MiB history and two rewrite rounds. Data lived on the
disposable container overlay filesystem; only the executable and profile output
directory were host mounts. Focused churn race tests and lint passed first.
Both full-size cases passed every survivor, stable-file, reclamation, reopen,
rollover, verification, and orphan check.

The complete profile spans 96.40 seconds and contains 72.00 seconds of CPU
samples. Rewrite-labeled samples total 16.77 seconds: 10.09 seconds for plain,
6.68 seconds for zstd. Across both encodings, round 1 contributes 8.54 seconds and
round 2 contributes 8.23 seconds. These are sampled CPU totals, not wall times.

Leading **flat** costs, as percentages of the 16.77-second labeled sample:

| Work | CPU samples | Share |
| --- | ---: | ---: |
| CRC32C assembly (`castagnoliUpdate`) | 7.81 s | 46.57% |
| Linux syscall entry (`Syscall6`) | 5.33 s | 31.78% |
| SHA-256 assembly (`blockSHA2`) | 1.15 s | 6.86% |
| Memory copying (`memmove`) | 0.51 s | 3.04% |
| Byte comparison (`memequal`) | 0.32 s | 1.91% |
| Memory clearing (`memclrNoHeapPointers`) | 0.20 s | 1.19% |
| Coroutine switching (`coroswitch_m`) | 0.11 s | 0.66% |

Cumulative call-path samples include children and overlap; do not add them:

- `scanTailHashed`: 12.86 seconds (76.68% of labeled samples).
- `openRangeSource` → `checkClosedTail`: 5.83 seconds (34.76%). This is the full
  validation scan before callbacks can receive records from a frozen append file.
- `os.File.ReadAt`: 4.92 seconds (29.34%).
- Paths containing zstd functions: 0.23 seconds (1.37%). Only four closed ranges
  per round are rewritten with compression, and the payloads are repetitive.

Within `scanTailHashed`, source-line attribution places 4.58 seconds under group
body reads, 3.41 seconds under the group checksum check, and 3.81 seconds under
frame parsing and validation. These are also cumulative, overlapping views.

## Interpretation and limits

Reading and checking append-format history dominate this sample. Allocation,
copying, and iterator switching account for much smaller direct CPU costs.
Most closed files remain in append format across both rounds, so the profile
does not represent a history consisting entirely of compressed indexed files.

Frozen append files are fully validated when opened, then their records are
read and validated again for transformation. This implements the current
validation-before-callback contract. The profile quantifies its cost; it does
not establish that either validation pass can be removed safely or that its
entire CPU cost would translate to wall-time savings.

CPU profiling does not account for all storage waits or scheduling delays.
Background runtime work may lack the rewrite label, so labeled samples are not
a complete accounting of every cost caused by rewriting. SHA-256 samples include
the fixture's expected-digest updates as well as storage hashing. bbolt appears
high in cumulative stacks because its range iterator invokes rewrite work; that
does not attribute the child work to metadata processing.

This is one profiled run on an uncontrolled, warm VM. It is separate from the
[unprofiled timing measurements](full-size-churn.md), and does not establish a
plain-versus-zstd performance advantage or production latency distribution.

## Reproduction

On Linux, from the repository root:

```sh
mkdir -p .claude-scratch/rewrite-profile
go test -c ./pkg/segmentlog -o .claude-scratch/rewrite-profile/segmentlog.test
.claude-scratch/rewrite-profile/segmentlog.test -test.run '^$' \
  -test.bench '^BenchmarkLiveSegmentChurnFullSize$' -test.benchtime=1x \
  -test.timeout=15m -test.cpuprofile=.claude-scratch/rewrite-profile/cpu.pprof
go run cmd/pprof -top -relative_percentages -tagfocus=segmentlog.phase=rewrite \
  .claude-scratch/rewrite-profile/segmentlog.test .claude-scratch/rewrite-profile/cpu.pprof
```

Use `-tagfocus=segmentlog.codec=2` or `-tagfocus=segmentlog.round=1` to select an
encoding or round. These labels are only installed inside the rewrite scope.
Use `-cum` for cumulative stacks or `-list=scanTailHashed` for source attribution.
