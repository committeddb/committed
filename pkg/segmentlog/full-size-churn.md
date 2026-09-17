# Full-size selective rewrite experiment

`BenchmarkLiveSegmentChurnFullSize` creates 64 closed ranges and a full active
tail through managed append, each with a 20 MiB original-frame target. Each range
contains 5,120 records with 4,080-byte payloads and sparse IDs spaced by three.
The history contains 332,800 records and 1,300 MiB of original record frames,
plus append-group framing and file headers. Initial append files are plain in
both encoding modes.

## Workload and checks

Two rewrite/reclaim/reopen rounds revisit four scattered closed ranges and the
active tail. The first round erases the first record in each affected range;
every round changes one payload byte in every remaining record in those ranges.
This ensures both rounds produce replacement files. The other 60 closed files
must retain identical catalog references, including filenames and digests.

Appends are streamed in batches of 64. The test oracle retains expected SHA-256
digests by record ID rather than a duplicate of the full payload history. Oracle
memory still grows with record count. After each reopen, every survivor's ID and
payload digest must match, and the survivor count must be exact. Reclamation must
remove exactly the replaced files. A final append must roll over despite erased
records; another reopen verifies it, full verification checks all selected files,
and orphan reclamation must find no unpublished files.

`TestLiveChurnBatchedFixture` exercises the same machinery with 16 closed 128 KiB
ranges and an active tail, in both encoding modes. The existing tiny-segment
churn tests also use the streaming fixture and expected digest map.

## Metrics and interpretation

- `initial-payload-file-B`: initial selected append-file bytes, excluding metadata.
- `replacement-B/round`: mean newly selected file bytes per rewrite, including
  the replacement active tail and excluding unchanged files and metadata.
- `retired-B/round`: mean obsolete payload bytes physically removed by reclamation.
  First-round retirements remove original plain append files; later retirements
  remove files in their rewrite encoding, so these rounds can differ greatly.
- Scrub, reclaim, and reopen timings are means of two rounds. Scrub timing includes
  the transform and its expected-digest updates. Final verification is untimed.
- `boundary-ms`: one post-churn append; `metadata-end-B`: final metadata file size.

The workload still scans the full history to identify affected records; stable
files reduce replacement bytes, not selection scan cost. Zstd applies to rewritten
closed files. The active tail remains uncompressed, even in the zstd case.
Payloads are deliberately repetitive except for the embedded ID and changed byte,
so compression ratios do not represent typical customer data.

This measures local replacement and reclamation, not S3 billing or an implemented
backup protocol. Backup object selection, metadata transfer, retention, and remote
erasure are outside this experiment. Two rounds over 65 files do not establish
100 TB scale, long-term fragmentation, or behavior with millions of files.

## Recorded Linux run

September 17, 2026, Go 1.26.6, Linux/arm64 in Alpine 3.20 on the local OrbStack VM.
Each encoding ran once with a fresh history on a disposable container overlay
filesystem, sequentially after tests and lint completed. The host and VM were
not load-controlled. Timings are means of two rounds within that single run,
not latency distributions or production throughput measurements. Filesystem
caches were warm; this does not establish cold recovery or power-loss behavior.

Both runs passed all checks. Each started with 1,363,400,480 selected payload-file
bytes (1,300.24 MiB). Every round replaced four closed files plus the active tail,
preserved the other 60 closed references, and reclaimed five obsolete files.
Final metadata size was 128 KiB in both cases.

| Rewrite encoding | Replacement MiB/round | Retired MiB/round | Scrub seconds/round | Reclaim ms/round | Reopen ms/round | Post-churn boundary ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain | 100.23 | 100.12 | 6.680 | 41.74 | 62.22 | 13.35 |
| ZstdDefault | 20.46 | 60.24 | 5.438 | 48.60 | 62.71 | 28.77 |

Plain replacement bytes were about 7.71% of initial payload-file size per round;
zstd replacement bytes were about 1.57%. The zstd result is dominated by the
uncompressed active tail and depends on the deliberately repetitive payloads.
These percentages describe this selective local rewrite, not backup cost savings.
The lower scrub time in the one zstd run is not evidence of a general speedup.

Scrubs still took seconds and held the managed log mutex throughout, blocking
reads and appends. Preserving most files reduces byte churn but does not avoid
full-history selection work. The small normal-suite fixtures passed under the
macOS race detector and on Linux; these full-size Linux runs were not race-enabled.

## Reproduction

```sh
go test -race ./pkg/segmentlog -run '^TestLive(SegmentChurn|ChurnBatchedFixture)$' -count=1
go test ./pkg/segmentlog -run '^$' -bench '^BenchmarkLiveSegmentChurnFullSize$' -benchtime=1x -count=1 -timeout=15m
```

Each encoding creates a fresh history. Overall Go `ns/op` includes fixture
construction and correctness checks; use the named metrics for individual phases.
