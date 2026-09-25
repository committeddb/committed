package wal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type workloadBackupFile struct {
	size   int64
	digest [32]byte
}

func captureWorkloadBackup(b *testing.B, s *Storage) map[string]workloadBackupFile {
	b.Helper()
	files := map[string]workloadBackupFile{}
	release := s.FreezeEventLayout()
	defer release()
	require.NoError(b, s.eventLog.backup.CaptureBackup(func(name string, size int64, write func(io.Writer) error) error {
		digest := sha256.New()
		if err := write(digest); err != nil {
			return err
		}
		var sum [32]byte
		copy(sum[:], digest.Sum(nil))
		files[name] = workloadBackupFile{size, sum}
		return nil
	}))
	return files
}

func workloadEntityEntry(b testing.TB, index uint64, entity *cluster.Entity) *pb.Entry {
	b.Helper()
	data, err := (&cluster.Proposal{Entities: []*cluster.Entity{entity}}).Marshal()
	require.NoError(b, err)
	return &pb.Entry{Index: proto.Uint64(index), Term: proto.Uint64(3), Type: pb.EntryNormal.Enum(), Data: data}
}

// BenchmarkBackupWorkload compares the production storage paths with identical
// synthetic records and RTBF requests. It measures event-log backup files only,
// not Raft/metadata archives, network transfer, or a hosting deduplication policy.
// Run with -benchtime=1x; each iteration creates a fresh durable log.
func BenchmarkBackupWorkload(b *testing.B) {
	const records = 32768
	const deleted = 128
	entries, typ, inputBytes := workloadEntries(b, records)
	for _, pattern := range []string{"localized", "distributed"} {
		for _, backend := range []string{"tidwall", "segmented"} {
			b.Run(pattern+"/"+backend, func(b *testing.B) {
				totals := map[string]float64{}
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					options := []Option{WithSafeMode()}
					if backend == "segmented" {
						options = append(options, WithSegmentedEventLog(segmentlog.LogOptions{
							Encoding: segmentlog.Options{Compression: segmentlog.ZstdDefault},
							Cache:    segmentlog.CacheOptions{RecentBytes: 160 << 20, HistoricalBytes: 160 << 20},
						}))
					}
					s, err := Open(b.TempDir(), nil, nil, nil, options...)
					require.NoError(b, err)
					b.Cleanup(func() { _ = s.Close() })
					// Drive existing compression explicitly to compare settled backups and
					// separate compression time from append time. No asynchronous scrub.
					s.stopSealer()
					b.StartTimer()
					apply := func(batch []*pb.Entry) {
						require.NoError(b, s.Save(&pb.HardState{Term: proto.Uint64(3), Commit: batch[len(batch)-1].Index}, batch, &pb.Snapshot{}))
						require.NoError(b, s.ApplyCommittedBatch(batch))
					}
					settle := func() {
						started := time.Now()
						if s.eventLog.compressor != nil {
							for {
								changed, err := s.eventLog.compressor.CompressNextSealed()
								require.NoError(b, err)
								if !changed {
									break
								}
							}
						}
						totals["compression-ms"] += float64(time.Since(started).Nanoseconds()) / 1e6
					}
					started := time.Now()
					for first := 0; first < len(entries); first += 256 {
						apply(entries[first:min(first+256, len(entries))])
					}
					totals["append-ns/record"] += float64(time.Since(started).Nanoseconds()) / records
					settle()
					before := captureWorkloadBackup(b, s)
					index := uint64(records + 2)
					deletes := make([]*pb.Entry, 0, deleted)
					for i := range deleted {
						key := i
						if pattern == "distributed" {
							key = i * records / deleted
						}
						deletes = append(deletes, workloadEntityEntry(b, index, cluster.NewDeleteEntity(typ, []byte(fmt.Sprint(key)))))
						index++
					}
					apply(deletes)
					command, err := cluster.NewScrubEntity(index-1, false)
					require.NoError(b, err)
					apply([]*pb.Entry{workloadEntityEntry(b, index, command)})
					started = time.Now()
					require.NoError(b, s.runPendingScrub())
					totals["scrub-ms"] += float64(time.Since(started).Nanoseconds()) / 1e6
					settle()
					after := captureWorkloadBackup(b, s)
					for _, f := range before {
						totals["before-bytes"] += float64(f.size)
					}
					for name, f := range after {
						totals["after-bytes"] += float64(f.size)
						if previous, exists := before[name]; exists && previous == f {
							totals["reused-bytes"] += float64(f.size)
							totals["reused-files"]++
						} else {
							totals["changed-bytes"] += float64(f.size)
						}
					}
					// Check that this workload actually performed erasure in both engines.
					for i := range deleted {
						key := i
						if pattern == "distributed" {
							key = i * records / deleted
						}
						_, err := s.ActualAt(uint64(key + 2))
						require.ErrorIs(b, err, ErrActualNotFound)
					}
					require.NoError(b, s.Close())
				}
				for unit, total := range totals {
					b.ReportMetric(total/float64(b.N), unit)
				}
				b.ReportMetric(float64(inputBytes), "input-bytes")
			})
		}
	}
}

// workloadEntries supplies identical deterministic input for storage workloads.
func workloadEntries(b testing.TB, records int) ([]*pb.Entry, *cluster.Type, int) {
	b.Helper()
	typ := &cluster.Type{ID: "items", Name: "items", Version: 1}
	registration, err := cluster.NewUpsertTypeEntity(typ)
	require.NoError(b, err)
	entries := make([]*pb.Entry, 0, records+1)
	entries = append(entries, workloadEntityEntry(b, 1, registration))
	rng := rand.New(rand.NewSource(42)) // deterministic synthetic data, not security material
	var entropy [1024]byte
	var inputBytes int
	for i := range records {
		_, err := rng.Read(entropy[:])
		require.NoError(b, err)
		payload := fmt.Sprintf(`{"order":%d,"customer":%d,"status":"paid","trace":"%s","description":"%s"}`,
			i, i%1000, hex.EncodeToString(entropy[:]), strings.Repeat("standard order line item;", 80))
		raw := experimentEntry(b, uint64(i+2), pb.EntryNormal, experimentRow(fmt.Sprint(i), payload))
		entry := new(pb.Entry)
		require.NoError(b, proto.Unmarshal(raw, entry))
		entries = append(entries, entry)
		inputBytes += len(raw)
	}
	return entries, typ, inputBytes
}
