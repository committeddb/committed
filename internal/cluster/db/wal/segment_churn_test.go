package wal

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/clusterpb"
	"github.com/committeddb/committed/pkg/segmentlog"
)

type (
	churnFile struct {
		hash [32]byte
		size int64
	}
	churnInventory map[string]churnFile
)

func inventoryChurn(t testing.TB, path string, segmented bool) (churnInventory, int64) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	result := churnInventory{}
	var metadata int64
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(path, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if segmented && !strings.HasSuffix(entry.Name(), ".seg") && !strings.HasSuffix(entry.Name(), ".active") {
			metadata += int64(len(data))
			continue
		}
		result[entry.Name()] = churnFile{sha256.Sum256(data), int64(len(data))}
	}
	return result, metadata
}

func churnSizes(before, after churnInventory, replaceAll bool) (stored, written, novel int64, retained int) {
	hashes := map[[32]byte]bool{}
	for _, f := range before {
		hashes[f.hash] = true
	}
	for name, f := range after {
		stored += f.size
		if !replaceAll && before[name] == f {
			retained++
		} else {
			written += f.size
		}
		if !hashes[f.hash] {
			novel += f.size
		}
	}
	return
}

func churnFixture(t testing.TB, mode string) ([][]byte, map[string]uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(7, 19))
	records := make([][]byte, 0, 550)
	selected := make([]string, 0, 32)
	for i := 1; i <= 512; i++ {
		key := fmt.Sprintf("subject-%04d", i)
		// Synthetic JSON-shaped data with both repeated structure and deterministic
		// varying values. This is not sampled production data.
		var value strings.Builder
		value.WriteString(`{"description":"example event","value":"`)
		for range 1024 {
			value.WriteByte("abcdefghijklmnopqrstuvwxyz0123456789"[rng.IntN(36)])
		}
		value.WriteString(`"}`)
		records = append(records, experimentEntry(t, uint64(i*10), pb.EntryNormal, experimentRow(key, value.String()), experimentRow("audit", "retain this entity")))
		if (mode == "isolated" && i == 1) || (mode == "scattered" && i%16 == 1) {
			selected = append(selected, key)
		}
	}
	selections := map[string]uint64{}
	for i, key := range selected {
		index := uint64(6000 + i)
		selections[string(tombstoneKey("items", []byte(key)))] = index
		entity := &clusterpb.LogEntity{Type: &clusterpb.TypeRef{ID: "items", Version: 1}, Body: &clusterpb.LogEntity_Delete{Delete: &clusterpb.LogDelete{Key: []byte(key)}}}
		records = append(records, experimentEntry(t, index, pb.EntryNormal, entity))
	}
	return records, selections
}

func drainChurnCompression(t testing.TB, log *tidwal.Log) {
	t.Helper()
	for {
		done, err := log.CompressNextSealed()
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			return
		}
	}
}

// TestSegmentRewriteChurnExperiment is a deterministic filesystem experiment,
// not a latency benchmark or a production backup-cost estimate. It compares
// survivor bytes and completed-file inventories after both compression drains.
func TestSegmentRewriteChurnExperiment(t *testing.T) {
	const target = 32 << 10
	t.Log("codec workload backend stored_bytes new_file_bytes new_hash_bytes retained_files total_files metadata_bytes")
	for _, compressed := range []bool{false, true} {
		codec := "plain"
		encoding := segmentlog.NoCompression
		options := tidwal.Options{SegmentSize: target, NoSync: true}
		if compressed {
			codec = "zstd"
			encoding = segmentlog.ZstdDefault
			options.SealedSegmentCompression = tidwal.CompressionZstd
		}
		for _, mode := range []string{"noop", "isolated", "scattered"} {
			t.Run(codec+"/"+mode, func(t *testing.T) {
				records, selections := churnFixture(t, mode)
				sourcePath, targetPath, segmentPath := t.TempDir(), t.TempDir(), t.TempDir()
				source, err := tidwal.Open(sourcePath, &options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = source.Close() })
				for i, raw := range records {
					if err := source.Write(uint64(i+1), frame(raw)); err != nil {
						t.Fatal(err)
					}
				}
				if err := source.Sync(); err != nil {
					t.Fatal(err)
				}
				if compressed {
					drainChurnCompression(t, source)
				}
				oldLegacy, _ := inventoryChurn(t, sourcePath, false)
				log, err := segmentlog.CreateLog(segmentPath, 1, segmentlog.LogOptions{SegmentBytes: target, Encoding: segmentlog.Options{Compression: encoding}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = log.Close() })
				adapter := &segmentEventLog{log: log}
				if err := adapter.appendRaw(records); err != nil {
					t.Fatal(err)
				}
				if _, err := log.Reclaim(t.Context()); err != nil {
					t.Fatal(err)
				}
				oldSegments, _ := inventoryChurn(t, segmentPath, true)
				filter := func(raw []byte) (bool, []byte, error) { return scrubFilterEntry(raw, selections, nil, 0) }
				// Match the existing scrub copy primitive: read and verify old frames,
				// transform, write every survivor to a fresh dense log, sync and compress.
				rewritten, err := tidwal.Open(targetPath, &options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = rewritten.Close() })
				var count uint64
				for i := range records {
					framed, err := source.Read(uint64(i + 1))
					if err != nil {
						t.Fatal(err)
					}
					raw, err := unframe(framed)
					if err != nil {
						t.Fatal(err)
					}
					keep, payload, err := filter(raw)
					if err != nil {
						t.Fatal(err)
					}
					if keep {
						count++
						if err := rewritten.Write(count, frame(payload)); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := rewritten.Sync(); err != nil {
					t.Fatal(err)
				}
				if compressed {
					drainChurnCompression(t, rewritten)
				}
				result, err := adapter.rewriteRaw(t.Context(), 1, filter)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := log.Reclaim(t.Context()); err != nil {
					t.Fatal(err)
				}
				// Compare each surviving protobuf byte and index, and reject extra survivors.
				var scanned uint64
				err = adapter.scanRaw(t.Context(), segmentlog.Coverage{Start: 1, End: ^uint64(0)}, func(id uint64, actual []byte) error {
					scanned++
					if scanned > count {
						return errors.New("unexpected trailing record")
					}
					framed, err := rewritten.Read(scanned)
					if err != nil {
						return err
					}
					expected, err := unframe(framed)
					if err != nil {
						return err
					}
					entry := new(pb.Entry)
					if err := proto.Unmarshal(expected, entry); err != nil {
						return err
					}
					if id != entry.GetIndex() || !bytes.Equal(actual, expected) {
						return fmt.Errorf("survivors differ at index %d", id)
					}
					return nil
				})
				if err != nil || scanned != count {
					t.Fatal("survivor scan mismatch", scanned, count, err)
				}
				newLegacy, _ := inventoryChurn(t, targetPath, false)
				newSegments, metadata := inventoryChurn(t, segmentPath, true)
				ls, lw, ln, lr := churnSizes(oldLegacy, newLegacy, true)
				ss, sw, sn, sr := churnSizes(oldSegments, newSegments, false)
				t.Logf("%s %s tidwall %d %d %d %d %d 0", codec, mode, ls, lw, ln, lr, len(newLegacy))
				t.Logf("%s %s segmentlog %d %d %d %d %d %d", codec, mode, ss, sw, sn, sr, len(newSegments), metadata)
				if mode == "noop" && (sw != 0 || sn != 0 || result.ChangedSegments != 0 || result.TailChanged) {
					t.Fatal("no-op changed payload files", result)
				}
				if mode == "isolated" && (result.ChangedSegments != 1 || result.TailChanged || sr != len(oldSegments)-1 || sw >= lw) {
					t.Fatal("isolated edit churned unrelated files", result, sr, sw, lw)
				}
			})
		}
	}
}
