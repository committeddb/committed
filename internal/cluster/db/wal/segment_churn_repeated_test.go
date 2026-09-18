package wal

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	tidwal "github.com/tidwall/wal"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/committeddb/committed/internal/cluster/db/eventlog"
	"github.com/committeddb/committed/internal/cluster/db/eventlog/segmented"
	"github.com/committeddb/committed/pkg/segmentlog"
)

func churnCatalog(t *testing.T, log *segmentlog.Log) segmentlog.Catalog {
	t.Helper()
	catalog, err := log.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func repeatedChurnEntry(t *testing.T, i int) []byte {
	t.Helper()
	rng := rand.New(rand.NewPCG(uint64(i), 19))
	payload := make([]byte, 1024)
	for j := range payload {
		payload[j] = "abcdefghijklmnopqrstuvwxyz0123456789"[rng.IntN(36)]
	}
	row := experimentRow(fmt.Sprintf("subject-%04d", i), string(payload))
	if i == 1 {
		return experimentEntry(t, uint64(i*10), pb.EntryNormal, row, experimentRow("audit", "retain"))
	}
	return experimentEntry(t, uint64(i*10), pb.EntryNormal, row)
}

// Repeated scrubs use the legacy copy primitive as a byte-for-byte oracle.
// This measures completed-file churn, not physical I/O or actual backup traffic.
func TestSegmentRepeatedRewriteChurn(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		codec := "plain"
		encoding := segmentlog.NoCompression
		options := tidwal.Options{SegmentSize: 8 << 10, NoSync: true}
		if compressed {
			codec = "zstd"
			encoding = segmentlog.ZstdDefault
			options.SealedSegmentCompression = tidwal.CompressionZstd
		}
		t.Run(codec, func(t *testing.T) {
			path, legacyPath := t.TempDir(), t.TempDir()
			engine, err := segmentlog.CreateLog(path, 1, segmentlog.LogOptions{SegmentBytes: 8 << 10, Encoding: segmentlog.Options{Compression: encoding}})
			if err != nil {
				t.Fatal(err)
			}
			log := segmented.Wrap(engine)
			t.Cleanup(func() { _ = log.Close() })
			adapter := &eventLogAdapter{log: log}
			legacy, err := tidwal.Open(legacyPath, &options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = legacy.Close() })
			appendRange := func(start, end int) {
				t.Helper()
				raws := make([][]byte, 0, end-start)
				last, e := legacy.LastIndex()
				if e != nil {
					t.Fatal(e)
				}
				for i := start; i < end; i++ {
					raw := repeatedChurnEntry(t, i)
					raws = append(raws, raw)
					last++
					if e := legacy.Write(last, frame(raw)); e != nil {
						t.Fatal(e)
					}
				}
				if e := legacy.Sync(); e != nil {
					t.Fatal(e)
				}
				if compressed {
					drainChurnCompression(t, legacy)
				}
				if e := adapter.appendRaw(raws); e != nil {
					t.Fatal(e)
				}
				if _, e := log.Reclaim(t.Context()); e != nil {
					t.Fatal(e)
				}
			}
			appendRange(1, 65)
			selections := map[string]uint64{}
			selectRange := func(start, end uint64) {
				for i := 1; i <= 80; i++ {
					id := uint64(i * 10)
					if id >= start && id < end {
						selections[string(tombstoneKey("items", []byte(fmt.Sprintf("subject-%04d", i))))] = 10000
					}
				}
			}
			var totalLegacy, totalSegment int64
			for step, mode := range []string{"noop", "partial-record", "whole-range", "active-tail", "repeat", "append-and-repeat"} {
				if mode == "append-and-repeat" {
					appendRange(65, 81)
				}
				before := churnCatalog(t, engine)
				if len(before.Segments) < 3 {
					t.Fatal("fixture lacks sealed ranges")
				}
				switch mode {
				case "partial-record":
					selectRange(10, 11)
				case "whole-range":
					selectRange(before.Segments[1].Coverage.Start, before.Segments[1].Coverage.End)
				case "active-tail":
					selectRange(before.Active.Start, 641)
				}
				oldSegments, _ := inventoryChurn(t, path, true)
				oldLegacy, _ := inventoryChurn(t, legacyPath, false)
				targetPath := t.TempDir()
				replacement, e := tidwal.Open(targetPath, &options)
				if e != nil {
					t.Fatal(e)
				}
				// Retain a cleanup even if a later assertion aborts before adoption.
				t.Cleanup(func() { _ = replacement.Close() })
				changed := map[uint64]bool{}
				count := uint64(0)
				last, e := legacy.LastIndex()
				if e != nil {
					t.Fatal(e)
				}
				filter := func(raw []byte) (bool, []byte, error) { return scrubFilterEntry(raw, selections, nil, 0) }
				for seq := uint64(1); seq <= last; seq++ {
					framed, e := legacy.Read(seq)
					if e != nil {
						t.Fatal(e)
					}
					raw, e := unframe(framed)
					if e != nil {
						t.Fatal(e)
					}
					keep, payload, e := filter(raw)
					if e != nil {
						t.Fatal(e)
					}
					entry := new(pb.Entry)
					if e := proto.Unmarshal(raw, entry); e != nil {
						t.Fatal(e)
					}
					if !keep || !bytes.Equal(raw, payload) {
						changed[entry.GetIndex()] = true
					}
					if keep {
						count++
						if e := replacement.Write(count, frame(payload)); e != nil {
							t.Fatal(e)
						}
					}
				}
				if e := replacement.Sync(); e != nil {
					t.Fatal(e)
				}
				if compressed {
					drainChurnCompression(t, replacement)
				}
				result, e := adapter.rewriteRaw(t.Context(), uint64(step+1), filter)
				if e != nil || !result.Published || result.ChangedRecords != uint64(len(changed)) {
					t.Fatal(mode, result, changed, e)
				}
				if _, e := log.Reclaim(t.Context()); e != nil {
					t.Fatal(e)
				}
				after := churnCatalog(t, engine)
				if len(after.Segments) != len(before.Segments) {
					t.Fatal(mode, "changed sealed range count")
				}
				for i, previous := range before.Segments {
					affected := false
					for id := range changed {
						if id >= previous.Coverage.Start && id < previous.Coverage.End {
							affected = true
						}
					}
					current := after.Segments[i]
					if previous.Coverage != current.Coverage {
						t.Fatal(mode, "shifted range")
					}
					if !affected && previous != current {
						t.Fatal(mode, "rewrote unrelated sealed range", previous.Coverage)
					}
				}
				if mode == "whole-range" && (after.Segments[1].File != "" || after.Segments[1].Count != 0) {
					t.Fatal("erased range still has a payload file")
				}
				if mode == "active-tail" && (len(changed) == 0 || before.Active.File == after.Active.File || before.Active.Start != after.Active.Start) {
					t.Fatal("fixture did not rewrite active tail")
				}
				if e := log.Close(); e != nil {
					t.Fatal(e)
				}
				engine, e = segmentlog.OpenLog(path, segmentlog.Options{Compression: encoding})
				if e != nil {
					t.Fatal(e)
				}
				log = segmented.Wrap(engine)
				adapter = &eventLogAdapter{log: log}
				frontier := uint64(640)
				if mode == "append-and-repeat" {
					frontier = 800
				}
				if head, e := adapter.eventIndex(); e != nil || head != frontier {
					t.Fatal("lost original append progress", head, e)
				}
				scanned := uint64(0)
				e = adapter.scanRaw(t.Context(), eventlog.Coverage{Start: 1, End: ^uint64(0)}, func(id uint64, raw []byte) error {
					scanned++
					if scanned > count {
						return fmt.Errorf("extra survivor %d", id)
					}
					framed, e := replacement.Read(scanned)
					if e != nil {
						return e
					}
					want, e := unframe(framed)
					if e != nil {
						return e
					}
					if !bytes.Equal(raw, want) {
						return fmt.Errorf("survivor differs at %d", id)
					}
					return nil
				})
				if e != nil || scanned != count {
					t.Fatal(mode, scanned, count, e)
				}
				newSegments, metadata := inventoryChurn(t, path, true)
				newLegacy, _ := inventoryChurn(t, targetPath, false)
				_, lw, ln, _ := churnSizes(oldLegacy, newLegacy, true)
				_, sw, sn, retained := churnSizes(oldSegments, newSegments, false)
				if len(changed) == 0 && (sw != 0 || sn != 0) {
					t.Fatal(mode, "no-op changed payload files")
				}
				if mode == "whole-range" {
					if _, exists := newSegments[before.Segments[1].File]; exists || sw != 0 {
						t.Fatal("range erasure retained old data or created payload files")
					}
				}
				if mode == "active-tail" {
					if _, exists := newSegments[before.Active.File]; exists || sw != newSegments[after.Active.File].size {
						t.Fatal("tail erasure retained old data or replaced unrelated payloads")
					}
				}
				// Verify retained descriptors correspond to actual unchanged file bytes.
				for _, previous := range before.Segments {
					if previous.File != "" {
						if current, ok := newSegments[previous.File]; ok && current != oldSegments[previous.File] {
							t.Fatal("mutated sealed file in place")
						}
					}
				}
				totalLegacy += lw
				totalSegment += sw
				t.Logf("%s changed=%d legacy_new=%d segment_new=%d legacy_new_hash=%d segment_new_hash=%d retained=%d metadata=%d", mode, len(changed), lw, sw, ln, sn, retained, metadata)
				if e := legacy.Close(); e != nil {
					t.Fatal(e)
				}
				legacy, legacyPath = replacement, targetPath
			}
			t.Logf("rewrite-only totals: legacy=%d segmented=%d", totalLegacy, totalSegment)
		})
	}
}
