package segmentlog

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/committeddb/committed/pkg/segmentlog/internal/format"
)

func compressed(t testing.TB, policy Compression, records ...Record) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := WriteSegment(&b, Coverage{0, 1000}, sequence(records...), Options{BlockSize: 4096, Compression: policy}); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestCompressionPolicies(t *testing.T) {
	records := []Record{{1, bytes.Repeat([]byte("private public "), 300)}, {10, bytes.Repeat([]byte("repeat "), 100)}, {900, []byte("tail")}}
	plain := compressed(t, NoCompression, records...)
	for policy := ZstdFast; policy <= ZstdBest; policy++ {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			data := compressed(t, policy, records...)
			if len(data) >= len(plain) {
				t.Fatal("compressible input did not shrink")
			}
			if !bytes.Equal(data, compressed(t, policy, records...)) {
				t.Fatal("encoding is not reproducible")
			}
			s := open(t, data)
			if got := collect(t, s); !reflect.DeepEqual(got, records) {
				t.Fatal("roundtrip differs")
			}
			r, err := s.Seek(2)
			if err != nil || r.ID != 10 {
				t.Fatal("sparse seek", err)
			}
			var reads int
			changed, err := s.Rewrite(context.Background(), func() (io.Writer, error) { t.Fatal("no-op created output"); return nil, nil }, func(r Record) ([]byte, bool, error) { reads++; return r.Payload, true, nil }, Options{Compression: NoCompression})
			if err != nil || changed || reads != len(records) {
				t.Fatal("no-op", changed, reads, err)
			}
			var replacement bytes.Buffer
			changed, err = s.Rewrite(context.Background(), func() (io.Writer, error) { return &replacement, nil }, func(r Record) ([]byte, bool, error) {
				if r.ID == 1 {
					return bytes.ReplaceAll(r.Payload, []byte("private "), nil), true, nil
				}
				return r.Payload, r.ID != 10, nil
			}, Options{Compression: policy})
			if err != nil || !changed {
				t.Fatal("rewrite", err)
			}
			rewritten := open(t, replacement.Bytes())
			if rewritten.Coverage() != s.Coverage() || rewritten.Count() != 2 {
				t.Fatal("rewrite metadata")
			}
			if _, err := rewritten.Read(10); !errors.Is(err, ErrNotFound) {
				t.Fatal(err)
			}
			r, err = rewritten.Read(1)
			if err != nil || bytes.Contains(r.Payload, []byte("private")) {
				t.Fatal("partial replacement", err)
			}
		})
	}
}

func TestMixedBlockCodecs(t *testing.T) {
	random := make([]byte, 4096)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range random {
		random[i] = byte(rng.Uint32())
	}
	data := compressed(t, ZstdDefault, Record{1, bytes.Repeat([]byte("a"), 4096)}, Record{2, random})
	s := open(t, data)
	if len(s.blocks) != 2 || s.blocks[0].codec != format.Zstd || s.blocks[1].codec != format.Plain {
		t.Fatal("expected compressed block followed by plain fallback", s.blocks)
	}
	if err := s.Verify(); err != nil {
		t.Fatal(err)
	}
	// A lookup reads just its own block, not every compressed block in the file.
	reader := &trackingReader{ReaderAt: bytes.NewReader(data)}
	s, err := OpenSegment(reader, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	reader.offsets = nil
	if _, err := s.Read(2); err != nil {
		t.Fatal(err)
	}
	if len(reader.offsets) != 1 || reader.offsets[0] != int64(s.blocks[1].offset) {
		t.Fatal("lookup read unrelated blocks", reader.offsets)
	}
}

type trackingReader struct {
	io.ReaderAt
	offsets []int64
}

func (r *trackingReader) ReadAt(b []byte, offset int64) (int, error) {
	r.offsets = append(r.offsets, offset)
	return r.ReaderAt.ReadAt(b, offset)
}

func TestFormatZeroFixture(t *testing.T) {
	// Fixed independently encoded format-0 segment: range [0,1000), ID 7, "old".
	data, err := hex.DecodeString("5345474c4f473030000000000000000000000000e803000000000000349a76750300000007000000000000006f6c64392d228e0700000000000000070000000000000020000000000000001300000001000000c74b674800000000330000000000000001000000010000000000000033e6ea6e454e4430ed8597bd")
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, data)
	r, err := s.Read(7)
	if err != nil || string(r.Payload) != "old" || s.Count() != 1 {
		t.Fatal("legacy fixture", r, err)
	}
}

func TestCompressedCorruption(t *testing.T) {
	data := compressed(t, ZstdDefault, Record{1, bytes.Repeat([]byte("a"), 4096)})
	for i := range data {
		bad := bytes.Clone(data)
		bad[i] ^= 1
		s, err := OpenSegment(bytes.NewReader(bad), int64(len(bad)))
		if err == nil {
			err = s.Verify()
		}
		if err == nil {
			t.Fatalf("accepted corrupt byte %d", i)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func([]byte)
		want   error
	}{
		{"unknown codec", func(e []byte) { format.LE.PutUint16(e[40:], 99) }, ErrUnsupported},
		{"reserved", func(e []byte) { e[42] = 1 }, ErrUnsupported},
		{"decoded too big", func(e []byte) { format.LE.PutUint32(e[36:], format.MaxBlock+1) }, ErrCorrupt},
		{"decoded too small", func(e []byte) { format.LE.PutUint32(e[36:], 16) }, ErrCorrupt},
		{"decoded mismatch", func(e []byte) { format.LE.PutUint32(e[36:], 5000) }, ErrCorrupt},
		{"invalid stream", func(e []byte) {}, ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := bytes.Clone(data)
			f := bad[len(bad)-format.FooterSize:]
			index := bad[int(format.LE.Uint64(f)) : len(bad)-format.FooterSize]
			tc.mutate(index)
			if tc.name == "invalid stream" {
				bad[format.HeaderSize] ^= 255
				format.LE.PutUint32(index[32:], format.CRC(bad[format.HeaderSize:int(format.LE.Uint64(f))]))
			}
			format.LE.PutUint32(f[20:], format.CRC(index))
			format.LE.PutUint32(f[28:], format.CRC(f[:28]))
			s, err := OpenSegment(bytes.NewReader(bad), int64(len(bad)))
			if err == nil {
				err = s.Verify()
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
		})
	}
}

func TestCompressionLimits(t *testing.T) {
	var out bytes.Buffer
	if err := WriteSegment(&out, Coverage{0, 1000}, sequence(), Options{Compression: 255}); !errors.Is(err, ErrInvalid) || out.Len() != 0 {
		t.Fatal("invalid policy", err)
	}
	data := compressed(t, ZstdFast, Record{1, bytes.Repeat([]byte("a"), format.MaxPayload)})
	if err := open(t, data).Verify(); err != nil {
		t.Fatal("maximum payload", err)
	}
}

func BenchmarkCompression(b *testing.B) {
	for _, random := range []bool{false, true} {
		records := make([]Record, 256)
		rng := rand.New(rand.NewPCG(1, 2))
		for i := range records {
			payload := bytes.Repeat([]byte(`{"topic":"orders","customer":"example","status":"completed"}`), 70)
			if random {
				for j := range payload {
					payload[j] = byte(rng.Uint32())
				}
			}
			records[i] = Record{uint64(i * 2), payload}
		}
		var input int
		for _, r := range records {
			input += len(r.Payload)
		}
		for _, size := range []int{256 << 10, 1 << 20} {
			for policy := NoCompression; policy <= ZstdBest; policy++ {
				name := fmt.Sprintf("random=%t/block=%d/policy=%d", random, size, policy)
				opts := Options{BlockSize: size, Compression: policy}
				var sample bytes.Buffer
				if err := WriteSegment(&sample, Coverage{0, 1000}, sequence(records...), opts); err != nil {
					b.Fatal(err)
				}
				b.Run(name+"/write", func(b *testing.B) {
					b.SetBytes(int64(input))
					b.ReportAllocs()
					for b.Loop() {
						if err := WriteSegment(io.Discard, Coverage{0, 1000}, sequence(records...), opts); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportMetric(float64(sample.Len())/float64(input), "stored/input")
				})
				s := open(b, sample.Bytes())
				b.Run(name+"/seek", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if _, err := s.Seek(251); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}

func TestConcurrentCompressedReads(t *testing.T) {
	s := open(t, compressed(t, ZstdDefault, Record{1, bytes.Repeat([]byte("payload"), 1000)}))
	for i := range 4 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			for range 20 {
				r, err := s.Read(1)
				if err != nil || len(r.Payload) != 7000 {
					t.Fatal("compressed read", err)
				}
				r.Payload[0] = 'X'
			}
		})
	}
}
