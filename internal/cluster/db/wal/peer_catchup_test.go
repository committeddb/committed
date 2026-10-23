package wal

import (
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/committeddb/committed/internal/cluster/db"
)

// fetchedStream encodes framed records as ServeEvents sends them.
func fetchedStream(raws ...[]byte) []byte {
	var data []byte
	for _, raw := range raws {
		data = binary.AppendUvarint(data, uint64(len(raw)))
		data = append(data, raw...)
	}
	return data
}

// receiverSink is the receiving side of a fetch, as db/catchup.go drives it:
// it pins the generation, stages any segment file and hands it over, and
// appends record runs.
type receiverSink struct {
	recv *Storage
	gens []uint64
}

func (r *receiverSink) Begin(gen, _ uint64) error {
	r.gens = append(r.gens, gen)
	if r.recv.EventIndex() == 0 {
		return r.recv.SetEventLogGeneration(gen)
	}
	if mine := r.recv.EventLogGeneration(); mine != gen {
		return &db.EventGenerationMismatch{Have: gen, Want: mine}
	}
	return nil
}

func (r *receiverSink) Segment(string, int64, io.Reader) error { return db.ErrPeerSentSegmentFiles }

func (r *receiverSink) Records(data []byte) error { return r.recv.AppendFetchedRecords(data) }

func (r *receiverSink) End(db.EventServeResult) error { return nil }

// catchUp drives ServeEvents from peer into recv until recv holds every
// event through `to`, returning how many exchanges it took.
func catchUp(t *testing.T, peer, recv *Storage, to uint64) (calls int) {
	t.Helper()
	sink := &receiverSink{recv: recv}
	for {
		calls++
		require.Less(t, calls, 1000, "catch-up is not converging")
		res, err := peer.ServeEvents(context.Background(), recv.EventIndex(), to, sink)
		require.NoError(t, err)
		if recv.EventIndex() >= min(to, res.EventIndex) {
			require.False(t, res.More, "a complete fetch ends with nothing more")
			return calls
		}
		require.True(t, res.More || res.LastIndex > 0, "no progress and nothing more: %+v", res)
	}
}
