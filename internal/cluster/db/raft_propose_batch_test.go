package db

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func queued(t *testing.T, props ...[]byte) *Raft {
	t.Helper()
	ch := make(chan []byte, len(props)+1)
	for _, p := range props {
		ch <- p
	}
	return &Raft{proposeC: ch}
}

func entryData(t *testing.T, n *Raft, first []byte) ([][]byte, bool) {
	t.Helper()
	entries, open := n.collectProposals(first)
	out := make([][]byte, len(entries))
	for i, e := range entries {
		out[i] = e.Data
	}
	return out, open
}

// The proposals already queued join the first, in the order they were
// submitted, and the reader does not block once the queue is empty.
func TestCollectProposals_TakesTheQueueInOrder(t *testing.T) {
	n := queued(t, []byte("b"), []byte("c"), []byte("d"))
	got, open := entryData(t, n, []byte("a"))
	require.True(t, open)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, got)

	got, open = entryData(t, n, []byte("e"))
	require.True(t, open)
	require.Equal(t, [][]byte{[]byte("e")}, got, "an empty queue leaves a batch of one")
}

// A batch stops at the entry cap and leaves the rest queued, in order, for
// the next.
func TestCollectProposals_EntryCap(t *testing.T) {
	props := make([][]byte, 0, maxProposeBatchEntries+10)
	for i := range maxProposeBatchEntries + 10 {
		props = append(props, fmt.Appendf(nil, "p%d", i))
	}
	n := queued(t, props...)
	got, _ := entryData(t, n, []byte("first"))
	require.Len(t, got, maxProposeBatchEntries)
	require.Equal(t, props[:maxProposeBatchEntries-1], got[1:])

	rest, _ := entryData(t, n, []byte("next"))
	require.Equal(t, append([][]byte{[]byte("next")}, props[maxProposeBatchEntries-1:]...), rest)
}

// A batch stops taking proposals once it reaches the byte cap; a proposal
// at the cap goes alone.
func TestCollectProposals_ByteCap(t *testing.T) {
	big := bytes.Repeat([]byte("x"), maxProposeBatchBytes/3+1)
	n := queued(t, big, big, big)
	got, _ := entryData(t, n, big)
	require.Len(t, got, 3, "three thirds-plus reach the cap")
	got, _ = entryData(t, n, bytes.Repeat([]byte("y"), maxProposeBatchBytes))
	require.Len(t, got, 1, "a proposal at the cap goes alone")
}

// A closed proposeC ends the batch with what was queued and reports it, so
// the reader proposes them and then stops reading.
func TestCollectProposals_ClosedChannel(t *testing.T) {
	ch := make(chan []byte, 2)
	ch <- []byte("b")
	close(ch)
	n := &Raft{proposeC: ch}
	got, open := entryData(t, n, []byte("a"))
	require.False(t, open)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, got)
}
