package segmentlog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type pausedRewriteVerification struct {
	layout
	entered, release chan struct{}
	failure          error
}

func (v pausedRewriteVerification) verifyRewrite(changed []SegmentRef, active *TailRef) (*verifiedRewriteFiles, error) {
	close(v.entered)
	<-v.release
	if v.failure != nil {
		return nil, v.failure
	}
	return v.layout.verifyRewrite(changed, active)
}

func TestRewriteVerificationAllowsRollover(t *testing.T) {
	for _, mode := range []string{"publish", "cancel", "failure"} {
		t.Run(mode, func(t *testing.T) {
			l := cachedLog(t, 42)
			if err := l.Append([]Record{{1, []byte("value")}, {10, []byte("value")}, {20, []byte("value")}}); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var failure error
			if mode == "failure" {
				failure = errors.New("verification failed")
			}
			l.catalog = pausedRewriteVerification{l.catalog, entered, release, failure}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := l.RewriteSealed(ctx, 1, func(r Record) ([]byte, bool, error) { r.Payload[0] = 'X'; return r.Payload, true, nil })
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("verification did not start")
			}
			access := make(chan error, 1)
			go func() {
				r, err := l.Read(1)
				if err == nil && string(r.Payload) != "value" {
					err = errors.New("replacement visible before publication")
				}
				if err == nil {
					err = l.Append([]Record{{30, []byte("later")}, {40, []byte("later")}})
				}
				access <- err
			}()
			if err := awaitOperation(t, access); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				cancel()
			}
			unblock()
			err := awaitOperation(t, done)
			switch mode {
			case "publish":
				if err != nil {
					t.Fatal(err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "failure":
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
			}
			l = reopenLog(t, l)
			want := "value"
			if mode == "publish" {
				want = "Xalue"
			}
			for id, payload := range map[uint64]string{1: want, 10: want, 20: "value", 30: "later", 40: "later"} {
				r, err := l.Read(id)
				if err != nil || string(r.Payload) != payload {
					t.Fatalf("record %d: %q, %v", id, r.Payload, err)
				}
			}
			if err := l.Verify(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRewriteVerificationBoundToCatalogAndSingleUse(t *testing.T) {
	l := rotatedLog(t)
	other := rotatedLog(t)
	c, err := l.InspectCatalog()
	if err != nil {
		t.Fatal(err)
	}
	files, err := l.catalog.verifyRewrite(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.catalog.publishRewrite(c.Revision, 1, files); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := l.catalog.publishRewrite(c.Revision, 1, files); err != nil {
		t.Fatal(err)
	}
	if err := l.catalog.publishRewrite(c.Revision+1, 2, files); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
