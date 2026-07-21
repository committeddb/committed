package db

import (
	"errors"
	"testing"

	"github.com/committeddb/committed/internal/cluster"
)

// TestFailurePlaneAdmittedAtDiskFull pins the disk-full failure-plane fix:
// dead-letter, stuck, and skip records (and their clears) are coordination-
// class writes admitted at EVERY disk state — freezing them converted one
// poison entry during a full-disk window into a permanently parked syncable
// (the record-persist retry inflated the breaker) with the operator's manual
// levers 409ing. Config-class writes stay frozen at full, and user data stays
// rejected from critical up.
func TestFailurePlaneAdmittedAtDiskFull(t *testing.T) {
	deadLetter, err := cluster.NewUpsertSyncableDeadLetterEntity(&cluster.SyncableDeadLetter{ID: "s", Index: 1, Kind: "permanent", Message: "x"})
	if err != nil {
		t.Fatal(err)
	}
	stuck, err := cluster.NewUpsertSyncableStuckEntity(&cluster.SyncableStuck{ID: "s", Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	skip, err := cluster.NewUpsertSyncableSkipRequestEntity(&cluster.SyncableSkipRequest{ID: "s", Index: 1})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := cluster.NewUpsertSyncableEntity(&cluster.Configuration{ID: "s", MimeType: "text/toml", Data: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}

	kindOf := func(e *cluster.Entity) string {
		return proposalKind(&cluster.Proposal{Entities: []*cluster.Entity{e}})
	}

	for name, e := range map[string]*cluster.Entity{"dead-letter": deadLetter, "stuck": stuck, "skip": skip} {
		if got := kindOf(e); got != string(cluster.AdmissionCoordination) {
			t.Fatalf("%s classifies as %q, want coordination", name, got)
		}
		if err := diskRejection(diskFull, kindOf(e)); err != nil {
			t.Fatalf("%s must be admitted at disk-full, got %v", name, err)
		}
	}
	if got := kindOf(cfg); got != string(cluster.AdmissionConfig) {
		t.Fatalf("config classifies as %q, want config", got)
	}
	if err := diskRejection(diskFull, kindOf(cfg)); !errors.Is(err, cluster.ErrInsufficientStorage) {
		t.Fatalf("config must stay frozen at disk-full, got %v", err)
	}
	if err := diskRejection(diskCritical, "user"); !errors.Is(err, cluster.ErrInsufficientStorage) {
		t.Fatalf("user data must stay rejected at critical, got %v", err)
	}
}
