package lease_test

import (
	"testing"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
	"task101-leaselock/internal/store"
)

func TestBulkReleaseRejectsDuplicateResourcesAsInvalid(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(600, 0))
	st, err := store.Open(t.TempDir() + "/lease.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := lease.NewService(st, clk)
	l, err := svc.Acquire("X", "H", 30, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.BulkRelease("H", []lease.ReleaseEntry{{Resource: "X", Token: l.Token}, {Resource: "X", Token: l.Token}})
	if !lease.IsInvalid(err) {
		t.Fatalf("duplicate bulk release error=%v want invalid request", err)
	}
}
