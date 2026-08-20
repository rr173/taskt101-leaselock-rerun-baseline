package lease_test

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"task101-leaselock/internal/clock"
	"task101-leaselock/internal/lease"
	"task101-leaselock/internal/store"
)

// newServiceOverRealStore wires a Service over a real bbolt store in a temp
// dir, with a controllable clock. Returns the service plus a close func.
func newServiceOverRealStore(t *testing.T, clk clock.Clock) (*lease.Service, func()) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	svc := lease.NewService(st, clk)
	return svc, func() { _ = st.Close() }
}

func TestServiceAcquireRenewRelease(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	l, err := svc.Acquire("X", "H1", 5, "")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if l.Token != 1 {
		t.Fatalf("token = %d want 1", l.Token)
	}
	// Held.
	if _, err := svc.Acquire("X", "H2", 5, ""); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("held Acquire = %v want ErrHeld", err)
	}
	// Renew.
	clk.Set(time.Unix(103, 0))
	l2, err := svc.Renew("X", "H1", l.Token, 5)
	if err != nil || l2.Deadline.Unix() != 108 {
		t.Fatalf("Renew = %v deadline=%d", err, l2.Deadline.Unix())
	}
	// Release.
	if err := svc.Release("X", "H1", l.Token); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestServiceAcquireViaPolicy(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	if _, err := svc.PutPolicy("short", 10, "d"); err != nil {
		t.Fatalf("PutPolicy: %v", err)
	}
	l, err := svc.Acquire("X", "H1", 0, "short")
	if err != nil {
		t.Fatalf("Acquire via policy: %v", err)
	}
	if l.TTLSeconds != 10 {
		t.Fatalf("ttl = %d want 10", l.TTLSeconds)
	}
	// Both ttl and policy -> invalid.
	if _, err := svc.Acquire("Y", "H1", 5, "short"); !lease.IsInvalid(err) {
		t.Fatalf("both ttl+policy = %v want ErrInvalid", err)
	}
	// Unknown policy -> ErrPolicyNotFound.
	if _, err := svc.Acquire("Z", "H1", 0, "nope"); !errors.Is(err, lease.ErrPolicyNotFound) {
		t.Fatalf("unknown policy = %v want ErrPolicyNotFound", err)
	}
}

func TestServiceRecoverReapsExpired(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	svc.Acquire("X", "H1", 5, "") // deadline 105
	svc.Acquire("Y", "H1", 100, "") // deadline 200
	clk.Set(time.Unix(110, 0)) // X expired, Y alive
	n, err := svc.Recover()
	if err != nil || n != 1 {
		t.Fatalf("Recover = %d err=%v want 1", n, err)
	}
	_, _, ok := svc.Inspect("X")
	if ok {
		t.Fatalf("X should have been reaped")
	}
	_, active, ok := svc.Inspect("Y")
	if !ok || !active {
		t.Fatalf("Y should be recovered active")
	}
}

func TestServiceBulkAcquireAllOrNothing(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	svc.Acquire("Y", "H", 5, "")
	if _, err := svc.BulkAcquire("H", []string{"X", "Y", "Z"}, 5); !errors.Is(err, lease.ErrConflict) {
		t.Fatalf("bulk with held member = %v want ErrConflict", err)
	}
	granted, err := svc.BulkAcquire("H", []string{"X", "Z"}, 5)
	if err != nil || len(granted) != 2 {
		t.Fatalf("clean bulk = %v granted=%d", err, len(granted))
	}
}

func TestServiceQuotaEnforcement(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	svc.PutHolder("H1", "d", 2)
	svc.Acquire("A", "H1", 5, "")
	svc.Acquire("B", "H1", 5, "")
	if _, err := svc.Acquire("C", "H1", 5, ""); !errors.Is(err, lease.ErrQuotaExceeded) {
		t.Fatalf("over-quota = %v want ErrQuotaExceeded", err)
	}
}

func TestServiceValidationErrors(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	if _, err := svc.Acquire("", "H", 5, ""); !lease.IsInvalid(err) {
		t.Fatalf("empty resource = %v want ErrInvalid", err)
	}
	if _, err := svc.Acquire("X", "H", 0, ""); !lease.IsInvalid(err) {
		t.Fatalf("ttl=0 = %v want ErrInvalid", err)
	}
	if _, err := svc.BulkAcquire("H", nil, 5); !lease.IsInvalid(err) {
		t.Fatalf("empty bulk = %v want ErrInvalid", err)
	}
}

func TestServiceStatsAndAudit(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	svc, closeFn := newServiceOverRealStore(t, clk)
	defer closeFn()

	svc.Acquire("X", "H1", 5, "")
	svc.PutHolder("H1", "d", 5)
	st, err := svc.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if st.TotalLeases != 1 || st.RegisteredHolders != 1 || st.NextFencingToken != 2 {
		t.Fatalf("stats = %+v", st)
	}
	entries, err := svc.ListAudit("X", 100, 0)
	if err != nil || len(entries) != 1 {
		t.Fatalf("audit = %d want 1", len(entries))
	}
}
